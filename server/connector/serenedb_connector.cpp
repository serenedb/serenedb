////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "serenedb_connector.hpp"

#include <iresearch/search/all_filter.hpp>

#include "basics/static_strings.h"
#include "catalog/secondary_index.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"
#include "search_filter_builder.hpp"
#include "storage_engine/secondary_index_shard.h"
LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {
namespace {

// Whether execute push downed into table scan filters or use separate filter
// operator. When value is changed, some explain tests will be broken (as
// execution graph may change).
constexpr bool kExecuteFiltersInTableScan = false;

// Try to extract an equality predicate (col = const) from a filter expression
// where col matches a secondary index column. Returns true if found and fills
// out_prefix / out_value_key_size.
struct SecondaryIndexMatch {
  ObjectId shard_id;
  std::string scan_prefix;
  size_t value_key_size = 0;
  catalog::Column::Id effective_column_id;
};

// Encode a Velox constant into the sortable index key format.
bool EncodeConstantToSortableKey(std::string& key,
                                 const velox::variant& value,
                                 velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN: {
      key.push_back(value.value<bool>() ? '\x01' : '\x00');
      return true;
    }
    case velox::TypeKind::TINYINT: {
      primary_key::AppendSigned(key, static_cast<int8_t>(value.value<int8_t>()));
      return true;
    }
    case velox::TypeKind::SMALLINT: {
      primary_key::AppendSigned(key, static_cast<int16_t>(value.value<int16_t>()));
      return true;
    }
    case velox::TypeKind::INTEGER: {
      primary_key::AppendSigned(key, value.value<int32_t>());
      return true;
    }
    case velox::TypeKind::BIGINT: {
      primary_key::AppendSigned(key, value.value<int64_t>());
      return true;
    }
    case velox::TypeKind::REAL: {
      float v = value.value<float>();
      auto bits = std::bit_cast<uint32_t>(v);
      if (bits & 0x80000000u) {
        bits = ~bits;
      } else {
        bits ^= 0x80000000u;
      }
      const auto base = key.size();
      basics::StrAppend(key, sizeof(uint32_t));
      absl::big_endian::Store32(key.data() + base, bits);
      return true;
    }
    case velox::TypeKind::DOUBLE: {
      double v = value.value<double>();
      auto bits = std::bit_cast<uint64_t>(v);
      if (bits & 0x8000000000000000ull) {
        bits = ~bits;
      } else {
        bits ^= 0x8000000000000000ull;
      }
      const auto base = key.size();
      basics::StrAppend(key, sizeof(uint64_t));
      absl::big_endian::Store64(key.data() + base, bits);
      return true;
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      auto sv = value.value<velox::StringView>();
      for (size_t i = 0; i < sv.size(); ++i) {
        char c = sv.data()[i];
        if (c == '\x00') {
          key.push_back('\x00');
          key.push_back('\x01');
        } else {
          key.push_back(c);
        }
      }
      key.push_back('\x00');
      key.push_back('\x00');
      return true;
    }
    default:
      return false;
  }
}

// Try to match a single filter expression against a secondary index.
// Looks for: eq(field_access("col"), constant) where "col" is an indexed column.
std::optional<SecondaryIndexMatch> TryMatchSecondaryIndex(
  const velox::core::TypedExprPtr& filter,
  const axiom::connector::Table& table,
  query::Transaction& transaction) {
  // Must be a function call
  const auto* call =
    dynamic_cast<const velox::core::CallTypedExpr*>(filter.get());
  if (!call || (call->name() != "eq" && !call->name().ends_with("_eq")) ||
      call->inputs().size() != 2) {
    return std::nullopt;
  }

  // One input must be field access, other must be constant
  const velox::core::FieldAccessTypedExpr* field = nullptr;
  const velox::core::ConstantTypedExpr* constant = nullptr;

  for (const auto& input : call->inputs()) {
    if (auto* f =
          dynamic_cast<const velox::core::FieldAccessTypedExpr*>(input.get())) {
      field = f;
    } else if (auto* c = dynamic_cast<const velox::core::ConstantTypedExpr*>(
                 input.get())) {
      constant = c;
    }
  }

  if (!field || !constant || constant->hasValueVector()) {
    return std::nullopt;
  }

  // Find the column in the table
  const auto* col_ptr = table.findColumn(field->name());
  if (!col_ptr) {
    return std::nullopt;
  }
  const auto* serene_col = dynamic_cast<const SereneDBColumn*>(col_ptr);
  if (!serene_col) {
    return std::nullopt;
  }
  auto column_id = serene_col->Id();

  // Look up secondary indexes on this table
  auto& rocksdb_table = basics::downCast<const RocksDBTable>(table);
  auto snapshot = transaction.GetCatalogSnapshot();

  for (auto index_shard : snapshot->GetIndexShardsByTable(rocksdb_table.TableId())) {
    if (index_shard->GetType() != IndexType::Secondary) {
      continue;
    }
    auto index =
      snapshot->GetObject<catalog::Index>(index_shard->GetIndexId());
    if (!index) {
      continue;
    }
    auto index_columns = index->GetColumnIds();
    // For now, only single-column indexes with the matching column
    if (index_columns.size() != 1 || index_columns[0] != column_id) {
      continue;
    }

    // Build scan prefix: <shard_id> <0x01 (not null)> <encoded_value>
    std::string prefix;
    secondary_key::AppendShardPrefix(prefix, index_shard->GetId());
    secondary_key::AppendNotNullMarker(prefix);

    if (!EncodeConstantToSortableKey(prefix, constant->value(),
                                     field->type()->kind())) {
      continue;
    }
    size_t value_key_size = prefix.size() - sizeof(ObjectId);

    // Find effective column id for materialization
    const auto& column_map = table.columnMap();
    SDB_ASSERT(!column_map.empty());
    auto eff_col_id =
      basics::downCast<const SereneDBColumn>(column_map.begin()->second)->Id();

    return SecondaryIndexMatch{
      .shard_id = index_shard->GetId(),
      .scan_prefix = std::move(prefix),
      .value_key_size = value_key_size,
      .effective_column_id = eff_col_id,
    };
  }

  return std::nullopt;
}

}  // namespace

SereneDBConnectorTableHandle::SereneDBConnectorTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  const axiom::connector::TableLayout& layout, std::vector<Point> points,
  velox::core::TypedExprPtr remaining_filter)
  : velox::connector::ConnectorTableHandle{StaticStrings::kSereneDBConnector},
    _name{layout.name()},
    _table_id{basics::downCast<RocksDBTable>(layout.table()).TableId()},
    _transaction{
      basics::downCast<RocksDBTable>(layout.table()).GetTransaction()},
    _points{std::move(points)},
    _remaining_filter{std::move(remaining_filter)} {
  const auto& column_map = layout.table().columnMap();
  SDB_ASSERT(!column_map.empty(),
             "Tables without columns must be processed in analyzer step");

  // TODO(Dronplane): measure the performance! Maybe it's worth selecting the
  // smallest possible field as the effective column, not just the first
  _effective_column_id =
    basics::downCast<const SereneDBColumn>(column_map.begin()->second)->Id();
  if (_effective_column_id == catalog::Column::kGeneratedPKId) {
    // Iterating over generated primary key gives 0 rows,
    // use another one
    SDB_ASSERT(column_map.size() >= 2);
    _effective_column_id = basics::downCast<const SereneDBColumn>(
                             std::next(column_map.begin())->second)
                             ->Id();
  }
  _pk_type = basics::downCast<RocksDBTable>(layout.table()).PKType();

  for (const auto& [orig_name, col_ptr] : column_map) {
    const auto* scol = basics::downCast<const SereneDBColumn>(col_ptr);
    _table_column_map.emplace(orig_name,
                              FilterColumn{scol->Id(), scol->type()});
  }

  _transaction.AddRocksDBRead();
}

velox::connector::ConnectorTableHandlePtr
SereneDBTableLayout::createTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  std::vector<velox::connector::ColumnHandlePtr> column_handles,
  velox::core::ExpressionEvaluator& evaluator,
  std::vector<velox::core::TypedExprPtr> filters,
  std::vector<velox::core::TypedExprPtr>& rejected_filters) const {
  const auto* table = &this->table();
  const auto* inv_index = dynamic_cast<const InvertedIndexTable*>(table);
  if (inv_index) {
    const auto& index = inv_index->GetIndex();
    auto column_getter =
      [&](std::string_view name) -> std::optional<SearchColumnInfo> {
      const auto* column = inv_index->findColumn(name);
      if (column) {
        const auto* serene_column = basics::downCast<SereneDBColumn>(column);
        auto index_columns = index.GetColumnIds();

        if (absl::c_find(index_columns, serene_column->Id()) !=
            index.GetColumnIds().end()) {
          return SearchColumnInfo{
            .info = *serene_column,
            .analyzer = index.GetColumnAnalyzer(serene_column->Id()),
          };
        }
      }
      return std::nullopt;
    };

    const auto& snapshot = inv_index->GetTransaction().EnsureSearchSnapshot(
      inv_index->GetIndex().GetId());
    // TODO(Dronplane) link irs memory manager to velox pool
    const auto& scorer = inv_index->GetScorerPtr();

    irs::And conjunct_root;
    for (auto& filter : filters) {
      const auto old_size = conjunct_root.size();
      if (MakeSearchFilter(conjunct_root, {&filter, 1}, column_getter).ok()) {
        SDB_ASSERT(conjunct_root.size() > old_size);
      } else {
        conjunct_root.Erase(old_size);
        rejected_filters.push_back(std::move(filter));
      }
    }

    irs::Filter::Query::ptr prepared;
    if (conjunct_root.empty()) {
      irs::All all_filter;
      prepared =
        all_filter.prepare({.index = snapshot.reader, .scorer = scorer.get()});
    } else {
      prepared = conjunct_root.prepare(
        {.index = snapshot.reader, .scorer = scorer.get()});
    }

    return std::make_shared<InvertedIndexTableHandle>(
      *inv_index, index.GetId(), std::move(prepared), scorer);
  }

  if (const auto* read_file_table = dynamic_cast<const ReadFileTable*>(table)) {
    double sample_rate = 1.0;
    velox::common::SubfieldFilters subfield_filters;
    std::vector<velox::core::TypedExprPtr> remaining_conjuncts;
    for (auto& filter : filters) {
      auto remaining =
        velox::connector::hive::extractFiltersFromRemainingFilter(
          filter, &evaluator, subfield_filters, sample_rate);
      if (remaining) {
        remaining_conjuncts.push_back(remaining);
        rejected_filters.push_back(std::move(remaining));
      }
    }

    velox::core::TypedExprPtr remaining_filter;
    if (remaining_conjuncts.size() == 1) {
      remaining_filter = std::move(remaining_conjuncts[0]);
    } else if (remaining_conjuncts.size() > 1) {
      remaining_filter = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(), std::move(remaining_conjuncts),
        velox::expression::kAnd);
    }

    return std::make_shared<FileTableHandle>(read_file_table->GetOptions(),
                                             std::move(subfield_filters),
                                             std::move(remaining_filter));
  }

  const auto& rocksdb_table = basics::downCast<RocksDBTable>(*table);
  const auto& pk_type = rocksdb_table.PKType();

  // Try secondary index pushdown for individual equality filters.
  for (size_t i = 0; i < filters.size(); ++i) {
    auto match = TryMatchSecondaryIndex(filters[i], *table,
                                        rocksdb_table.GetTransaction());
    if (match) {
      // Reject all other filters for post-scan evaluation.
      for (size_t j = 0; j < filters.size(); ++j) {
        if (j != i) {
          rejected_filters.push_back(std::move(filters[j]));
        }
      }
      return std::make_shared<SecondaryIndexTableHandle>(
        std::string{table->name()}, rocksdb_table.TableId(),
        rocksdb_table.GetTransaction(), match->effective_column_id,
        match->shard_id, std::move(match->scan_prefix),
        match->value_key_size, table);
    }
  }

  velox::core::TypedExprPtr remaining_filter;
  if (filters.size() == 1) {
    remaining_filter = filters[0];
  } else if (filters.size() > 1) {
    remaining_filter = std::make_shared<velox::core::CallTypedExpr>(
      velox::BOOLEAN(), filters, velox::expression::kAnd);
  }

  std::vector<Point> points;
  if (remaining_filter) {
    auto res = ExtractAndRewriteFilterExpr(remaining_filter, pk_type->names());

    if (!res.points.empty()) {
      points = std::move(res.points);
      SortPoints(points, *pk_type);
      remaining_filter = std::move(res.remaining_filter);
    }
  }

  if (!kExecuteFiltersInTableScan && remaining_filter) {
    rejected_filters = {std::move(remaining_filter)};
    remaining_filter.reset();
  }

  SDB_ASSERT(!table->columnMap().empty(),
             "SereneDBFullScanTableHandle: need a column for count field");
  return std::make_shared<SereneDBConnectorTableHandle>(
    session, *table->layouts().front(), std::move(points),
    std::move(remaining_filter));
}

}  // namespace sdb::connector
