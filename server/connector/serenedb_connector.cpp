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
#include "search_filter_printer.hpp"
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

void RejectFilter(velox::core::TypedExprPtr filter,
                  std::vector<velox::core::TypedExprPtr>& rejected_filters) {
  if (!filter) {
    return;
  }
  if (const auto* call =
        dynamic_cast<const velox::core::CallTypedExpr*>(filter.get());
      call && call->name() == velox::expression::kAnd) {
    rejected_filters.assign(call->inputs().begin(), call->inputs().end());
  } else {
    rejected_filters = {std::move(filter)};
  }
}

std::shared_ptr<SecondaryIndexTableHandle> TryMatchSecondaryIndex(
  const velox::core::TypedExprPtr& filter, const axiom::connector::Table& table,
  query::Transaction& transaction,
  velox::core::TypedExprPtr& remaining_filter) {
  auto& rocksdb_table = basics::downCast<const RocksDBTable>(table);
  auto snapshot = transaction.EnsureCatalogSnapshot();

  for (auto index_shard :
       snapshot->GetIndexShardsByTable(rocksdb_table.TableId())) {
    if (index_shard->GetType() != IndexType::Secondary) {
      continue;
    }
    auto index = snapshot->GetObject<catalog::Index>(index_shard->GetIndexId());
    if (!index) {
      continue;
    }
    auto index_col_ids = index->GetColumnIds();

    std::vector<std::string> sk_names;
    std::vector<velox::TypePtr> sk_types;
    sk_names.reserve(index_col_ids.size());
    sk_types.reserve(index_col_ids.size());
    for (auto col_id : index_col_ids) {
      for (const auto& [name, col_ptr] : table.columnMap()) {
        const auto* scol = basics::downCast<const SereneDBColumn>(col_ptr);
        if (scol->Id() == col_id) {
          sk_names.push_back(name);
          sk_types.push_back(scol->type());
          break;
        }
      }
    }
    if (sk_names.size() != index_col_ids.size()) {
      continue;
    }

    auto res = ExtractAndRewriteFilterExpr(filter, sk_names);
    if (res.points.empty() || !absl::c_all_of(res.points, [](const Point& p) {
          return p.IsSpecific();
        })) {
      continue;
    }

    auto sk_type = velox::ROW(std::move(sk_names), std::move(sk_types));
    auto points = ToSpecificPoints(res.points, *sk_type);
    SortPoints(points);

    const auto& sec_index =
      basics::downCast<const catalog::SecondaryIndex>(*index);

    remaining_filter = std::move(res.remaining_filter);
    return std::make_shared<SecondaryIndexTableHandle>(
      table.name(), rocksdb_table.TableId(), transaction, index_shard->GetId(),
      std::move(points), std::move(sk_type), table, sec_index.IsUnique());
  }

  return nullptr;
}

}  // namespace

SereneDBConnectorTableHandle::SereneDBConnectorTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  const axiom::connector::TableLayout& layout,
  std::vector<SpecificPoint> points, velox::core::TypedExprPtr remaining_filter)
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
  _effective_column_id = ComputeEffectiveColumnId(column_map);
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
  const auto* idx_table = dynamic_cast<const IndexTable*>(table);
  if (idx_table &&
      idx_table->GetIndex().GetIndexType() == IndexType::Inverted) {
    const auto* inv_index = idx_table;
    const auto& index =
      basics::downCast<const catalog::InvertedIndex>(inv_index->GetIndex());
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
            .analyzer = index.GetColumnAnalyzer(
              inv_index->GetTransaction().EnsureCatalogSnapshot(),
              serene_column->Id()),
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

    const auto& col_map = inv_index->columnMap();
    auto column_id_to_name =
      [&](sdb::catalog::Column::Id id) -> std::string_view {
      for (const auto& [name, col_ptr] : col_map) {
        if (basics::downCast<const SereneDBColumn>(col_ptr)->Id() == id) {
          return name;
        }
      }
      SDB_ASSERT(false, "Unknown column id");
      return "unknown";
    };

    return std::make_shared<InvertedIndexTableHandle>(
      *inv_index, index.GetId(), std::move(prepared), scorer,
      irs::ToStringDemangled(conjunct_root, column_id_to_name));
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

  velox::core::TypedExprPtr remaining_filter;
  if (filters.size() == 1) {
    remaining_filter = filters[0];
  } else if (filters.size() > 1) {
    remaining_filter = std::make_shared<velox::core::CallTypedExpr>(
      velox::BOOLEAN(), filters, velox::expression::kAnd);
  }

  std::vector<SpecificPoint> points;
  if (remaining_filter) {
    // 1. Try PK point lookup.
    auto res = ExtractAndRewriteFilterExpr(remaining_filter, pk_type->names());
    if (!res.points.empty() && absl::c_all_of(res.points, [](const Point& p) {
          return p.IsSpecific();
        })) {
      points = ToSpecificPoints(res.points, *pk_type);
      SortPoints(points);
      remaining_filter = std::move(res.remaining_filter);
    }

    // 2. No PK match -- try secondary index.
    if (points.empty()) {
      velox::core::TypedExprPtr remaining;
      if (auto handle =
            TryMatchSecondaryIndex(remaining_filter, *table,
                                   rocksdb_table.GetTransaction(), remaining)) {
        RejectFilter(std::move(remaining), rejected_filters);
        return handle;
      }
    }
  }

  if (!kExecuteFiltersInTableScan) {
    RejectFilter(std::move(remaining_filter), rejected_filters);
  }

  SDB_ASSERT(!table->columnMap().empty(),
             "SereneDBFullScanTableHandle: need a column for count field");
  return std::make_shared<SereneDBConnectorTableHandle>(
    session, *table->layouts().front(), std::move(points),
    std::move(remaining_filter));
}

std::string SereneDBConnectorTableHandle::toString() const {
  const std::string filter_str =
    _remaining_filter ? absl::StrCat(", filter=", _remaining_filter->toString())
                      : "";
  if (!_points.empty()) {
    const auto& names = _pk_type->names();
    const auto& types = _pk_type->children();
    std::string points_str = absl::StrJoin(
      _points, ", ", [&](std::string* out, const SpecificPoint& point) {
        SDB_ASSERT(types.size() == point.size());
        absl::StrAppend(out, "(");
        for (size_t i = 0; i < point.size(); ++i) {
          if (i > 0) {
            absl::StrAppend(out, ", ");
          }
          absl::StrAppend(out, names[i], "=", point[i].toString(types[i]));
        }
        absl::StrAppend(out, ")");
      });
    return absl::StrCat(_name, ", type=rocksdb_point_lookup, points=[",
                        points_str, "]", filter_str);
  }
  return absl::StrCat(_name, ", type=rocksdb_full_scan", filter_str);
}

}  // namespace sdb::connector
