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
#include "connector/key_utils.hpp"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"
#include "search_filter_builder.hpp"
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
  const RocksDBInvertedIndexTable* inverted_index_table;
  if ((inverted_index_table =
         dynamic_cast<const RocksDBInvertedIndexTable*>(&this->table()))) {
    const auto& index = inverted_index_table->GetIndex();
    auto column_getter =
      [&](std::string_view name) -> std::optional<SearchColumnInfo> {
      const auto* column = inverted_index_table->findColumn(name);
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

    auto handle = std::make_shared<SereneDBConnectorTableHandle>(
      session, *this, std::vector<Point>{}, nullptr);
    const auto& snapshot =
      inverted_index_table->GetTransaction().EnsureSearchSnapshot(
        inverted_index_table->GetIndex().GetId());
    // TODO(Dronplane) link irs memory manager to velox pool
    const auto& scorer = inverted_index_table->GetScorerPtr();

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

    handle->AddSearchQuery(index.GetId(), std::move(prepared));
    if (scorer) {
      handle->AddScorer(scorer);
    }
    return handle;
  }

  if (const auto* read_file_table =
        dynamic_cast<const ReadFileTable*>(&this->table())) {
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

  const auto& pk_type = basics::downCast<RocksDBTable>(table()).PKType();

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

  SDB_ASSERT(!table().columnMap().empty(),
             "SereneDBFullScanTableHandle: need a column for count field");
  return std::make_shared<SereneDBConnectorTableHandle>(
    session, *this, std::move(points), std::move(remaining_filter));
}

auto RocksDBSplitSource::getSplits(uint64_t /*target_bytes*/)
  -> std::vector<SplitAndGroup> {
  constexpr size_t kKeyPrefixSize =
    sizeof(ObjectId) + sizeof(catalog::Column::Id);

  if (_done) {
    return {SplitAndGroup{}};
  }
  _done = true;

  auto single_split = [&]() -> std::vector<SplitAndGroup> {
    return {SplitAndGroup{std::make_shared<SereneDBConnectorSplit>(
              StaticStrings::kSereneDBConnector)},
            SplitAndGroup{}};
  };

  auto target_n = static_cast<size_t>(
    std::max<int32_t>(_options.targetSplitCount, 2));
  target_n = std::min<size_t>(target_n, 64);

  auto [col_start, col_end] =
    key_utils::CreateTableColumnRange(_table_id, _reference_column_id);

  // Collect SST files that overlap with our column range.
  struct SstRange {
    std::string smallest_pk;
    std::string largest_pk;
    uint64_t size_bytes;
  };
  std::vector<SstRange> ranges;

  rocksdb::ColumnFamilyMetaData cf_meta;
  rocksdb::GetColumnFamilyMetaDataOptions meta_opts{
    rocksdb::Slice{col_start}, rocksdb::Slice{col_end}};
  _db.GetColumnFamilyMetaData(&_cf, meta_opts, &cf_meta);

  for (const auto& level : cf_meta.levels) {
    for (const auto& file : level.files) {
      if (file.largestkey < col_start || file.smallestkey >= col_end) {
        continue;
      }
      if (file.size == 0) {
        continue;
      }
      auto clamp_start =
        file.smallestkey < col_start ? col_start : file.smallestkey;
      auto clamp_end = file.largestkey >= col_end ? col_end : file.largestkey;
      std::string pk_start =
        clamp_start.size() > kKeyPrefixSize
          ? clamp_start.substr(kKeyPrefixSize)
          : std::string{};
      std::string pk_end =
        clamp_end.size() > kKeyPrefixSize
          ? clamp_end.substr(kKeyPrefixSize)
          : std::string{};
      ranges.push_back(
        SstRange{std::move(pk_start), std::move(pk_end), file.size});
    }
  }

  // Debug: dump SST ranges.
  auto hex = [](const std::string& s) -> std::string {
    std::string out;
    out.reserve(s.size() * 2);
    for (unsigned char c : s) {
      char buf[3];
      snprintf(buf, sizeof(buf), "%02x", c);
      out.append(buf, 2);
    }
    return out;
  };
  std::cerr << "[RocksDBSplitSource] table_id=" << _table_id.id()
            << " ref_col=" << _reference_column_id
            << " target_n=" << target_n
            << " sst_count=" << ranges.size() << "\n";
  for (size_t i = 0; i < ranges.size(); ++i) {
    std::cerr << "  SST[" << i << "] size=" << ranges[i].size_bytes
              << " pk_start=" << hex(ranges[i].smallest_pk)
              << " pk_end=" << hex(ranges[i].largest_pk) << "\n";
  }

  if (ranges.empty()) {
    std::cerr << "[RocksDBSplitSource] -> single_split (no SSTs)\n";
    return single_split();
  }

  // Sort by smallest PK.
  absl::c_sort(ranges, [](const SstRange& a, const SstRange& b) {
    return a.smallest_pk < b.smallest_pk;
  });

  // Read up to 16 bytes of a PK string as a big-endian __uint128_t.
  auto pk_to_u128 = [](const std::string& s) -> __uint128_t {
    __uint128_t v = 0;
    for (size_t j = 0; j < s.size() && j < 16; ++j) {
      v = (v << 8) | static_cast<uint8_t>(s[j]);
    }
    // Left-align: if shorter than 16 bytes, shift up.
    if (s.size() < 16) {
      v <<= (16 - s.size()) * 8;
    }
    return v;
  };

  auto u128_to_pk = [](const __uint128_t v, size_t len) -> std::string {
    std::string out(len, '\0');
    for (size_t j = 0; j < len; ++j) {
      out[j] = static_cast<char>(v >> ((15 - j) * 8));
    }
    return out;
  };

  // lo + (hi - lo) * numerator / denominator
  auto interpolate = [&](const std::string& lo, const std::string& hi,
                         uint64_t numerator,
                         uint64_t denominator) -> std::string {
    auto a = pk_to_u128(lo);
    auto b = pk_to_u128(hi);
    auto result = a + (b - a) / denominator * numerator;
    return u128_to_pk(result, std::max(lo.size(), hi.size()));
  };

  // Prefix sum of SST bytes for binary search.
  std::vector<uint64_t> cumulative(ranges.size() + 1, 0);
  for (size_t i = 0; i < ranges.size(); ++i) {
    cumulative[i + 1] = cumulative[i] + ranges[i].size_bytes;
  }
  const uint64_t total_bytes = cumulative.back();
  if (total_bytes == 0) {
    return single_split();
  }

  // For each split boundary, find the byte offset and map it to a PK key.
  std::vector<std::string> boundaries;
  for (size_t i = 1; i < target_n; ++i) {
    const uint64_t target_byte =
      static_cast<uint64_t>(static_cast<__uint128_t>(i) * total_bytes /
                            target_n);

    // Find which SST contains this byte offset.
    auto it = std::upper_bound(cumulative.begin(), cumulative.end(),
                               target_byte);
    size_t sst_idx = static_cast<size_t>(it - cumulative.begin()) - 1;
    const auto& sst = ranges[sst_idx];

    if (sst.smallest_pk >= sst.largest_pk) {
      continue;
    }

    // Interpolate within this SST.
    uint64_t offset_in_sst = target_byte - cumulative[sst_idx];
    auto boundary =
      interpolate(sst.smallest_pk, sst.largest_pk, offset_in_sst,
                  sst.size_bytes);

    if (boundary > sst.smallest_pk && boundary < sst.largest_pk &&
        (boundaries.empty() || boundary > boundaries.back())) {
      boundaries.push_back(std::move(boundary));
    }
  }

  if (boundaries.empty()) {
    std::cerr << "[RocksDBSplitSource] -> single_split (no boundaries)\n";
    return single_split();
  }

  std::cerr << "[RocksDBSplitSource] total_bytes=" << total_bytes
            << " bytes_per_split=" << (total_bytes / target_n)
            << " boundaries=" << boundaries.size()
            << " splits=" << (boundaries.size() + 1) << "\n";
  for (size_t i = 0; i < boundaries.size(); ++i) {
    std::cerr << "  boundary[" << i << "]=" << hex(boundaries[i]) << "\n";
  }

  // Create splits from boundary points.
  std::vector<SplitAndGroup> splits;
  splits.reserve(boundaries.size() + 2);

  splits.emplace_back(std::make_shared<SereneDBConnectorSplit>(
    StaticStrings::kSereneDBConnector, std::string{}, boundaries.front()));

  for (size_t i = 0; i + 1 < boundaries.size(); ++i) {
    splits.emplace_back(std::make_shared<SereneDBConnectorSplit>(
      StaticStrings::kSereneDBConnector, boundaries[i], boundaries[i + 1]));
  }

  splits.emplace_back(std::make_shared<SereneDBConnectorSplit>(
    StaticStrings::kSereneDBConnector, boundaries.back(), std::string{}));

  splits.emplace_back();

  return splits;
}

}  // namespace sdb::connector
