////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_pk_range_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_key_builder.hpp"
#include "connector/duckdb_range_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<PKRangeScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);
  auto column_keys = InitPKScanColumns(*state, bind_data);
  const auto num_scan = column_keys.size();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  auto& range_source = bind_data.scan_source->Cast<PkRangeScan>();
  const auto& ranges = range_source.ranges;

  // Build column-independent lower/upper suffix pairs for each non-empty range.
  struct RangeBounds {
    std::string lower;
    std::string upper;  // empty = no right bound; use column-level fallback
  };

  std::vector<RangeBounds> range_bounds;
  range_bounds.reserve(ranges.size());

  for (const auto& range : ranges) {
    if (range.IsEmpty()) {
      continue;
    }

    std::string lower;
    for (const auto& v : range.prefix) {
      AppendDuckDBValueToKey(lower, v);
    }
    const size_t prefix_len = lower.size();

    if (range.range_column.HasLeft()) {
      AppendDuckDBValueToKey(lower, range.range_column.LeftValue());
      if (!range.range_column.IsLeftInclusive()) {
        MakeExclusive(lower);
      }
    }

    std::string upper(lower, 0, prefix_len);

    if (range.range_column.HasRight()) {
      AppendDuckDBValueToKey(upper, range.range_column.RightValue());
      if (range.range_column.IsRightInclusive()) {
        MakeExclusive(upper);
      }
    } else if (prefix_len > 0) {
      MakeExclusive(upper);
    }
    // else: no right bound and no prefix -> upper stays empty -> column-level
    // fallback

    range_bounds.push_back({std::move(lower), std::move(upper)});
  }

  const size_t n_ranges = range_bounds.size();

  // Populate flat key arrays: [col*n_ranges + range_idx].
  // Reserve first so spans taken below remain stable.
  state->split_prefix_keys.reserve(num_scan * n_ranges);
  state->split_upper_bound_keys.reserve(num_scan * n_ranges);

  for (size_t col_i = 0; col_i < num_scan; ++col_i) {
    for (const auto& rb : range_bounds) {
      state->split_prefix_keys.push_back(column_keys[col_i] + rb.lower);
      state->split_upper_bound_keys.push_back(
        rb.upper.empty() ? std::string{} : column_keys[col_i] + rb.upper);
    }
  }

  // Create one RocksDBPrefixRangeColumnIterator per column.
  rocksdb::ReadOptions base_opts;
  base_opts.snapshot = state->snapshot;
  base_opts.async_io = false;
  base_opts.adaptive_readahead = true;
  base_opts.auto_prefix_mode = true;

  for (size_t col_i = 0; col_i < num_scan; ++col_i) {
    const size_t off = col_i * n_ranges;
    std::span<const std::string> lower_span{
      state->split_prefix_keys.data() + off, n_ranges};
    std::span<const std::string> upper_span{
      state->split_upper_bound_keys.data() + off, n_ranges};

    auto factory =
      [&, txn = state->txn,
       cf](rocksdb::ReadOptions opts) -> std::unique_ptr<rocksdb::Iterator> {
      return std::unique_ptr<rocksdb::Iterator>(
        txn ? txn->GetIterator(opts, cf) : db->NewIterator(opts, cf));
    };

    state->iterators.push_back(
      std::make_unique<RocksDBPrefixRangeColumnIterator>(
        std::move(factory), base_opts, lower_span, upper_span,
        state->upper_bound_slices[col_i]));
  }

  return state;
}

void PKRangeScanFunction(duckdb::ClientContext& /*context*/,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<PKRangeScanGlobalState>();
  PKScanFunctionImpl(gstate, gstate.iterators, output);
}

}  // namespace sdb::connector
