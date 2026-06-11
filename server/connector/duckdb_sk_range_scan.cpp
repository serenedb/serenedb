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

#include "connector/duckdb_sk_range_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_key_builder.hpp"
#include "connector/duckdb_range_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/pk_batch_helpers.h"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb::connector {
namespace {

// Build lower and upper bound keys for a single ResolvedRange over an SK index.
//
// SK key layout per entry:
//   [shard (8B)][dummy col_id=0 (8B)]
//   [per prefix column: 0x01(NULL)|0x02(NOT NULL) + encoded_value ...]
//   [range column: 0x01(NULL)|0x02(NOT NULL) + encoded_value (partial)]
//   [PK bytes]  <- only for non-unique or unique-null entries
//
// Null entries (marker 0x01) sort before non-null entries (marker 0x02).
void BuildSkRangeBounds(ObjectId shard_id, const ResolvedRange& range,
                        std::string& lower, std::string& upper) {
  // Fixed 16-byte header.
  secondary_key::AppendShardPrefix(lower, shard_id);
  secondary_key::AppendDummyColumnId(lower);

  // Exact prefix columns.
  for (const auto& v : range.prefix) {
    if (v.IsNull()) {
      secondary_key::AppendNullMarker(lower);
    } else {
      secondary_key::AppendNotNullMarker(lower);
      AppendDuckDBValueToKey(lower, v);
    }
  }
  upper = lower;  // upper shares the same prefix

  const auto& range_column = range.range_column;
  // kIsNull is exclusive (enforced in ColumnRange::Make), so no separate
  // invariant check is needed here.

  if (range_column.IsNull()) {
    // col IS NULL -> scan the null bucket only.
    secondary_key::AppendNullMarker(lower);     // start at 0x01
    secondary_key::AppendNotNullMarker(upper);  // exclusive end at 0x02
  } else {
    // Lower bound.
    secondary_key::AppendNotNullMarker(lower);  // unbounded left, no null

    if (range_column.HasLeft()) {
      AppendDuckDBValueToKey(lower, range_column.LeftValue());
      if (!range_column.IsLeftInclusive()) {
        MakeExclusive(lower);
      }
    }

    // Upper bound.
    if (range_column.HasRight()) {
      secondary_key::AppendNotNullMarker(upper);
      AppendDuckDBValueToKey(upper, range_column.RightValue());
      if (range_column.IsRightInclusive()) {
        MakeExclusive(upper);
      }
    } else if (!upper.empty()) {
      // Prefix non-empty but no right bound: cap at end of prefix level.
      // If MakeExclusiveUpperBound clears upper (all-0xFF), falls back to
      // shard_upper_bound.
      MakeExclusive(upper);
    }
    // else: no right bound, no prefix -> upper stays empty -> shard_upper_bound
    // fallback
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SKRangeScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);

  auto& scan = bind_data.scan_source->Cast<SkRangeScan>();

  // Build shard-level fallback upper bound: increment the [shard][dummy]
  // prefix.
  secondary_key::AppendShardPrefix(state->shard_upper_bound, scan.shard_id);
  secondary_key::AppendDummyColumnId(state->shard_upper_bound);
  MakeExclusive(state->shard_upper_bound);
  state->shard_upper_bound_slice = rocksdb::Slice{state->shard_upper_bound};

  // Build per-range bounds.
  state->lower_keys.reserve(scan.ranges.size());
  state->upper_keys.reserve(scan.ranges.size());
  for (const auto& range : scan.ranges) {
    if (range.IsEmpty()) {
      continue;
    }
    std::string lower, upper;
    BuildSkRangeBounds(scan.shard_id, range, lower, upper);
    state->lower_keys.push_back(std::move(lower));
    state->upper_keys.push_back(std::move(upper));
  }

  if (state->lower_keys.empty()) {
    state->finished = true;
    return state;
  }

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  auto factory =
    [db, cf, txn = state->txn](
      rocksdb::ReadOptions opts) -> std::unique_ptr<rocksdb::Iterator> {
    return std::unique_ptr<rocksdb::Iterator>(txn ? txn->GetIterator(opts, cf)
                                                  : db->NewIterator(opts, cf));
  };

  rocksdb::ReadOptions base_opts;
  base_opts.snapshot = state->snapshot;
  base_opts.async_io = false;
  base_opts.adaptive_readahead = true;
  base_opts.total_order_seek = true;

  state->sk_iterator = std::make_unique<RocksDBPrefixRangeColumnIterator>(
    std::move(factory), base_opts,
    std::span<const std::string>{state->lower_keys},
    std::span<const std::string>{state->upper_keys},
    state->shard_upper_bound_slice);

  return state;
}

void SKRangeScanFunction(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SKRangeScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  auto& it = *gstate.sk_iterator;

  if (!gstate.index_source) {
    gstate.index_source =
      MakeIndexSource(context, bind_data, gstate.snapshot, gstate.txn,
                      gstate.external_projected_columns, gstate.projected_types,
                      bind_data.column_ids);
  }
  if (std::holds_alternative<std::monostate>(gstate.pk_batch)) {
    gstate.pk_batch = gstate.index_source->CreatePkBatch();
  }

  size_t num_rows = std::visit(
    [&](auto& pk) -> size_t {
      using T = std::decay_t<decltype(pk)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "pk_batch must be initialised");
        return 0;
      } else {
        pk.Reset();
        if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
          pk.EnsureInit(duckdb::Allocator::DefaultAllocator());
        }
        while (PrimaryKeysSize(pk) < STANDARD_VECTOR_SIZE && it.Valid()) {
          auto val = it.value();
          auto key = it.key();
          SDB_ASSERT(val.size() >= 2);
          std::string_view pk_bytes;
          if (val[0] == secondary_key::kPKInValue) {
            pk_bytes = std::string_view{val.data() + 1, val.size() - 1};
          } else {
            SDB_ASSERT(val[0] == secondary_key::kPKInKey);
            uint8_t pk_size = static_cast<uint8_t>(val[1]);
            SDB_ASSERT(key.size() >= pk_size);
            pk_bytes =
              std::string_view{key.data() + key.size() - pk_size, pk_size};
          }
          AppendPrimaryKey(pk, pk_bytes);
          it.Next();
        }
        return PrimaryKeysSize(pk);
      }
    },
    gstate.pk_batch);
  rocksutils::CheckIteratorStatus(it);

  if (num_rows == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }
  if (!it.Valid()) {
    gstate.finished = true;
  }

  gstate.index_source->Materialize(context, gstate.pk_batch, 0, num_rows,
                                   output);

  if (gstate.scan_rowid) {
    const auto row_base = gstate.produced_rows.load(std::memory_order_relaxed);
    auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.rowid_output_idx]);
    for (size_t i = 0; i < num_rows; ++i) {
      d[i] = static_cast<int64_t>(row_base + i);
    }
  }
  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value), duckdb::count_t(num_rows));
  }

  output.SetChildCardinality(num_rows);
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
}

}  // namespace sdb::connector
