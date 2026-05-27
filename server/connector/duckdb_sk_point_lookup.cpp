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

#include "connector/duckdb_sk_point_lookup.hpp"

#include <duckdb/common/types/data_chunk.hpp>
#include <string_view>

#include "basics/assert.h"
#include "connector/duckdb_key_builder.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/multiget_context.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKPointLookupInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SKPointLookupGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  SDB_ASSERT(bind_data.scan_source->Cast<SkPointScan>().is_unique);

  return state;
}

void SKPointLookupFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SKPointLookupGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  auto& scan = bind_data.scan_source->Cast<SkPointScan>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  auto* db = GetServerEngine().db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  rocksdb::ReadOptions ro;
  ro.snapshot = gstate.snapshot;
  MultiGetContext mgc{*cf, ro};

  const auto probe_col =
    scan.column_ids.empty() ? catalog::Column::Id{0} : scan.column_ids[0];
  DuckDBSecondaryKeyBuilder builder{scan.shard_id};

  if (!gstate.index_source) {
    gstate.index_source =
      MakeIndexSource(context, bind_data, gstate.snapshot, gstate.txn,
                      gstate.external_projected_columns, gstate.projected_types,
                      bind_data.column_ids);
  }
  if (std::holds_alternative<std::monostate>(gstate.pk_batch)) {
    gstate.pk_batch = gstate.index_source->CreatePkBatch();
  }

  size_t row_count = std::visit(
    [&](auto& pk) -> size_t {
      using T = std::decay_t<decltype(pk)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "pk_batch must be initialised");
        return 0;
      } else {
        auto on_sk_result = [&](const rocksdb::Slice&,
                                const rocksdb::PinnableSlice& val,
                                const rocksdb::Status& s) {
          if (s.ok()) {
            SDB_ASSERT(val.size() >= 2 && val[0] == secondary_key::kPKInValue);
            AppendPrimaryKey(pk,
                             std::string_view{val.data() + 1, val.size() - 1});
          } else {
            SDB_ASSERT(s.IsNotFound(), "SK lookup error: ", s.ToString());
          }
        };

        while (gstate.point_offset < scan.points.size()) {
          pk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            pk.EnsureInit(duckdb::Allocator::DefaultAllocator());
          }
          const size_t batch_start = gstate.point_offset;
          const size_t batch = std::min<size_t>(
            STANDARD_VECTOR_SIZE, scan.points.size() - batch_start);

          if (gstate.txn) {
            mgc.MultiGet(
              *gstate.txn,
              builder.BuildKeys(probe_col, scan.points, batch_start, batch),
              on_sk_result);
          } else {
            mgc.MultiGet(
              *db,
              builder.BuildKeys(probe_col, scan.points, batch_start, batch),
              on_sk_result);
          }

          gstate.point_offset += batch;
          if (PrimaryKeysSize(pk) > 0) {
            break;
          }
        }
        return PrimaryKeysSize(pk);
      }
    },
    gstate.pk_batch);

  if (gstate.point_offset >= scan.points.size()) {
    gstate.finished = true;
  }

  if (row_count == 0) {
    output.SetCardinality(0);
    return;
  }

  gstate.index_source->Materialize(context, gstate.pk_batch, 0, row_count,
                                   output);

  if (gstate.scan_rowid) {
    const auto row_base = gstate.produced_rows.load(std::memory_order_relaxed);
    auto* rowid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.rowid_output_idx]);
    for (size_t i = 0; i < row_count; ++i) {
      rowid_data[i] = static_cast<int64_t>(row_base + i);
    }
  }

  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value), duckdb::count_t(row_count));
  }

  output.SetChildCardinality(static_cast<duckdb::idx_t>(row_count));
  gstate.produced_rows.fetch_add(row_count, std::memory_order_relaxed);
}

}  // namespace sdb::connector
