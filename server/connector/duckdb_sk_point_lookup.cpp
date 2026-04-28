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
#include "connector/lookup.h"
#include "connector/multiget_context.hpp"
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

  std::vector<std::string> batch_pk_bytes;
  auto on_sk_result = [&](const rocksdb::Slice&,
                          const rocksdb::PinnableSlice& val,
                          const rocksdb::Status& s) {
    if (s.ok()) {
      SDB_ASSERT(val.size() >= 2 && val[0] == secondary_key::kPKInValue);
      batch_pk_bytes.emplace_back(val.data() + 1, val.size() - 1);
    } else {
      SDB_ASSERT(s.IsNotFound(), "SK lookup error: ", s.ToString());
    }
  };

  while (gstate.point_offset < scan.points.size()) {
    batch_pk_bytes.clear();
    const size_t batch_start = gstate.point_offset;
    const size_t batch =
      std::min<size_t>(STANDARD_VECTOR_SIZE, scan.points.size() - batch_start);

    if (gstate.txn) {
      mgc.MultiGet(
        *gstate.txn,
        builder.BuildKeys(probe_col, scan.points, batch_start, batch),
        on_sk_result);
    } else {
      mgc.MultiGet(
        *db, builder.BuildKeys(probe_col, scan.points, batch_start, batch),
        on_sk_result);
    }

    gstate.point_offset += batch;
    if (!batch_pk_bytes.empty()) {
      break;
    }
  }

  if (gstate.point_offset >= scan.points.size()) {
    gstate.finished = true;
  }

  if (batch_pk_bytes.empty()) {
    output.SetCardinality(0);
    return;
  }

  const size_t row_count = batch_pk_bytes.size();
  std::vector<std::string_view> views;
  views.reserve(row_count);
  for (const auto& pk : batch_pk_bytes) {
    views.emplace_back(pk);
  }

  LookupRows(context, bind_data, gstate.snapshot, gstate.projected_columns,
             gstate.projected_types, bind_data.column_ids, gstate.txn, views,
             gstate.file_lookup_session, output);

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
      duckdb::Value::BIGINT(gstate.tableoid_value));
  }

  output.SetCardinality(static_cast<duckdb::idx_t>(row_count));
  gstate.produced_rows.fetch_add(row_count, std::memory_order_relaxed);
}

}  // namespace sdb::connector
