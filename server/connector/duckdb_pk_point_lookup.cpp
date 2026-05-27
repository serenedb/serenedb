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

#include "connector/duckdb_pk_point_lookup.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_key_builder.hpp"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/multiget_context.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKPointLookupInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<PKPointLookupGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  state->pk_suffixes.resize(STANDARD_VECTOR_SIZE);
  state->pk_multiget_key_storage.resize(STANDARD_VECTOR_SIZE);
  state->pk_multiget_key_slices.resize(STANDARD_VECTOR_SIZE);
  state->pk_found_indices.reserve(STANDARD_VECTOR_SIZE);

  return state;
}

void PKPointLookupFunction(duckdb::ClientContext& /*context*/,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  auto& gstate = data.global_state->Cast<PKPointLookupGlobalState>();

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

  const auto& points = bind_data.scan_source->Cast<PkPointScan>().points;
  const size_t total = points.size();
  const size_t batch_start = gstate.point_offset;
  if (batch_start >= total) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  MultiGetContext mgc{*cf, ro};
  // PK point lookup only fires on rocksdb-backed binds.
  SDB_ASSERT(!bind_data.IsViewBacked());
  DuckDBPrimaryKeyBuilder builder{
    bind_data.As<TableScanBindData>().table->GetId()};
  DuckDBPKResultCollector collector;

  duckdb::idx_t found_count = 0;

  auto do_multiget = [&](std::span<const rocksdb::Slice> slices, auto&& cb) {
    size_t pos = 0;
    auto proc = [&](const rocksdb::Slice&, const rocksdb::PinnableSlice& val,
                    const rocksdb::Status& s) { cb(pos++, val, s); };
    if (gstate.txn) {
      mgc.MultiGet(*gstate.txn, slices, proc);
    } else {
      mgc.MultiGet(*db, slices, proc);
    }
  };

  // Find first real projected column (used as probe key).
  duckdb::idx_t first_proj = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t p = 0; p < gstate.projected_columns.size(); ++p) {
    if (gstate.projected_columns[p] != duckdb::DConstants::INVALID_INDEX) {
      first_proj = p;
      break;
    }
  }

  std::vector<size_t> present_rows;

  if (first_proj != duckdb::DConstants::INVALID_INDEX) {
    const auto col_id =
      bind_data.column_ids[gstate.projected_columns[first_proj]];
    auto& type = gstate.projected_types[first_proj];
    collector.Init(type, batch_size, output.data[first_proj]);
    auto slices = builder.BuildKeys(col_id, points, batch_start, batch_size);
    size_t found_idx = 0;
    do_multiget(slices, [&](size_t batch_idx, const rocksdb::PinnableSlice& val,
                            const rocksdb::Status& s) {
      if (s.ok()) {
        collector.Fill(batch_idx, found_idx++, val);
      } else {
        SDB_ASSERT(s.IsNotFound(), "RocksDB PK lookup failed: ", s.ToString());
      }
    });
    found_count = static_cast<duckdb::idx_t>(found_idx);
  } else {
    // No real column projected (rowid/tableoid only): probe first bind column
    // to determine which points exist. The deserialized value is discarded;
    // only collector.PresentRows() is consumed downstream.
    SDB_ASSERT(!bind_data.column_ids.empty());
    const auto& probe_type = bind_data.column_types[0];
    duckdb::Vector dummy{probe_type};
    collector.Init(probe_type, batch_size, dummy);
    auto slices = builder.BuildKeys(bind_data.column_ids[0], points,
                                    batch_start, batch_size);
    size_t found_idx = 0;
    do_multiget(slices, [&](size_t batch_idx, const rocksdb::PinnableSlice& val,
                            const rocksdb::Status& s) {
      if (s.ok()) {
        collector.Fill(batch_idx, found_idx++, val);
      } else {
        SDB_ASSERT(s.IsNotFound(), "RocksDB PK lookup failed: ", s.ToString());
      }
    });
    found_count = static_cast<duckdb::idx_t>(found_idx);
  }

  collector.Finish(found_count);
  auto prows = collector.PresentRows();
  present_rows.assign(prows.begin(), prows.end());

  // Fetch remaining real columns: patch key prefix for found rows only.
  if (first_proj != duckdb::DConstants::INVALID_INDEX) {
    for (duckdb::idx_t proj = first_proj + 1;
         proj < gstate.projected_columns.size(); ++proj) {
      const auto bind_col = gstate.projected_columns[proj];
      if (bind_col == duckdb::DConstants::INVALID_INDEX) {
        continue;
      }
      const auto col_id = bind_data.column_ids[bind_col];
      auto& type = gstate.projected_types[proj];
      auto slices = builder.BuildPresentKeys(col_id, present_rows);
      size_t j = 0;
      do_multiget(slices, [&](size_t, const rocksdb::PinnableSlice& val,
                              const rocksdb::Status& s) {
        if (s.IsNotFound()) {
          duckdb::FlatVector::ValidityMutable(output.data[proj]).SetInvalid(j);
        } else {
          SDB_ASSERT(s.ok(), "RocksDB PK lookup failed: ", s.ToString());
          DeserializeValueIntoDuckDB(val.ToStringView(), output.data[proj],
                                     type, j);
        }
        ++j;
      });
    }
  }

  SDB_ASSERT(!gstate.has_generated_pk);

  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value),
      duckdb::count_t(output.size()));
  }

  gstate.point_offset += batch_size;
  if (gstate.point_offset >= total) {
    gstate.finished = true;
  }
  output.SetChildCardinality(static_cast<duckdb::idx_t>(found_count));
  if (found_count > 0) {
    gstate.produced_rows.fetch_add(found_count, std::memory_order_relaxed);
  }
}

}  // namespace sdb::connector
