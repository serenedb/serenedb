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

#include "connector/duckdb_sk_full_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_table_function.h"
#include "connector/lookup.h"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SKFullScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);
  // The SK iterator is created lazily in SKFullScanFunction on first call.
  return state;
}

void SKFullScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SKFullScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  auto& sk_scan = bind_data.scan_source->Cast<SecondaryIndexScan>();

  // Lazy init: create SK iterator on first call.
  if (!gstate.sk_iterator) {
    auto& engine = GetServerEngine();
    auto* db = engine.db();
    auto* cf = RocksDBColumnFamilyManager::get(
      RocksDBColumnFamilyManager::Family::Default);

    std::string scan_prefix;
    secondary_key::AppendShardPrefix(scan_prefix, sk_scan.shard_id);
    secondary_key::AppendDummyColumnId(scan_prefix);

    gstate.sk_upper_bound = scan_prefix;
    for (auto it = gstate.sk_upper_bound.rbegin();
         it != gstate.sk_upper_bound.rend(); ++it) {
      if (static_cast<uint8_t>(*it) < 0xFF) {
        ++(*it);
        break;
      }
      *it = 0;
    }
    gstate.sk_upper_bound_slice = rocksdb::Slice{gstate.sk_upper_bound};

    rocksdb::ReadOptions ro;
    ro.snapshot = gstate.snapshot;
    ro.total_order_seek = true;
    ro.iterate_upper_bound = &gstate.sk_upper_bound_slice;

    if (gstate.txn) {
      gstate.sk_iterator.reset(gstate.txn->GetIterator(ro, cf));
    } else {
      gstate.sk_iterator.reset(db->NewIterator(ro, cf));
    }
    gstate.sk_iterator->Seek(scan_prefix);
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  auto& it = *gstate.sk_iterator;

  std::vector<std::string> pk_bytes;
  pk_bytes.reserve(batch_size);

  while (pk_bytes.size() < batch_size && it.Valid()) {
    auto key = it.key();
    auto val = it.value();

    SDB_ASSERT(val.size() >= 2);
    if (val[0] == secondary_key::kPKInValue) {
      pk_bytes.emplace_back(val.data() + 1, val.size() - 1);
    } else {
      SDB_ASSERT(val[0] == secondary_key::kPKInKey);
      uint8_t pk_size = static_cast<uint8_t>(val[1]);
      SDB_ASSERT(key.size() >= pk_size);
      pk_bytes.emplace_back(key.data() + key.size() - pk_size, pk_size);
    }
    it.Next();
  }
  rocksutils::CheckIteratorStatus(it);

  if (pk_bytes.empty()) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const auto num_rows = pk_bytes.size();
  std::vector<std::string_view> views(pk_bytes.begin(), pk_bytes.end());
  LookupRows(context, bind_data, gstate.snapshot, gstate.projected_columns,
             gstate.projected_types, bind_data.column_ids, gstate.txn, views,
             gstate.file_lookup_session, output);

  if (gstate.scan_rowid) {
    const auto row_base = gstate.produced_rows.load(std::memory_order_relaxed);
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.rowid_output_idx]);
    for (size_t i = 0; i < num_rows; ++i) {
      data[i] = static_cast<int64_t>(row_base + i);
    }
  }
  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value));
  }

  output.SetCardinality(num_rows);
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
}

}  // namespace sdb::connector
