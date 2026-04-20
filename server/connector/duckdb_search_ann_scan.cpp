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

#include "connector/duckdb_search_ann_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/search_remove_filter.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchAnnScanGlobalState>();
  InitCommonState(*state, context, bind_data, input);
  // HNSW search is lazy: executed on the first SearchAnnScanFunction call.
  return state;
}

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchAnnScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  // Lazy init: run HNSW search on first call.
  if (!gstate.ann_search_done) {
    gstate.ann_search_done = true;
    auto& ann = std::get<ANNScan>(bind_data.scan_source);
    auto& snapshot =
      GetSereneDBContext(context).EnsureSearchSnapshot(ann.index_id);

    if (snapshot.reader.size() > 0 && ann.top_k > 0) {
      const size_t top_k = ann.top_k;
      std::vector<float> dis(top_k, std::numeric_limits<float>::max());
      std::vector<int64_t> ids(top_k, -1);

      irs::HNSWSearchInfo info{
        .query =
          reinterpret_cast<const irs::byte_type*>(ann.query_vector.data()),
        .top_k = top_k,
      };
      snapshot.reader.Search(ann.field_name, info, dis.data(), ids.data());

      gstate.ann_pk_bytes.reserve(top_k);
      auto& reader = snapshot.reader;
      for (size_t i = 0; i < top_k; ++i) {
        if (ids[i] == -1) {
          continue;
        }
        auto [seg_id, doc_id] =
          irs::UnpackSegmentWithDoc(static_cast<uint64_t>(ids[i]));
        if (seg_id >= reader.size()) {
          continue;
        }
        const auto& segment = reader[seg_id];
        const auto* pk_col = segment.column(kPkFieldName);
        if (!pk_col) {
          continue;
        }
        auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
        if (!pk_iter) {
          continue;
        }
        const auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
        if (!pk_val) {
          continue;
        }
        if (pk_iter->seek(doc_id) != doc_id) {
          continue;
        }
        auto val = pk_val->value;
        gstate.ann_pk_bytes.emplace_back(
          reinterpret_cast<const char*>(val.data()), val.size());
      }
    }

    if (gstate.ann_pk_bytes.empty()) {
      gstate.finished = true;
      output.SetCardinality(0);
      return;
    }
  }

  const size_t total = gstate.ann_pk_bytes.size();
  const size_t batch_start = gstate.ann_current_idx;

  if (batch_start >= total) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, total - batch_start);

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  std::string table_key = key_utils::PrepareTableKey(bind_data.table->GetId());
  std::string key_buffer;
  rocksdb::ReadOptions ro;
  ro.snapshot = gstate.snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;
  rocksdb::PinnableSlice value;

  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    auto bind_col = gstate.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
        output.data[proj].Reference(
          duckdb::Value::BIGINT(gstate.tableoid_value));
      } else {
        auto* data =
          duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
        for (duckdb::idx_t i = 0; i < batch_size; ++i) {
          data[i] = static_cast<int64_t>(batch_start + i);
        }
      }
      continue;
    }

    auto col_id = bind_data.column_ids[bind_col];
    auto& type = gstate.projected_types[proj];

    for (duckdb::idx_t row = 0; row < batch_size; ++row) {
      const auto& pk = gstate.ann_pk_bytes[batch_start + row];
      key_buffer = table_key;
      basics::StrResize(key_buffer, key_utils::kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col_id);
      key_buffer.append(pk);

      value.Reset();
      auto s = db->Get(ro, cf, key_buffer, &value);
      if (s.IsNotFound()) {
        duckdb::FlatVector::Validity(output.data[proj]).SetInvalid(row);
        continue;
      }
      SDB_ASSERT(s.ok(), "RocksDB read failed: ", s.ToString());
      DeserializeValueIntoDuckDB(value.ToStringView(), output.data[proj], type,
                                 row);
    }
  }

  gstate.ann_current_idx += batch_size;
  output.SetCardinality(static_cast<duckdb::idx_t>(batch_size));
  if (batch_size > 0) {
    gstate.produced_rows.fetch_add(batch_size, std::memory_order_relaxed);
  }

  if (gstate.ann_current_idx >= total) {
    gstate.finished = true;
  }
}

}  // namespace sdb::connector
