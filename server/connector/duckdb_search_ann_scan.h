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

#pragma once

#include <algorithm>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <iresearch/formats/hnsw/hnsw_writer.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <memory>
#include <string>
#include <vector>

#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"

namespace sdb::connector {

struct SearchAnnScanGlobalState : public CommonScanGlobalState {
  const ANNScan* scan = nullptr;
  std::unique_ptr<ANNFilterContext> filter_ctx;

  const irs::IndexReader* reader = nullptr;
  int ef_search = 0;

  std::atomic_bool search_finished = false;

  // Variables for sending the result
  PrimaryKeyBatch pk_batch;
  size_t current_idx = 0;
  size_t total_results = 0;

  std::vector<float> dis;
  std::vector<int64_t> ids;
  std::atomic<float> global_kth_dis{std::numeric_limits<float>::max()};
  absl::Mutex m;

  size_t total_segments = 0;
  std::atomic_size_t remained_segments = 0;
  std::atomic_uint32_t next_segment = 0;

  std::vector<SegDoc> merged_seg_docs;
  std::vector<uint32_t> lookup_scratch;

  duckdb::idx_t MaxThreads() const override {
    return std::max<duckdb::idx_t>(1, total_segments / 2);
  }
};

struct SearchAnnScanLocalState : public CommonScanLocalState {
  SearchAnnScanLocalState(float* dis_data, int64_t* ids_data, size_t size)
    : buffer{dis_data, ids_data, size} {}
  irs::HNSWAnnSearchBuffer buffer;
  irs::Filter::Query::ptr text_filter_query;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

struct SearchRangeScanGlobalState : public CommonScanGlobalState {
  const RangeSearchScan* scan = nullptr;
  std::unique_ptr<ANNFilterContext> filter_ctx;

  const irs::IndexReader* reader = nullptr;
  int ef_search = 0;

  size_t total_segments = 0;
  std::atomic_uint32_t next_segment = 0;
  std::atomic_size_t total_results = 0;
  absl::Mutex m;

  duckdb::idx_t MaxThreads() const override {
    return std::max<duckdb::idx_t>(1, total_segments);
  }
};

struct SearchRangeScanLocalState : public CommonScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::vector<SegDoc> seg_docs;
  size_t current_idx = 0;
  irs::HNSWRangeSearchBuffer range_buffer;
  std::vector<uint32_t> lookup_scratch;
  irs::Filter::Query::ptr text_filter_query;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchRangeScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output);

}  // namespace sdb::connector
