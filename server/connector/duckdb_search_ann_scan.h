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
#include <iresearch/formats/column/hnsw_index.hpp>
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

  int ef_search = 0;

  // Pre-sized to MaxThreads * top_k; threads carve a top_k slice via
  // fetch_add on next_slice_idx in InitLocal.
  std::vector<float> dis;
  std::vector<int64_t> ids;
  std::atomic<float> global_kth_dis{std::numeric_limits<float>::max()};
  std::atomic<size_t> next_slice_idx{0};

  duckdb::idx_t MaxThreads() const override {
    return std::max<duckdb::idx_t>(1, total_segments / 2);
  }
};

struct SearchAnnScanLocalState : public CommonScanLocalState {
  SearchAnnScanLocalState(float* dis_data, int64_t* ids_data, size_t size)
    : buffer{dis_data, ids_data, size} {}
  irs::HNSWAnnSearchBuffer buffer;
  irs::Filter::Query::ptr text_filter_query;

  // Post-collect emit state (filled by OnSegmentsExhausted, drained by
  // EmitNextChunk).
  std::vector<SegDoc> seg_docs;
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  std::vector<uint32_t> lookup_scratch;
  size_t current_idx = 0;

  const SereneDBScanBindData* bind_data = nullptr;
  CompositeScanFilter composite;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchAnnScanGlobalState& g);
  void OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchAnnScanGlobalState& g);
  EmitOutput EmitNextChunk(duckdb::ClientContext& ctx,
                           SearchAnnScanGlobalState& g,
                           duckdb::DataChunk& output);
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

  int ef_search = 0;
  std::atomic_size_t total_results = 0;

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
  std::shared_ptr<IndexSource> index_source;

  const SereneDBScanBindData* bind_data = nullptr;
  CompositeScanFilter composite;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchRangeScanGlobalState& g);
  EmitOutput EmitNextChunk(duckdb::ClientContext& ctx,
                           SearchRangeScanGlobalState& g,
                           duckdb::DataChunk& output);
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
