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
#include <iresearch/formats/hnsw/hnsw_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"

namespace sdb::connector {

struct SearchAnnScanGlobalState : public CommonScanGlobalState {
  const SearchScan* scan = nullptr;

  int ef_search = 0;

  // Top-K mode only: cross-thread threshold for ANN pruning.
  std::atomic<float> global_kth_dis{std::numeric_limits<float>::max()};

  duckdb::idx_t MaxThreads() const final {
    return std::max<duckdb::idx_t>(1, total_segments);
  }
};

struct SearchAnnTopKLocalState : public SegDocBufferedScanLocalState {
  std::vector<float> dis_buf;
  std::vector<int64_t> ids_buf;
  irs::HNSWAnnSearchBuffer buffer;
  irs::Filter::Query::ptr text_filter_query;
  std::optional<TextScanFilter> text_filter;

  explicit SearchAnnTopKLocalState(size_t k)
    : dis_buf(k), ids_buf(k), buffer{dis_buf.data(), ids_buf.data(), k} {}

  std::vector<irs::ScoreDoc> hits;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchAnnScanGlobalState& g);
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchAnnScanGlobalState& g,
                           duckdb::DataChunk& output);

 private:
  bool prepped = false;
  void PrepEmitBuffer(duckdb::ClientContext& ctx, SearchAnnScanGlobalState& g);
};

struct SearchAnnRangeLocalState : public SegDocBufferedScanLocalState {
  irs::HNSWRangeSearchBuffer range_buffer;
  std::vector<irs::ScoreDoc> hits;
  irs::Filter::Query::ptr text_filter_query;
  std::optional<TextScanFilter> text_filter;

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, SearchAnnScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          SearchAnnScanGlobalState& g,
                          duckdb::DataChunk& output,
                          duckdb::idx_t output_start);
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchAnnScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

}  // namespace sdb::connector
