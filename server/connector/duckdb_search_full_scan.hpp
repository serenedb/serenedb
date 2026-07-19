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

#include <absl/synchronization/notification.h>

#include <array>
#include <cstdint>
#include <duckdb.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "connector/offsets_collector.hpp"

namespace sdb::connector {

struct SearchFullScanTopKLocalState : public SegDocBufferedScanLocalState {
  std::vector<irs::ScoreDoc> hit_buf;
  std::span<irs::ScoreDoc> hit_slice;
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::lowest();
  irs::ColumnArgsFetcher score_fetcher;
  using Collector = irs::NthPartitionScoreCollector;
  std::variant<std::monostate, Collector> collector;
  std::span<const irs::ScoreDoc> top_hits;
  bool emit_prepared = false;

  void PrepareEmitBuffer(duckdb::ClientContext& ctx,
                         IResearchScanGlobalState& g);
};

struct SearchFullScanScanLocalState : public SegDocBufferedScanLocalState {
  irs::DocIterator::ptr streaming_doc;
  irs::ScoreFunction streaming_score_function;
  irs::ColumnArgsFetcher score_fetcher;

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, IResearchScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          IResearchScanGlobalState& g,
                          duckdb::DataChunk& output);

 protected:
  void PushHits(IResearchScanGlobalState& g);
};

// ColScan: bulk units read `.col` through per-segment FullScanners; a
// non-bulk unit (segment with deletes) runs the inherited masked streaming
// walk -- match-all, covered, unscored, so the batcher emit needs neither the
// lookup source nor offsets.
struct ColScanLocalState : public SearchFullScanScanLocalState {
  uint32_t current_seg_idx = 0;
  uint64_t bulk_doc_in_seg = 0;
  uint64_t bulk_seg_doc_count = 0;
  std::vector<std::unique_ptr<FullScanner>> full_scanners;

  void StartUnit(duckdb::ClientContext& ctx,
                 const IResearchScanGlobalState::ScanUnit& unit,
                 IResearchScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          IResearchScanGlobalState& g,
                          duckdb::DataChunk& output);

 private:
  FullScanner* OpenScanner(const IResearchScanGlobalState& g);
};

struct SearchFullScanCountLocalState : public IResearchScanLocalState {
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;
};

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             IResearchScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output);

duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx,
                            IResearchScanGlobalState& g,
                            SegDocBufferedScanLocalState& l,
                            duckdb::DataChunk& output, duckdb::idx_t collected);

bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx,
                           IResearchScanGlobalState& g,
                           SegDocBufferedScanLocalState& l,
                           std::span<const irs::ScoreDoc> hits,
                           size_t& current_idx, duckdb::DataChunk& output);

void RunCountScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                  SearchFullScanCountLocalState& l, duckdb::DataChunk& output);

void RunTopKScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                 SearchFullScanTopKLocalState& l, duckdb::DataChunk& output);

void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                      SearchFullScanScanLocalState& l,
                      duckdb::DataChunk& output);

void RunColScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                ColScanLocalState& l, duckdb::DataChunk& output);

struct TsDictLocalState : public IResearchScanLocalState {
  // Per enumerated field: its output column slots.
  struct FieldState {
    irs::field_id field_id = irs::field_limits::invalid();
    irs::field_id null_field_id = irs::field_limits::invalid();
    duckdb::idx_t term_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_slot = duckdb::DConstants::INVALID_INDEX;
    TsDictTermUses term_uses = TsDictTermUses::kNone;
    const irs::Filter* having_filter = nullptr;
  };

  enum class CountMode { Meta, Masked, Where };

  std::vector<FieldState> fields;
  CountMode count_mode = CountMode::Meta;
  const irs::QueryBuilder* where_query = nullptr;

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, IResearchScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          IResearchScanGlobalState& g,
                          duckdb::DataChunk& output,
                          duckdb::idx_t output_start);

 private:
  bool NextField();
  irs::TermIterator::ptr MakeTermSource(const FieldState& field,
                                        const irs::TermReader& reader);
  duckdb::idx_t EmitField(duckdb::DataChunk& output, duckdb::idx_t output_start,
                          duckdb::idx_t capacity);
  duckdb::idx_t AppendNullRow(duckdb::DataChunk& output,
                              const FieldState& field, duckdb::idx_t row);

  const irs::SubReader* _seg = nullptr;
  bool _null_pending = false;
  const FieldState* _field = nullptr;
  const FieldState* _next_field = nullptr;
  CountMode _cursor_mode = CountMode::Meta;
  irs::TermIterator::ptr _cursor;
};

void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                      TsDictLocalState& l, duckdb::DataChunk& output);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
