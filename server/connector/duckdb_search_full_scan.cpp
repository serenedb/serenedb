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

#include "connector/duckdb_search_full_scan.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/utils/string.hpp>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_pk_lookup.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {
namespace {

const irs::Filter& MatchAllFilter() {
  static const std::shared_ptr<irs::Filter> kInstance = [] {
    auto root = std::make_shared<irs::And>();
    root->add<irs::All>();
    return root;
  }();
  return *kInstance;
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  const auto& ss = bind_data.scan_source->Cast<SearchScan>();
  state->scan = &ss;
  state->reader = &ss.snapshot->reader;
  state->total_segments = ss.snapshot->reader.size();

  if ((state->count_only = ss.count_only)) {
    if (ss.stored_filter) {
      state->query = ss.stored_filter->prepare({.index = ss.snapshot->reader});
    }
    return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  if (ss.text_scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
  }
  const auto& filter = ss.stored_filter ? *ss.stored_filter : MatchAllFilter();
  state->query = filter.prepare({
    .index = ss.snapshot->reader,
    .scorer = state->scorer_obj.get(),
  });

  if (ss.score_top_k && ss.text_scorer) {
    state->parallel_topk = true;
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

namespace {

template<typename Lstate>
void BuildOffsetsEntries(Lstate& lstate, duckdb::TableFunctionInitInput& input,
                         const SereneDBScanBindData& bd) {
  const auto& ss = bd.scan_source->Cast<SearchScan>();
  if (ss.offsets.empty()) {
    return;
  }
  std::vector<size_t> ss_idx_at_bind(bd.column_ids.size(),
                                     std::numeric_limits<size_t>::max());
  size_t k = 0;
  for (size_t i = 0; i < bd.column_ids.size(); ++i) {
    if (bd.column_ids[i] == catalog::Column::kInvertedIndexOffsetsId) {
      ss_idx_at_bind[i] = k++;
    }
  }
  duckdb::idx_t out_slot = 0;
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
        col_id >= duckdb::VIRTUAL_COLUMN_START) {
      ++out_slot;
      continue;
    }
    if (col_id >= bd.column_ids.size()) {
      continue;
    }
    if (bd.column_ids[col_id] == catalog::Column::kInvertedIndexOffsetsId) {
      const auto ss_idx = ss_idx_at_bind[col_id];
      SDB_ASSERT(ss_idx < ss.offsets.size());
      FieldEntry entry;
      entry.output_idx = out_slot;
      entry.limit = ss.offsets[ss_idx].limit;
      entry.id = static_cast<irs::field_id>(ss.offsets[ss_idx].column_id);
      lstate.offsets_entries.push_back(std::move(entry));
    }
    ++out_slot;
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchFullScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.count_only) {
    auto lstate = duckdb::make_uniq<SearchFullScanCountLocalState>();
    if (!gstate.query) {
      lstate->local_count = gstate.reader->live_docs_count();
      lstate->segments_exhausted = true;
    }
    return lstate;
  }
  if (gstate.parallel_topk) {
    auto lstate = duckdb::make_uniq<SearchFullScanTopKLocalState>();
    lstate->bind_data = &bd;
    const size_t k = *bd.scan_source->Cast<SearchScan>().score_top_k;
    lstate->hit_buf.resize(irs::BlockSize(k));
    lstate->hit_slice = std::span<irs::ScoreDoc>{lstate->hit_buf};
    BuildOffsetsEntries(*lstate, input, bd);
    return lstate;
  }
  auto lstate = duckdb::make_uniq<SearchFullScanScanLocalState>();
  lstate->bind_data = &bd;
  BuildOffsetsEntries(*lstate, input, bd);
  return lstate;
}

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  if (gstate.count_only) {
    auto& l = data.local_state->Cast<SearchFullScanCountLocalState>();
    RunCollectThenEmitScan(context, gstate, l, output);
  } else if (gstate.parallel_topk) {
    auto& l = data.local_state->Cast<SearchFullScanTopKLocalState>();
    RunCollectThenEmitScan(context, gstate, l, output);
  } else {
    auto& l = data.local_state->Cast<SearchFullScanScanLocalState>();
    RunStreamingScan(context, gstate, l, output);
  }
}

void IResearchSetScanOrder(
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  if (!bind_data || !options || !options->row_limit.IsValid()) {
    return;
  }
  auto& bd = bind_data->Cast<SereneDBScanBindData>();
  auto& search_scan = bd.scan_source->Cast<SearchScan>();
  if (search_scan.score_top_k) {
    return;
  }
  if (search_scan.text_scorer) {
    if (options->order_type != duckdb::OrderType::DESCENDING) {
      return;
    }
    search_scan.score_top_k = options->row_limit.GetIndex();
    return;
  }
  if (search_scan.vector_scorer) {
    if (options->order_type != search_scan.vector_scorer->natural_order) {
      return;
    }
    search_scan.score_top_k = options->row_limit.GetIndex();
  }
}

void SearchFullScanTopKLocalState::OnSegment(duckdb::ClientContext& ctx,
                                             const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             SearchFullScanGlobalState& g) {
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  if (!collector) {
    const size_t k = *search.score_top_k;
    collector.emplace(local_threshold, k, hit_slice);
  }

  score_fetcher.Clear();
  collector->SetSegment(seg_idx);

  const auto seen_global = g.global_kth_score.load(std::memory_order_relaxed);
  if (seen_global > local_threshold) {
    local_threshold = seen_global;
  }

  const bool wand_enabled =
    WandEnabled(bind_data->inverted_index.get(), search.text_scorer);
  auto it = seg.mask(g.query->execute({
    .segment = seg,
    .scorer = g.scorer_obj.get(),
    .wand = {.wand_enabled = wand_enabled},
  }));
  auto score_func = it->PrepareScore({
    .scorer = g.scorer_obj.get(),
    .segment = &seg,
    .fetcher = &score_fetcher,
  });
  if (auto* it_threshold = irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
    collector->SetScoreThreshold(it_threshold->value);
  }
  it->Collect(score_func, score_fetcher, *collector);
  collector->SetScoreThreshold(local_threshold);

  const irs::score_t kth = local_threshold;
  auto cur = g.global_kth_score.load(std::memory_order_relaxed);
  while (kth > cur && !g.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

bool SearchFullScanTopKLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (!prepped) {
    PrepEmitBuffer(ctx, g);
  }

  const size_t remaining = top_hits.size() - current_idx;
  if (remaining == 0) {
    return false;
  }
  const auto take = std::min<size_t>(remaining, STANDARD_VECTOR_SIZE);
  const ScoreDocsView view{top_hits.subspan(current_idx, take)};
  const auto emitted = MaterializeChunk(ctx, g, *this, view, output);
  current_idx += take;
  return emitted > 0;
}

void SearchFullScanTopKLocalState::PrepEmitBuffer(
  duckdb::ClientContext& /*ctx*/, SearchFullScanGlobalState& g) {
  prepped = true;
  if (!collector) {
    return;  // no segments claimed by this thread
  }

  const size_t accepted = collector->AcceptedCount();
  auto accepted_slice = hit_slice.subspan(0, accepted);
  absl::c_sort(
    accepted_slice, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
      return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
    });
  top_hits = hit_slice.subspan(0, accepted);
}

void SearchFullScanScanLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                             const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             SearchFullScanGlobalState& g) {
  current_seg_idx = seg_idx;
  const bool has_real = absl::c_any_of(g.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });
  current_segment_bulk = has_real && !g.scan_score &&
                         !g.has_external_projections && g.scan->IsMatchAll() &&
                         !g.scan->EmitOffsets() &&
                         seg.live_docs_count() == seg.docs_count();

  if (current_segment_bulk) {
    bulk_doc_in_seg = 0;
    bulk_seg_doc_count = seg.docs_count();
    streaming_doc.reset();
    return;
  }

  streaming_doc.reset();
  streaming_doc = seg.mask(g.query->execute({
    .segment = seg,
    .scorer = g.scorer_obj.get(),
  }));

  if (g.has_external_projections &&
      !SegmentPkColumn(*g.reader, seg_idx).second) {
    streaming_doc.reset();
    return;
  }

  if (g.scan_score) {
    score_fetcher.Clear();
    streaming_score_function = streaming_doc->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &score_fetcher,
    });
  }
}

void SearchFullScanScanLocalState::AdvanceChunk(SearchFullScanGlobalState& g) {
  chunk_hits.clear();
  chunk_scores.clear();
  if (!streaming_doc) {
    return;
  }
  chunk_hits.reserve(STANDARD_VECTOR_SIZE);

  if (!g.scan_score) {
    while (chunk_hits.size() < STANDARD_VECTOR_SIZE) {
      auto doc_id = streaming_doc->advance();
      if (irs::doc_limits::eof(doc_id)) {
        streaming_doc.reset();
        return;
      }
      chunk_hits.push_back(doc_id);
    }
    return;
  }

  static_assert(STANDARD_VECTOR_SIZE % irs::kScoreBlock == 0,
                "kScoreBlock must divide STANDARD_VECTOR_SIZE");
  chunk_scores.reserve(STANDARD_VECTOR_SIZE);

  std::array<irs::doc_id_t, irs::kScoreBlock> block_docs;
  irs::scores_size_t block_count = 0;
  auto flush = [&](bool full) {
    if (!full && block_count == 0) {
      return;
    }
    score_fetcher.Fetch({block_docs.data(), block_count});
    const size_t pos = chunk_scores.size();
    chunk_scores.resize(pos + block_count);
    if (full) {
      streaming_score_function.ScoreBlock(&chunk_scores[pos]);
    } else {
      streaming_score_function.Score(&chunk_scores[pos], block_count);
    }
    block_count = 0;
  };

  while (chunk_hits.size() < STANDARD_VECTOR_SIZE) {
    auto doc_id = streaming_doc->advance();
    if (irs::doc_limits::eof(doc_id)) {
      flush(false);
      streaming_score_function = {};
      streaming_doc.reset();
      break;
    }
    streaming_doc->FetchScoreArgs(block_count);
    block_docs[block_count++] = doc_id;
    chunk_hits.push_back(doc_id);
    if (block_count == irs::kScoreBlock) {
      flush(true);
    }
  }
  if (block_count != 0) {
    flush(false);
  }
  SDB_ASSERT(chunk_scores.size() == chunk_hits.size());
}

bool SearchFullScanScanLocalState::EmitNextChunk(duckdb::ClientContext& ctx,
                                                 SearchFullScanGlobalState& g,
                                                 duckdb::DataChunk& output) {
  if (current_segment_bulk) {
    if (bulk_doc_in_seg >= bulk_seg_doc_count) {
      return false;
    }
    auto* mat =
      GetOrOpenSegmentMaterializer(*this, g, *g.reader, current_seg_idx);
    SDB_ENSURE(mat, sdb::ERROR_INTERNAL,
               "bulk cs scan: segment has no columnstore reader");
    const auto take = std::min<duckdb::idx_t>(
      STANDARD_VECTOR_SIZE, bulk_seg_doc_count - bulk_doc_in_seg);
    mat->Scan(bulk_doc_in_seg, take, output);
    const auto row_base =
      g.produced_rows.fetch_add(take, std::memory_order_relaxed);
    WriteVirtualColumns(g, row_base, take, ScoreDocsView{}, output);
    output.SetChildCardinality(take);
    bulk_doc_in_seg += take;
    return true;
  }

  AdvanceChunk(g);
  if (chunk_hits.empty()) {
    return false;
  }

  SDB_IF_FAILURE("SearchRocksDBLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }

  const StreamingHitsView view{
    .docs = {chunk_hits.data(), chunk_hits.size()},
    .scores = g.scan_score ? std::span<const float>{chunk_scores.data(),
                                                    chunk_scores.size()}
                           : std::span<const float>{},
    .fixed_seg = current_seg_idx,
  };
  const auto emitted = MaterializeChunk(ctx, g, *this, view, output);
  return emitted > 0;
}

void SearchFullScanCountLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                              const irs::SubReader& seg,
                                              uint32_t /*seg_idx*/,
                                              SearchFullScanGlobalState& g) {
  auto doc = seg.mask(g.query->execute({.segment = seg}));
  local_count += doc->count();
}

bool SearchFullScanCountLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& /*ctx*/, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (local_emitted >= local_count) {
    return false;
  }
  const auto batch =
    std::min<duckdb::idx_t>(local_count - local_emitted, STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  local_emitted += batch;
  return true;
}

}  // namespace sdb::connector
