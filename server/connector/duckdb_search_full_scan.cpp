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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/dfi.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/indri_dirichlet.hpp>
#include <iresearch/search/lm_dirichlet.hpp>
#include <iresearch/search/lm_jelinek_mercer.hpp>
#include <iresearch/search/raw_tf.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/utils/string.hpp>
#include <ranges>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/mangling.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/key_utils.hpp"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/pk_batch_helpers.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_pk_lookup.h"
#include "connector/search_remove_filter.hpp"
#include "query/duckdb_engine.h"
#include "rocksdb/db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

// Rebuild per-field sub-filter state for a new segment via OffsetsCollector.
static void ResetOffsetsForSegment(SearchFullScanLocalState& lstate,
                                   const irs::Filter::Query& query,
                                   const irs::SubReader& segment) {
  for (auto& entry : lstate.offsets_entries) {
    entry.state.Clear();
  }
  OffsetsCollector visitor{lstate.offsets_entries};
  query.visit(segment, visitor, irs::kNoBoost);
}

static void WriteOffsetsRow(SearchFullScanLocalState& lstate,
                            const irs::SubReader& segment, irs::doc_id_t doc_id,
                            duckdb::DataChunk& output, duckdb::idx_t row_idx) {
  // entry.limit is stashed at init time because UNUSED_COLUMNS pruning
  // can shrink offsets_entries vs. search.offsets, breaking index alignment.
  for (auto& entry : lstate.offsets_entries) {
    FillRowOffsets(entry.state, segment, doc_id, entry.limit,
                   lstate.offsets_doc_scratch);
    WriteRowOffsets(output.data[entry.output_idx], row_idx,
                    lstate.offsets_doc_scratch);
  }
}

static void WriteTopkOffsets(SearchFullScanLocalState& lstate,
                             const irs::Filter::Query& query,
                             const irs::IndexReader& reader,
                             duckdb::DataChunk& output,
                             std::span<const irs::ScoreDoc> hit_slice) {
  for (const auto& entry : lstate.offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }

  VisitSegmentsSorted(
    hit_slice,
    [](const irs::ScoreDoc& sd) { return std::pair{sd.segment_idx, sd.doc}; },
    lstate.lookup_scratch,
    [&](uint32_t seg) {
      ResetOffsetsForSegment(lstate, query, reader[seg]);
      return true;
    },
    [&](uint32_t orig, uint32_t seg, uint32_t doc) {
      WriteOffsetsRow(lstate, reader[seg], doc, output, orig);
    });
}

// Promote FullTableScan to a match-all SearchScan so the rest of init
// can assume a SearchScan. Triggered by bare `SELECT * FROM idx;`.
static void EnsureDefaultMatchAllSearchScan(SereneDBScanBindData& bind_data) {
  if (bind_data.scan_source->Kind() == ScanSourceKind::Search) {
    return;
  }
  SDB_ASSERT(bind_data.IsInvertedIndexEntry());
  SDB_ASSERT(bind_data.inverted_index);

  auto cat_snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::shared_ptr<search::InvertedIndexShard> shard;
  for (auto& s : cat_snapshot->GetIndexShardsByRelation(
         bind_data.inverted_index->GetRelationId())) {
    if (s->GetIndexId() == bind_data.inverted_index->GetId() &&
        s->GetType() == catalog::ObjectType::InvertedIndexShard) {
      shard = basics::downCast<search::InvertedIndexShard>(std::move(s));
      break;
    }
  }
  SDB_ASSERT(shard);
  auto idx_snapshot = shard->GetInvertedIndexSnapshot();
  SDB_ASSERT(idx_snapshot);

  auto root = std::make_shared<irs::And>();
  root->add<irs::All>();
  auto search = std::make_unique<SearchScan>();
  search->snapshot = std::move(idx_snapshot);
  search->stored_filter = root;
  search->filter_summary = "All";
  search->match_all = true;
  bind_data.scan_source = std::move(search);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = const_cast<SereneDBScanBindData&>(
    input.bind_data->Cast<SereneDBScanBindData>());
  EnsureDefaultMatchAllSearchScan(bind_data);
  auto state = duckdb::make_uniq<SearchFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  const auto& ss = bind_data.scan_source->Cast<SearchScan>();
  if (ss.scorer) {
    state->scorer_obj = catalog::MakeScorer(*ss.scorer);
  }
  // Single prepare site -- scorer attached up front so IDF/norm stats
  // are collected once and filters that mutate options() (GeoFilter)
  // don't get re-prepared with stale state.
  SDB_ASSERT(ss.stored_filter);
  SDB_ASSERT(ss.snapshot);
  state->query = ss.stored_filter->prepare({
    .index = ss.snapshot->reader,
    .scorer = state->scorer_obj.get(),
  });

  state->reader = &ss.snapshot->reader;
  state->total_segments = ss.snapshot->reader.size();

  // parallel_topk must be set before MaxThreads() is queried for the
  // hits-buffer sizing below.
  if (ss.score_top_k && ss.scorer) {
    state->parallel_topk = true;
    const size_t k = *ss.score_top_k;
    state->hits.resize(state->MaxThreads() * irs::BlockSize(k));
  }

  ClassifyColumnstoreProjections(*state, bind_data);

  // Decide bulk-cs-scan eligibility here so MaxThreads() picks the right
  // parallelism before InitLocal runs.
  if (!state->parallel_topk) {
    const bool has_real = absl::c_any_of(state->projected_columns, [](auto p) {
      return p != duckdb::DConstants::INVALID_INDEX;
    });
    if (has_real && !state->scan_score && !ss.score_top_k &&
        !ss.EmitOffsets() && !state->has_external_projections && ss.match_all) {
      bool any_masked = false;
      for (size_t si = 0; si < state->reader->size(); ++si) {
        if ((*state->reader)[si].live_docs_count() !=
            (*state->reader)[si].docs_count()) {
          any_masked = true;
          break;
        }
      }
      if (!any_masked) {
        state->bulk_scan_active = true;
      }
    }
  }

  return duckdb::unique_ptr_cast<SearchFullScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchFullScanGlobalState>();
  auto lstate = duckdb::make_uniq<SearchFullScanLocalState>();
  lstate->bind_data = &input.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.parallel_topk) {
    // Parallel top-K: claim a BlockSize(top_k)-sized slice out of the
    // shared gstate.hits buffer (pre-sized to MaxThreads * BlockSize(k)
    // in InitGlobal). The slice is owned by the gstate vector; the
    // lstate just holds a span into it.
    const size_t k_block = gstate.hits.size() / gstate.MaxThreads();
    const auto slot =
      gstate.next_slice_idx.fetch_add(1, std::memory_order_relaxed);
    lstate->hit_slice =
      std::span<irs::ScoreDoc>{gstate.hits}.subspan(slot * k_block, k_block);
  }

  // Per-thread offsets entries -- shared gstate would race in top-K.
  const auto& bd = *lstate->bind_data;
  const auto& ss = bd.scan_source->Cast<SearchScan>();
  if (!ss.offsets.empty()) {
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
        MakeFieldName(ss.offsets[ss_idx].column_id, entry.name);
        search::mangling::MangleString(entry.name);
        lstate->offsets_entries.push_back(std::move(entry));
      }
      ++out_slot;
    }
  }
  return lstate;
}

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchFullScanGlobalState>();
  auto& lstate = data.local_state->Cast<SearchFullScanLocalState>();
  SDB_ASSERT(gstate.query);
  // Single dispatch -- lstate hooks branch on parallel_topk / bulk_scan_active.
  RunParallelInvertedIndexScan(context, gstate, lstate, output);
}

bool SereneDBSetTopNHint(duckdb::ClientContext& context,
                         duckdb::LogicalTopN& top_n, duckdb::LogicalGet& get,
                         duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  if (top_n.limit == 0 || top_n.offset != 0 || top_n.orders.size() != 1) {
    return false;
  }
  if (!bind_data) {
    return false;
  }
  auto& bd = bind_data->Cast<SereneDBScanBindData>();
  // FullTable → try ANN rewrite. Search → stamp score_top_k for WAND.
  switch (bd.scan_source->Kind()) {
    case ScanSourceKind::FullTable:
      return optimizer::TryAttachAnnTopK(context, top_n, get);
    case ScanSourceKind::Search:
      break;
    default:
      return false;
  }
  if (top_n.orders[0].type != duckdb::OrderType::DESCENDING) {
    return false;
  }
  auto& search_scan = bd.scan_source->Cast<SearchScan>();
  if (!search_scan.scorer) {
    return false;  // No scoring requested -- nothing to prune.
  }
  if (search_scan.score_top_k) {
    return false;  // Idempotent: already pulled.
  }
  auto& order_expr = *top_n.orders[0].expression;
  if (order_expr.type != duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  const auto order_binding =
    order_expr.Cast<duckdb::BoundColumnRefExpression>().binding;
  SDB_ASSERT(!top_n.children.empty());
  if (!optimizer::BindingIsScoreColumn(*top_n.children[0], order_binding)) {
    return false;
  }
  search_scan.score_top_k = static_cast<size_t>(top_n.limit);
  return true;
}

// SearchFullScanLocalState hooks. Each dispatches on parallel_topk /
// bulk_scan_active to the per-mode helper.
void SearchFullScanLocalState::OnSegment(duckdb::ClientContext& ctx,
                                         const irs::SubReader& seg,
                                         uint32_t seg_idx,
                                         SearchFullScanGlobalState& g) {
  if (g.parallel_topk) {
    OnSegmentTopK(seg, seg_idx, g);
  } else if (g.bulk_scan_active) {
    OnSegmentBulk(seg, seg_idx, g);
  } else {
    OnSegmentStreaming(ctx, seg, seg_idx, g);
  }
}

void SearchFullScanLocalState::OnSegmentTopK(const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             SearchFullScanGlobalState& g) {
  SDB_ASSERT(bind_data);
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  if (!collector) {
    SDB_ASSERT(g.parallel_topk);
    const size_t k = *search.score_top_k;
    collector.emplace(local_threshold, k, hit_slice);
  }

  score_fetcher.Clear();
  collector->SetSegment(seg_idx);

  // Pull tightened global threshold before opening the iterator.
  const auto seen_global = g.global_kth_score.load(std::memory_order_relaxed);
  if (seen_global > local_threshold) {
    local_threshold = seen_global;
  }

  const bool wand_enabled = search.WandEnabled();
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
  // Rebind threshold back to the lstate slot (iterator-local one goes
  // out of scope).
  collector->SetScoreThreshold(local_threshold);

  // CAS-push tightened floor so other threads' WAND can prune harder.
  const irs::score_t kth = local_threshold;
  auto cur = g.global_kth_score.load(std::memory_order_relaxed);
  while (kth > cur && !g.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

void SearchFullScanLocalState::OnSegmentsExhausted(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g) {
  if (g.parallel_topk) {
    OnSegmentsExhaustedTopK(ctx, g);
  }
  // Streaming / bulk: nothing to prep. Skeleton sets segments_exhausted
  // after this returns, which flips EmitNextChunk into Finished mode.
}

void SearchFullScanLocalState::OnSegmentsExhaustedTopK(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g) {
  if (!collector) {
    return;  // no segments claimed by this thread
  }
  SDB_ASSERT(bind_data);
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  SDB_ASSERT(g.reader);

  const size_t accepted = collector->AcceptedCount();
  auto accepted_begin = hit_slice.begin();
  auto accepted_end = accepted_begin + static_cast<std::ptrdiff_t>(accepted);
  // ColumnReader::ScanCursor is forward-only -- sort by (seg, doc).
  std::sort(accepted_begin, accepted_end,
            [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
              return std::pair{l.segment_idx, l.doc} <
                     std::pair{r.segment_idx, r.doc};
            });

  top_hits.assign(accepted_begin, accepted_end);
  topk_scores.resize(accepted);
  for (size_t i = 0; i < accepted; ++i) {
    topk_scores[i] = top_hits[i].score;
  }

  if (g.has_external_projections && accepted > 0) {
    if (!index_source) {
      index_source = MakeIndexSource(
        ctx, *bind_data, /*snapshot=*/nullptr, /*txn=*/nullptr,
        g.external_projected_columns, g.projected_types, bind_data->column_ids);
    }
    if (std::holds_alternative<std::monostate>(pk_batch)) {
      pk_batch = index_source->CreatePkBatch();
    }
    std::visit(
      [&](auto& topk) {
        using T = std::decay_t<decltype(topk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
        } else {
          topk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            topk.EnsureInit(duckdb::Allocator::DefaultAllocator());
          }
          PkResize(topk, accepted);
          LookupSegmentsValues(
            std::span<const irs::ScoreDoc>{top_hits.data(), accepted},
            [](const irs::ScoreDoc& sd) {
              return std::pair{sd.segment_idx, static_cast<uint32_t>(sd.doc)};
            },
            *g.reader, lookup_scratch,
            [&](size_t orig, std::string_view pk) {
              SetPrimaryKey(topk, orig, pk);
            });
          // Always compact top_hits alongside topk_scores so the trailing
          // seg_docs build below stays aligned with the resolved-PK rows.
          // (The pre-skeleton code only compacted top_hits when offsets
          // were emitted, which made it possible for the cs INCLUDE path
          // to read stale entries on RocksDB lookup misses.)
          PkCompactResolved(topk, accepted, topk_scores, top_hits);
        }
      },
      pk_batch);
  }

  // Aligned 1:1 with topk_scores for EmitChunkFromBuffer.
  seg_docs.resize(topk_scores.size());
  for (size_t i = 0; i < topk_scores.size(); ++i) {
    seg_docs[i] = {
      .segment_idx = top_hits[i].segment_idx,
      .doc_pos =
        static_cast<irs::doc_id_t>(top_hits[i].doc - irs::doc_limits::min()),
    };
  }
  (void)search;
}

EmitOutput SearchFullScanLocalState::EmitNextChunk(duckdb::ClientContext& ctx,
                                                   SearchFullScanGlobalState& g,
                                                   duckdb::DataChunk& output) {
  if (g.parallel_topk) {
    return EmitNextChunkTopK(ctx, g, output);
  }
  if (g.bulk_scan_active) {
    return EmitNextChunkBulk(g, output);
  }
  return EmitNextChunkStreaming(ctx, g, output);
}

EmitOutput SearchFullScanLocalState::EmitNextChunkTopK(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (!segments_exhausted) {
    return EmitOutput::NeedsSegment;
  }
  SDB_ASSERT(bind_data);
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  SDB_ASSERT(g.reader);

  const size_t remaining = topk_scores.size() - current_idx;
  if (remaining == 0) {
    return EmitOutput::Finished;
  }
  const auto num_rows = static_cast<duckdb::idx_t>(
    std::min<size_t>(remaining, STANDARD_VECTOR_SIZE));

  // Offsets are BM25-only; written before EmitChunkFromBuffer fills the rest.
  if (search.EmitOffsets()) {
    std::span<const irs::ScoreDoc> hit_slice{top_hits.data() + current_idx,
                                             num_rows};
    SDB_ASSERT(g.query);
    WriteTopkOffsets(*this, *g.query, *g.reader, output, hit_slice);
  }
  return EmitChunkFromBuffer(ctx, g, *this, seg_docs,
                             std::span<const float>{topk_scores}, output) > 0
           ? EmitOutput::Chunk
           : EmitOutput::Finished;
}

void SearchFullScanLocalState::OnSegmentStreaming(
  duckdb::ClientContext& /*ctx*/, const irs::SubReader& seg, uint32_t seg_idx,
  SearchFullScanGlobalState& g) {
  SDB_ASSERT(bind_data);
  SDB_ASSERT(g.query);
  streaming_seg_idx = seg_idx;
  streaming_doc.reset();
  streaming_doc = seg.mask(g.query->execute({
    .segment = seg,
    .scorer = g.scorer_obj.get(),
  }));

  if (g.has_external_projections) {
    const auto [cs_reader, pk_col] = SegmentPkColumn(*g.reader, seg_idx);
    if (!pk_col) {
      // Segment has no PK column -- can't materialise external columns.
      // Drop the iterator so EmitNextChunkStreaming returns false and
      // the skeleton claims the next segment.
      streaming_doc.reset();
      return;
    }
    if (!streaming_segment_pk) {
      streaming_segment_pk =
        std::make_unique<SegmentPkSequentialFetcher>(*cs_reader, *pk_col);
    } else {
      streaming_segment_pk->Reset(*cs_reader, *pk_col);
    }
  }

  if (g.scan_score) {
    score_fetcher.Clear();
    streaming_score_function = streaming_doc->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &score_fetcher,
    });
  }

  auto& search = bind_data->scan_source->Cast<SearchScan>();
  if (search.EmitOffsets()) {
    for (auto& entry : offsets_entries) {
      entry.state.Clear();
    }
    OffsetsCollector visitor{offsets_entries};
    g.query->visit(seg, visitor, irs::kNoBoost);
  }
}

EmitOutput SearchFullScanLocalState::EmitNextChunkStreaming(
  duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (!streaming_doc) {
    // segment EOF (or had no PK col). If OnSegmentsExhausted already ran, we're
    // truly done; otherwise ask the skeleton to claim the next live segment.
    return segments_exhausted ? EmitOutput::Finished : EmitOutput::NeedsSegment;
  }
  SDB_ASSERT(bind_data);
  SDB_ASSERT(g.reader);
  auto& search = bind_data->scan_source->Cast<SearchScan>();
  const auto& seg = (*g.reader)[streaming_seg_idx];

  // Reset offsets list vectors for this chunk.
  if (search.EmitOffsets()) {
    for (const auto& entry : offsets_entries) {
      auto& list_vec = output.data[entry.output_idx];
      list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
      duckdb::ListVector::SetListSize(list_vec, 0);
      auto& child = duckdb::ListVector::GetChildMutable(list_vec);
      child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    }
  }

  static_assert(STANDARD_VECTOR_SIZE % irs::kScoreBlock == 0,
                "kScoreBlock must divide STANDARD_VECTOR_SIZE");
  float* score_data = g.scan_score ? duckdb::FlatVector::GetDataMutable<float>(
                                       output.data[g.score_output_idx])
                                   : nullptr;
  duckdb::idx_t score_pos = 0;
  std::array<irs::doc_id_t, irs::kScoreBlock> score_block_docs;
  irs::scores_size_t block_count = 0;
  auto flush_score_block = [&]() {
    if (!score_data || block_count == 0) {
      return;
    }
    score_fetcher.Fetch({score_block_docs.data(), block_count});
    streaming_score_function.Score(&score_data[score_pos], block_count);
    score_pos += block_count;
    block_count = 0;
  };

  const bool need_pk = g.has_external_projections;
  if (need_pk) {
    if (!index_source) {
      index_source = MakeIndexSource(
        ctx, *bind_data, /*snapshot=*/nullptr, /*txn=*/nullptr,
        g.external_projected_columns, g.projected_types, bind_data->column_ids);
    }
    if (std::holds_alternative<std::monostate>(pk_batch)) {
      pk_batch = index_source->CreatePkBatch();
    }
  }

  duckdb::idx_t collected = 0;
  std::vector<irs::doc_id_t> chunk_docs;
  streaming_chunk_doc_ids.clear();

  auto run_chunk = [&](auto&& pk_collect) {
    while (collected < STANDARD_VECTOR_SIZE) {
      // Iterator may have EOF'd at the end of a previous outer iteration
      // (we still emit the partial chunk). Don't call advance() on a
      // null iterator -- break out so the skeleton claims the next
      // segment on the next Function call.
      if (!streaming_doc) {
        break;
      }
      chunk_docs.clear();
      const duckdb::idx_t quota = STANDARD_VECTOR_SIZE - collected;
      while (chunk_docs.size() < quota) {
        auto doc_id = streaming_doc->advance();
        if (irs::doc_limits::eof(doc_id)) {
          flush_score_block();
          if (g.scan_score) {
            streaming_score_function = irs::ScoreFunction{};
          }
          streaming_doc.reset();
          break;
        }
        if (score_data) {
          streaming_doc->FetchScoreArgs(block_count);
          score_block_docs[block_count++] = doc_id;
          if (block_count == irs::kScoreBlock) {
            score_fetcher.Fetch({score_block_docs.data(), block_count});
            streaming_score_function.ScoreBlock(&score_data[score_pos]);
            score_pos += irs::kScoreBlock;
            block_count = 0;
          }
        }
        chunk_docs.push_back(doc_id);
      }
      if (chunk_docs.empty()) {
        break;
      }

      const duckdb::string_t* pk_data = nullptr;
      if (need_pk) {
        if (!streaming_pk_vec) {
          streaming_pk_vec = std::make_unique<duckdb::Vector>(
            duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE);
        }
        streaming_segment_pk->Fetch(chunk_docs, *streaming_pk_vec, 0);
        pk_data =
          duckdb::FlatVector::GetData<duckdb::string_t>(*streaming_pk_vec);
      }

      for (size_t i = 0; i < chunk_docs.size(); ++i) {
        const auto doc_id = chunk_docs[i];
        if (need_pk) {
          std::string_view pk_bytes{pk_data[i].GetData(),
                                    static_cast<size_t>(pk_data[i].GetSize())};
          SDB_ENSURE(!pk_bytes.empty(), ERROR_INTERNAL);
          pk_collect(pk_bytes);
        }
        if (search.EmitOffsets()) {
          for (size_t e = 0; e < search.offsets.size(); ++e) {
            auto& entry = offsets_entries[e];
            FillRowOffsets(entry.state, seg, doc_id, search.offsets[e].limit,
                           offsets_doc_scratch);
            WriteRowOffsets(output.data[entry.output_idx], collected,
                            offsets_doc_scratch);
          }
        }
        if (!g.cs_projections.empty()) {
          streaming_chunk_doc_ids.push_back(doc_id - irs::doc_limits::min());
        }
        ++collected;
      }
    }
    flush_score_block();
  };

  if (need_pk) {
    std::visit(
      [&](auto& pk) {
        using T = std::decay_t<decltype(pk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
        } else {
          pk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            pk.EnsureInit(duckdb::Allocator::DefaultAllocator());
          }
          run_chunk(
            [&](std::string_view pk_bytes) { AppendPrimaryKey(pk, pk_bytes); });
        }
      },
      pk_batch);
  } else {
    run_chunk([](std::string_view) {});
  }

  if (collected == 0) {
    // Iterator EOF'd without producing rows. Need another segment unless
    // we've already segments_exhausted.
    return segments_exhausted ? EmitOutput::Finished : EmitOutput::NeedsSegment;
  }

  if (need_pk) {
    SDB_IF_FAILURE("SearchRocksDBLookupFault") { SDB_THROW(ERROR_DEBUG); }
    index_source->Materialize(ctx, pk_batch, 0, collected, output);
  }
  if (!g.cs_projections.empty() && !streaming_chunk_doc_ids.empty()) {
    auto* mat = GetOrOpenSegmentMaterializer(g, *g.reader, streaming_seg_idx);
    if (mat && mat->HasAny()) {
      mat->SelectByDocIds(streaming_chunk_doc_ids, output, 0);
    }
  }
  WriteVirtualColumns(g, collected, output, std::span<const float>{});
  output.SetCardinality(collected);
  g.produced_rows.fetch_add(collected, std::memory_order_relaxed);
  return EmitOutput::Chunk;
}

// Bulk-cs-scan: skips iresearch; OnSegmentBulk stashes the cursor,
// EmitNextChunkBulk drains it via ColumnSegment::Scan.
void SearchFullScanLocalState::OnSegmentBulk(const irs::SubReader& seg,
                                             uint32_t seg_idx,
                                             SearchFullScanGlobalState& /*g*/) {
  bulk_seg_idx = seg_idx;
  bulk_doc_in_seg = 0;
  bulk_seg_doc_count = seg.docs_count();
}

EmitOutput SearchFullScanLocalState::EmitNextChunkBulk(
  SearchFullScanGlobalState& g, duckdb::DataChunk& output) {
  if (bulk_doc_in_seg >= bulk_seg_doc_count) {
    // Segment drained. Claim next live segment unless we've segments_exhausted.
    return segments_exhausted ? EmitOutput::Finished : EmitOutput::NeedsSegment;
  }
  auto* mat = GetOrOpenSegmentMaterializer(g, *g.reader, bulk_seg_idx);
  SDB_ENSURE(mat, sdb::ERROR_INTERNAL,
             "bulk cs scan: segment has no columnstore reader");
  const auto take = static_cast<duckdb::idx_t>(std::min<uint64_t>(
    STANDARD_VECTOR_SIZE, bulk_seg_doc_count - bulk_doc_in_seg));
  mat->Scan(bulk_doc_in_seg, take, output);
  WriteVirtualColumns(g, take, output, std::span<const float>{});
  output.SetCardinality(take);
  g.produced_rows.fetch_add(take, std::memory_order_relaxed);
  bulk_doc_in_seg += take;
  return EmitOutput::Chunk;
}

}  // namespace sdb::connector
