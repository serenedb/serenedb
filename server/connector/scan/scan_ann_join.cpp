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

#include "connector/scan/scan_ann_join.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <core_functions/array_kernels.hpp>
#include <deque>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <functional>
#include <iresearch/formats/ivf/ivf_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/ann/batch.hpp>
#include <iresearch/search/ann/family.hpp>
#include <iresearch/search/detail/ann_index.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <limits>
#include <memory>
#include <vector>

#include "connector/scan/scan_function.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

struct AnnJoinGlobalState final : duckdb::GlobalTableFunctionState {
  ScanGlobalState scan;
  VectorScorerOptions vs;
  irs::Filter::ptr filter;
  const irs::ByVectorSimilarityOptions* options = nullptr;
  uint32_t k = 0;
  uint32_t batch_rows = 0;
  bool rescore = false;
  bool left_join = false;
  duckdb::vector<duckdb::ColumnIndex> columns;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> outputs;

  duckdb::idx_t MaxThreads() const final { return scan.workers; }
};

constexpr uint32_t kNullRow = std::numeric_limits<uint32_t>::max();

struct Emit {
  uint32_t row;
  uint32_t fetched;
  irs::score_t score;
};

struct Candidate {
  uint32_t segment;
  irs::doc_id_t doc;
  uint32_t query;
  uint32_t slot;
};

constexpr uint32_t kRescoreRound = 8;

struct AnnJoinLocalState final : RankedLocalState {
  duckdb::idx_t next_row = 0;
  std::vector<float> vectors;
  std::vector<uint32_t> rows;
  std::deque<irs::ann::SharedFloor> floors;
  std::vector<irs::ann::Collector> collectors;
  std::vector<std::vector<irs::ScoreDoc>> pools;
  std::vector<Candidate> candidates;
  std::vector<uint32_t> cursors;
  std::vector<bool> stopped;
  std::vector<std::vector<irs::score_t>> best;
  std::vector<std::unique_ptr<irs::detail::RawVectorReader>> rescorers;
  std::vector<irs::ScoreDoc> unique;
  std::vector<irs::ScoreDoc> row_hits;
  irs::containers::FlatHashMap<uint64_t, uint32_t> answer_row;
  std::vector<Emit> answer;
  size_t emitted = 0;
  std::vector<bool> answered;
  irs::ColumnArgsFetcher fetcher;
  duckdb::DataChunk batch;
  duckdb::DataChunk rows_chunk;
  duckdb::Vector scores{duckdb::LogicalType::FLOAT};
  duckdb::ExpressionExecutor executor;

  explicit AnnJoinLocalState(duckdb::ClientContext& context)
    : executor{context} {}
};

void Renumber(duckdb::Expression& expr, const AnnJoinBindData& join,
              std::vector<duckdb::idx_t>& kept,
              duckdb::vector<duckdb::ColumnIndex>& columns) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
    auto& index = expr.Cast<duckdb::BoundReferenceExpression>().IndexMutable();
    if (kept[index] == duckdb::DConstants::INVALID_INDEX) {
      kept[index] = columns.size();
      columns.push_back(join.columns[index]);
    }
    index = kept[index];
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr,
    [&](duckdb::Expression& child) { Renumber(child, join, kept, columns); });
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> AnnJoinInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& join = input.bind_data->Cast<AnnJoinBindData>();
  const auto& bind = *join.scan;
  auto state = duckdb::make_uniq<AnnJoinGlobalState>();
  std::vector<duckdb::idx_t> kept(join.columns.size(),
                                  duckdb::DConstants::INVALID_INDEX);
  kept.back() = 0;
  state->columns.push_back(join.columns.back());
  const auto count = input.projection_ids.empty() ? input.column_ids.size()
                                                  : input.projection_ids.size();
  for (duckdb::idx_t c = 0; c < count; ++c) {
    const auto id =
      input
        .column_ids[input.projection_ids.empty() ? c : input.projection_ids[c]];
    if (id >= join.outputs.size()) {
      state->outputs.emplace_back();
      continue;
    }
    auto output = join.outputs[id]->Copy();
    Renumber(*output, join, kept, state->columns);
    state->outputs.push_back(std::move(output));
  }
  auto& g = state->scan;
  duckdb::TableFunctionInitInput scan_input{&bind, state->columns, {}, nullptr};
  InitScanState(g, &context, bind, scan_input);
  g.scan = &bind;
  g.snapshot = bind.Snapshot(context);
  g.reader = &g.snapshot->reader;
  g.total_segments = g.reader->size();
  g.workers = duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads();
  g.rg_size = bind.RowGroupSize();
  ClassifyColumnstoreProjections(g, bind);
  EnsureLookupable(g, bind);

  SDB_ASSERT(bind.score.vector && bind.score.top_k);
  auto& vs = state->vs;
  vs = *bind.score.vector;
  const bool exact = vs.exact;
  RefreshVectorKnobs(vs, context);
  vs.exact = vs.exact || exact;
  const auto live = g.reader->live_docs_count();
  state->k = static_cast<uint32_t>(
    std::min<uint64_t>(*bind.score.top_k, std::max<uint64_t>(live, 1)));
  vs.top_k = state->k;
  const auto k = static_cast<double>(state->k);
  if (vs.kind == irs::AnnKind::Hnsw) {
    vs.min_ef = ClampCandidates(SegmentBeamShare(k, g.total_segments), k, live);
    if (vs.ef_search == 0) {
      vs.ef_search = std::max(vs.ef_construction, state->k);
    }
  }
  state->rescore = !vs.exact && vs.rescore_width > 0.f;
  g.vector_scorer = &vs;
  if (!vs.exact) {
    state->filter = MakeVectorFilter(vs, bind.search.filter,
                                     std::numeric_limits<float>::max());
    state->options =
      &static_cast<const irs::ByVectorSimilarity&>(*state->filter).options();
  }
  state->batch_rows = irs::ann::BatchRows(vs.kind, vs.dims);
  state->left_join = join.left_join;
  return state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> AnnJoinInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState* global_state) {
  const auto& g = global_state->Cast<AnnJoinGlobalState>();
  auto state = duckdb::make_uniq<AnnJoinLocalState>(context.client);
  state->batch.Initialize(context.client, g.scan.projected_types);
  state->rows_chunk.InitializeEmpty(g.scan.projected_types);
  for (const auto& output : g.outputs) {
    if (output) {
      state->executor.AddExpression(*output);
    }
  }
  return state;
}

void PrepareQueries(const AnnJoinGlobalState& g, AnnJoinLocalState& l,
                    duckdb::DataChunk& input, duckdb::idx_t end) {
  const auto d = g.vs.dims;
  auto& arrays = input.data[0];
  const auto& valid = duckdb::FlatVector::Validity(arrays);
  auto& child = duckdb::ArrayVector::GetEntry(arrays);
  const auto& child_valid = duckdb::FlatVector::Validity(child);
  const auto* data = duckdb::FlatVector::GetData<float>(child);
  l.vectors.clear();
  l.rows.clear();
  for (; l.next_row < end; ++l.next_row) {
    const auto row = l.next_row;
    if (!valid.RowIsValid(row)) {
      continue;
    }
    const auto first = row * d;
    bool complete = true;
    for (uint32_t j = 0; j < d && complete; ++j) {
      complete = child_valid.RowIsValid(first + j);
    }
    if (!complete) {
      continue;
    }
    l.vectors.insert(l.vectors.end(), data + first, data + first + d);
    if (g.vs.metric == irs::VectorMetric::Cosine) {
      auto* query = l.vectors.data() + l.vectors.size() - d;
      duckdb::L2NormalizeOp::Operation(query, query, d);
    }
    l.rows.push_back(static_cast<uint32_t>(row));
  }
}

void ResetCollectors(const AnnJoinGlobalState& g, AnnJoinLocalState& l,
                     uint32_t count) {
  while (l.floors.size() < count) {
    l.floors.emplace_back(g.k);
  }
  l.collectors.clear();
  for (uint32_t i = 0; i < count; ++i) {
    l.floors[i].Reset(g.scan.score_static_floor);
    l.collectors.emplace_back(l.floors[i], g.k);
  }
  l.pools.resize(count);
  for (auto& pool : l.pools) {
    pool.clear();
  }
}

void Drain(AnnJoinLocalState& l, uint32_t segment) {
  for (size_t i = 0; i < l.collectors.size(); ++i) {
    auto& collector = l.collectors[i];
    for (auto hit : collector.Flush()) {
      hit.segment_idx = segment;
      l.pools[i].push_back(hit);
    }
    collector.Clear();
  }
}

void SearchApprox(const AnnJoinGlobalState& g, AnnJoinLocalState& l) {
  const auto& reader = *g.scan.reader;
  const irs::ann::QueryBatch batch{.vectors = l.vectors, .d = g.vs.dims};
  for (uint32_t seg = 0; seg < reader.size(); ++seg) {
    const auto& segment = reader[seg];
    if (segment.live_docs_count() == 0) {
      continue;
    }
    const auto* ann = segment.Ann(g.options->ann_id);
    if (!ann) {
      continue;
    }
    ann->SearchBatch(segment, {}, *g.options, batch, g.options->nprobe,
                     l.collectors);
    Drain(l, seg);
  }
}

void SearchExactly(const AnnJoinGlobalState& g, AnnJoinLocalState& l,
                   uint32_t query) {
  const auto& reader = *g.scan.reader;
  auto vs = g.vs;
  vs.exact = true;
  const auto d = vs.dims;
  vs.query_vector.assign(l.vectors.begin() + size_t{query} * d,
                         l.vectors.begin() + size_t{query + 1} * d);
  const auto filter = MakeVectorFilter(vs, g.scan.scan->search.filter,
                                       std::numeric_limits<float>::max());
  const irs::VectorSimilarityScorer scorer;
  auto& collector = l.collectors[query];
  for (uint32_t seg = 0; seg < reader.size(); ++seg) {
    const auto& segment = reader[seg];
    if (segment.live_docs_count() == 0) {
      continue;
    }
    const auto prepared = filter->PrepareSegment(segment, {});
    l.fetcher.Clear();
    auto root = irs::ann::MakeRoot(
      *prepared, {.scorer = scorer, .fetcher = l.fetcher, .k = collector.K()});
    EnsurePlanned(static_cast<bool>(root));
    root->Run(collector);
    for (auto hit : collector.Flush()) {
      hit.segment_idx = seg;
      l.pools[query].push_back(hit);
    }
    collector.Clear();
  }
}

irs::detail::RawVectorReader* Rescorer(const AnnJoinGlobalState& g,
                                       AnnJoinLocalState& l, uint32_t seg) {
  if (l.rescorers.size() <= seg) {
    l.rescorers.resize(g.scan.reader->size());
  }
  auto& reader = l.rescorers[seg];
  if (!reader) {
    const auto& sub = (*g.scan.reader)[seg];
    const auto* column = sub.Column(g.vs.field_id);
    const auto* columns = sub.GetColReader();
    if (!column || !columns) {
      return nullptr;
    }
    reader = std::make_unique<irs::detail::RawVectorReader>(*column, *columns,
                                                            g.vs.dims);
  }
  return reader.get();
}

irs::score_t RescoreBar(const AnnJoinGlobalState& g, const AnnJoinLocalState& l,
                        uint32_t q) {
  const auto& best = l.best[q];
  const auto exact = best.size() == g.k
                       ? best.front()
                       : std::numeric_limits<irs::score_t>::lowest();
  return std::max(exact, l.floors[q].Load());
}

void Rescore(const AnnJoinGlobalState& g, AnnJoinLocalState& l) {
  const auto count = static_cast<uint32_t>(l.pools.size());
  l.cursors.assign(count, 0);
  l.stopped.assign(count, false);
  l.best.resize(count);
  for (uint32_t q = 0; q < count; ++q) {
    l.best[q].clear();
    absl::c_sort(l.pools[q], BetterScore{});
  }
  const auto dist = irs::ResolveScoringDistance(g.vs.metric);
  const auto d = static_cast<uint16_t>(g.vs.dims);
  for (uint32_t take = g.k;; take = kRescoreRound) {
    l.candidates.clear();
    for (uint32_t q = 0; q < count; ++q) {
      const auto& pool = l.pools[q];
      auto& cursor = l.cursors[q];
      const auto bar = RescoreBar(g, l, q);
      for (uint32_t t = 0; t < take && !l.stopped[q] && cursor < pool.size();
           ++t) {
        if (pool[cursor].score < bar) {
          l.stopped[q] = true;
          break;
        }
        l.candidates.push_back({.segment = pool[cursor].segment_idx,
                                .doc = pool[cursor].doc,
                                .query = q,
                                .slot = cursor});
        ++cursor;
      }
    }
    if (l.candidates.empty()) {
      break;
    }
    absl::c_sort(l.candidates, [](const Candidate& a, const Candidate& b) {
      return std::tie(a.segment, a.doc) < std::tie(b.segment, b.doc);
    });
    for (size_t i = 0; i < l.candidates.size();) {
      const auto& first = l.candidates[i];
      auto* reader = Rescorer(g, l, first.segment);
      const auto* row = reader ? reader->Row(first.doc) : nullptr;
      for (; i < l.candidates.size() &&
             l.candidates[i].segment == first.segment &&
             l.candidates[i].doc == first.doc;
           ++i) {
        const auto& c = l.candidates[i];
        auto& score = l.pools[c.query][c.slot].score;
        if (!row) {
          score = std::numeric_limits<irs::score_t>::lowest();
          continue;
        }
        score = dist(reinterpret_cast<const irs::byte_type*>(
                       l.vectors.data() + size_t{c.query} * d),
                     reinterpret_cast<const irs::byte_type*>(row), d);
        auto& best = l.best[c.query];
        best.push_back(score);
        absl::c_push_heap(best, std::greater<>{});
        if (best.size() > g.k) {
          absl::c_pop_heap(best, std::greater<>{});
          best.pop_back();
        }
      }
    }
  }
  for (uint32_t q = 0; q < count; ++q) {
    auto& pool = l.pools[q];
    for (size_t slot = l.cursors[q]; slot < pool.size(); ++slot) {
      pool[slot].score = std::numeric_limits<irs::score_t>::lowest();
    }
  }
}

void KeepTop(std::vector<irs::ScoreDoc>& pool, uint32_t k, irs::score_t floor) {
  std::erase_if(pool,
                [&](const irs::ScoreDoc& hit) { return !(hit.score > floor); });
  if (pool.size() > k) {
    std::nth_element(pool.begin(), pool.begin() + k, pool.end(), BetterScore{});
    pool.resize(k);
  }
  absl::c_sort(pool, BetterScore{});
}

uint64_t AddressOf(const irs::ScoreDoc& hit) noexcept {
  return (uint64_t{hit.segment_idx} << 32) | hit.doc;
}

void FetchAnswer(duckdb::ClientContext& ctx, AnnJoinGlobalState& g,
                 AnnJoinLocalState& l) {
  l.unique.clear();
  for (const auto& pool : l.pools) {
    l.unique.insert(l.unique.end(), pool.begin(), pool.end());
  }
  SortByAddress(l.unique);
  l.unique.erase(
    std::unique(l.unique.begin(), l.unique.end(),
                [](const irs::ScoreDoc& a, const irs::ScoreDoc& b) {
                  return AddressOf(a) == AddressOf(b);
                }),
    l.unique.end());
  l.answer_chunk = duckdb::make_uniq<duckdb::DataChunk>();
  l.answer_chunk->Initialize(ctx, g.scan.projected_types,
                             std::max<size_t>(1, l.unique.size()));
  l.row_hits.clear();
  if (g.scan.needs_lookup) {
    EnsureIndexSource(ctx, g.scan, l);
  }
  const auto units = FetchUnitsOf(l.unique, g.scan.rg_size);
  for (size_t u = 0; u < units.size(); ++u) {
    const auto& unit = units[u];
    const std::span<const irs::ScoreDoc> hits{l.unique.data() + unit.first,
                                              unit.count};
    auto fetched = FetchHits(ctx, g.scan, l, unit.seg, hits,
                             u == 0 || units[u - 1].seg != unit.seg);
    if (g.scan.needs_lookup) {
      MaterializeHits(ctx, g.scan, l, *fetched.chunk, *fetched.pk, hits,
                      l.batch, l.row_hits);
    } else {
      l.answer_chunk->Append(*fetched.chunk,
                             duckdb::VectorAppendMode::ALLOW_RESIZE);
      l.row_hits.insert(l.row_hits.end(), hits.begin(), hits.end());
    }
  }
  l.answer_row.clear();
  for (uint32_t r = 0; r < l.row_hits.size(); ++r) {
    l.answer_row.emplace(AddressOf(l.row_hits[r]), r);
  }
  l.answer.clear();
  l.emitted = 0;
  for (size_t q = 0; q < l.pools.size(); ++q) {
    for (const auto& hit : l.pools[q]) {
      if (const auto it = l.answer_row.find(AddressOf(hit));
          it != l.answer_row.end()) {
        l.answer.push_back(
          {.row = l.rows[q], .fetched = it->second, .score = hit.score});
      }
    }
  }
}

void AppendNullRows(AnnJoinLocalState& l, duckdb::idx_t first,
                    duckdb::idx_t end) {
  l.answered.assign(end - first, false);
  for (const auto& emit : l.answer) {
    l.answered[emit.row - first] = true;
  }
  for (auto row = first; row < end; ++row) {
    if (!l.answered[row - first]) {
      l.answer.push_back(
        {.row = static_cast<uint32_t>(row), .fetched = kNullRow, .score = 0});
    }
  }
}

void SearchRows(duckdb::ClientContext& ctx, AnnJoinGlobalState& g,
                AnnJoinLocalState& l, duckdb::DataChunk& input) {
  const auto first = l.next_row;
  const auto end =
    std::min<duckdb::idx_t>(input.size(), l.next_row + g.batch_rows);
  PrepareQueries(g, l, input, end);
  const auto count = static_cast<uint32_t>(l.rows.size());
  ResetCollectors(g, l, count);
  if (count == 0) {
    l.answer.clear();
    l.emitted = 0;
    if (g.left_join) {
      AppendNullRows(l, first, end);
    }
    return;
  }
  if (g.vs.exact) {
    for (uint32_t q = 0; q < count; ++q) {
      SearchExactly(g, l, q);
    }
  } else {
    SearchApprox(g, l);
    if (g.rescore) {
      Rescore(g, l);
    }
  }
  const auto want = std::min<uint64_t>(g.k, g.scan.reader->live_docs_count());
  for (uint32_t q = 0; q < count; ++q) {
    KeepTop(l.pools[q], g.k, g.scan.score_static_floor);
    if (!g.vs.exact && g.vs.kind == irs::AnnKind::Ivf &&
        l.pools[q].size() < want) {
      l.pools[q].clear();
      l.floors[q].Reset(g.scan.score_static_floor);
      SearchExactly(g, l, q);
      KeepTop(l.pools[q], g.k, g.scan.score_static_floor);
    }
  }
  FetchAnswer(ctx, g, l);
  if (g.left_join) {
    AppendNullRows(l, first, end);
  }
}

void EmitRows(AnnJoinGlobalState& g, AnnJoinLocalState& l,
              duckdb::TableFunctionInput& data, duckdb::DataChunk& output) {
  const bool null_rows = l.answer[l.emitted].fetched == kNullRow;
  const auto limit =
    std::min<size_t>(STANDARD_VECTOR_SIZE, l.answer.size() - l.emitted);
  duckdb::idx_t n = 0;
  for (;
       n < limit && (l.answer[l.emitted + n].fetched == kNullRow) == null_rows;
       ++n) {
    data.pk_survivors[n] = l.answer[l.emitted + n].row;
  }
  if (!null_rows) {
    duckdb::SelectionVector sel(n);
    auto* scores = duckdb::FlatVector::GetDataMutable<float>(l.scores);
    const auto score_emit = ScoreEmitOf(g.scan);
    for (duckdb::idx_t w = 0; w < n; ++w) {
      const auto& emit = l.answer[l.emitted + w];
      sel.set_index(w, emit.fetched);
      scores[w] = ApplyScoreEmit(score_emit, emit.score);
    }
    for (duckdb::idx_t c = 0; c < l.rows_chunk.ColumnCount(); ++c) {
      if (c == g.scan.score_output_idx) {
        l.rows_chunk.data[c].Reference(l.scores);
      } else {
        l.rows_chunk.data[c].Slice(l.answer_chunk->data[c], sel, n);
      }
    }
    l.rows_chunk.SetCardinality(n);
    l.executor.SetChunk(&l.rows_chunk);
  }
  duckdb::idx_t expr = 0;
  for (duckdb::idx_t c = 0; c < g.outputs.size(); ++c) {
    auto& column = output.data[c];
    const bool evaluated = !null_rows && g.outputs[c];
    if (evaluated) {
      l.executor.ExecuteExpression(expr, column);
    } else {
      column.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
      duckdb::ConstantVector::SetNull(column, true);
    }
    if (g.outputs[c]) {
      ++expr;
    }
  }
  l.emitted += n;
  output.SetChildCardinality(n);
}

duckdb::OperatorResultType AnnJoinFunction(duckdb::ExecutionContext& context,
                                           duckdb::TableFunctionInput& data,
                                           duckdb::DataChunk& input,
                                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<AnnJoinGlobalState>();
  auto& l = data.local_state->Cast<AnnJoinLocalState>();
  if (l.next_row == 0 && l.emitted == l.answer.size()) {
    input.data[0].Flatten(input.size());
    duckdb::ArrayVector::GetEntry(input.data[0])
      .Flatten(input.size() * g.vs.dims);
  }
  while (l.emitted == l.answer.size()) {
    if (l.next_row >= input.size()) {
      l.next_row = 0;
      output.SetChildCardinality(0);
      return duckdb::OperatorResultType::NEED_MORE_INPUT;
    }
    SearchRows(context.client, g, l, input);
  }
  EmitRows(g, l, data, output);
  if (l.emitted == l.answer.size() && l.next_row >= input.size()) {
    l.next_row = 0;
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> AnnJoinToStringValue(
  duckdb::TableFunctionToStringInput& input) {
  if (!input.bind_data) {
    return {};
  }
  const auto& join = input.bind_data->Cast<AnnJoinBindData>();
  duckdb::TableFunctionToStringInput scan{input.table_function,
                                          join.scan.get()};
  scan.projected_column_ids = &join.columns;
  return ScanToStringValue(scan);
}

}  // namespace

duckdb::unique_ptr<duckdb::FunctionData> AnnJoinBindData::Copy() const {
  auto copy = duckdb::make_uniq<AnnJoinBindData>();
  copy->scan =
    duckdb::unique_ptr_cast<duckdb::FunctionData, ScanBindData>(scan->Copy());
  copy->columns = columns;
  for (const auto& output : outputs) {
    copy->outputs.push_back(output->Copy());
  }
  copy->left_join = left_join;
  return copy;
}

bool AnnJoinBindData::Equals(const duckdb::FunctionData& other) const {
  const auto& join = other.Cast<AnnJoinBindData>();
  return scan->Equals(*join.scan) && columns == join.columns &&
         duckdb::Expression::ListEquals(outputs, join.outputs) &&
         left_join == join.left_join;
}

duckdb::TableFunction CreateAnnJoinFunction() {
  duckdb::TableFunction func;
  func.name = "iresearch_ann_join";
  func.init_global = AnnJoinInitGlobal;
  func.init_local = AnnJoinInitLocal;
  func.in_out_function = AnnJoinFunction;
  func.to_string_value = AnnJoinToStringValue;
  func.projection_pushdown = true;
  func.batch_projected_input = true;
  func.verify_serialization = false;
  return func;
}

}  // namespace sdb::connector
