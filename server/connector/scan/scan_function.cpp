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

#include "connector/scan/scan_function.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <cmath>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/filters/nested_filter.hpp>
#include <iresearch/search/queries/hnsw_query.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/scan/scan_state.h"
#include "query/config.h"

namespace sdb::connector {
namespace {

bool HasNested(const irs::Filter& filter) {
  if (filter.type() == irs::Type<irs::ByNestedFilter>::id()) {
    return true;
  }
  bool nested = false;
  const_cast<irs::Filter&>(filter).VisitChildren(
    [&](irs::Filter::ptr& child, bool) {
      if (!nested && child && HasNested(*child)) {
        nested = true;
      }
    });
  return nested;
}

void ClassifySegments(ScanGlobalState& g) {
  g.segment_order.clear();
  g.segment_order.reserve(g.total_segments);
  if (g.col_filters.empty()) {
    for (uint32_t si = 0; si < g.total_segments; ++si) {
      g.segment_order.push_back(si);
    }
    return;
  }
  irs::ColFilterStateCache init_states;
  irs::ColFilterClassification cls;
  for (uint32_t si = 0; si < g.total_segments; ++si) {
    ClassifySegmentColFilters((*g.reader)[si], g, init_states, cls);
    if (!cls.segment_dead) {
      g.segment_order.push_back(si);
    }
  }
}

// The LIMIT of a parameterized statement, from the parameters bound to this
// execution. A value the scan cannot use (NULL, negative, or wider than a
// size_t) leaves the top-k unset: the scan then streams and the sort above it
// trims, which is what the plan would do without the pushdown.
std::optional<size_t> EvaluateTopK(duckdb::ClientContext& context,
                                   const duckdb::Expression& expr) {
  duckdb::Value folded;
  if (!duckdb::ExpressionExecutor::TryEvaluateScalar(context, expr, folded) ||
      folded.IsNull()) {
    return std::nullopt;
  }
  duckdb::Value casted;
  if (!folded.DefaultTryCastAs(duckdb::LogicalType::UBIGINT, casted, nullptr) ||
      casted.IsNull()) {
    return std::nullopt;
  }
  const auto k = casted.GetValue<uint64_t>();
  if (k == 0 || k > std::numeric_limits<uint32_t>::max()) {
    return std::nullopt;
  }
  return static_cast<size_t>(k);
}

// The query vector of a parameterized statement, from the parameters bound to
// this execution. The expression is evaluated into a vector and cast in one
// vectorised step, never through one duckdb::Value per dimension: at 1024
// dimensions that materialisation was a fifth of the query.
std::vector<float> EvaluateQueryVector(duckdb::ClientContext& context,
                                       const VectorScorerOptions& vs) {
  const auto target =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, vs.dims);
  const auto bad = [&](const char* what) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("the query vector of a vector search ", what));
  };
  duckdb::Vector evaluated{vs.query_expr->GetReturnType(), 1};
  {
    duckdb::ExpressionExecutor executor{context, *vs.query_expr};
    executor.ExecuteExpression(evaluated);
  }
  duckdb::Vector casted{target, 1};
  std::string error;
  if (!duckdb::VectorOperations::TryCast(context, evaluated, casted, 1,
                                         &error)) {
    bad(absl::StrCat("is not a ", target.ToString(), ": ", error).c_str());
  }
  casted.Flatten(1);
  if (!duckdb::FlatVector::Validity(casted).RowIsValid(0)) {
    bad("is NULL");
  }
  auto& child = duckdb::ArrayVector::GetEntry(casted);
  child.Flatten(vs.dims);
  if (!duckdb::FlatVector::Validity(child).CheckAllValid(vs.dims)) {
    bad("holds NULL");
  }
  const auto* data = duckdb::FlatVector::GetData<float>(child);
  return std::vector<float>{data, data + vs.dims};
}


}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<ScanBindData>();
  auto state = duckdb::make_uniq<ScanGlobalState>();

  InitScanState(*state, &context, bind_data, input);

  const auto& ss = bind_data;
  if (!ss.offsets.requests.empty() && !ss.search.filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"));
  }
  state->scan = &ss;
  if (ss.plan_cache.cache_plan && ss.plan_cache.reacquire_snapshot) {
    state->snapshot = ss.plan_cache.reacquire_snapshot(context);
  }
  const auto& snapshot =
    state->snapshot ? *state->snapshot : *ss.search.snapshot;
  state->reader = &snapshot.reader;
  state->total_segments = snapshot.reader.size();
  state->vector_scorer = ss.score.vector ? &*ss.score.vector : nullptr;
  state->top_k = ss.score.top_k;
  if (!state->top_k && ss.score.top_k_expr) {
    state->top_k = EvaluateTopK(context, *ss.score.top_k_expr);
  }

  ClassifyColumnstoreProjections(*state, bind_data);
  state->shape = DecideShape(*state, ss);

  if (state->shape == ScanShape::TsDict) {
    const auto& out = state->output_projection_ids;
    const bool real_output = out.empty()
                               ? state->has_real_column
                               : absl::c_any_of(out, [&](duckdb::idx_t proj) {
                                   return state->projected_columns[proj] !=
                                          duckdb::DConstants::INVALID_INDEX;
                                 });
    if (real_output) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ts_dict_agg() cannot be combined with other table columns"));
    }
    if (ss.search.filter) {
      state->filter = ss.search.filter.get();
    }
    state->queries.resize(state->total_segments);
    const bool seeks_one_term =
      absl::c_any_of(ss.ts_dict.requests, [](const TsDictRequest& req) {
        return req.having_filter == nullptr &&
               req.term_uses != TsDictTermUses::None &&
               (req.term_uses & TsDictTermUses::Full) == TsDictTermUses::None;
      });
    state->splittable =
      !seeks_one_term &&
      (ss.search.filter != nullptr || !state->col_filters.empty() ||
       absl::c_any_of(*state->reader, [](const auto& seg) {
         return seg.live_docs_count() != seg.docs_count();
       }));
    ClassifySegments(*state);
    BuildClaimPlan(*state, context);
    if (state->splittable) {
      BuildTsDictCounts(*state);
    }
    return state;
  }

  if (state->shape == ScanShape::CountFast) {
    state->workers = 1;
    return state;
  }

  if (state->needs_lookup && ss.relation.IsInvertedIndex()) {
    const auto pk_kind = ss.relation.ScannedIndex().GetOptions().pk_column;
    if (pk_kind == catalog::PkColumnKind::None) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("inverted index \"", ss.relation.indexes.front()->GetName(),
                "\" was created WITH (store_pk = 'none'), so it does not store "
                "row PKs and hits cannot be mapped back to source rows; select "
                "only INCLUDE'd columns, counts or scores through this index"));
    }
    if (pk_kind == catalog::PkColumnKind::Unable) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("materialising real columns from this view-backed inverted "
                "index is not yet supported -- view body must be a simple "
                "`SELECT * FROM <reader>(literal_args)` over a recognised "
                "fast-path source (read_parquet/csv/json/...)"));
    }
  }
  // The claimed WHERE, rebuilt with this execution's parameter values where
  // the plan deferred it; otherwise the one the plan built.
  std::shared_ptr<const irs::Filter> where = ss.search.filter;
  if (ss.plan_cache.deferred) {
    state->owned_where = BuildDeferredFilter(context, ss);
    where = state->owned_where;
  }
  if (ss.score.vector) {
    state->owned_vector_scorer = *ss.score.vector;
    auto& vs = *state->owned_vector_scorer;
    // The knobs are the executing session's, not the planning session's: a
    // cached plan sees every SET made since it was prepared.
    RefreshVectorKnobs(vs, context);
    // A parameterized query vector is kept as an expression at plan time and
    // read from this execution's parameters.
    if (vs.query_expr) {
      vs.query_vector = EvaluateQueryVector(context, vs);
    }
    SDB_ENSURE(!vs.query_vector.empty(),
               "a vector search has a query vector to search for");
    if (state->top_k) {
      // The beam is the result ceiling, so it is at least k -- and at least
      // the rescore pool, which is the same thing said of the pool: a search
      // that returns a hundred cannot hand four hundred to the rescorer. This
      // has to be decided here rather than next to the pool itself, because
      // the filter below captures min_ef as it is built. Qdrant takes
      // ef = max(hnsw_ef, oversampling * k) at the same point and for the same
      // reason; docs/hnsw-parity.md in vectorbench has the mapping.
      //
      // HNSW is the one kind whose search can return fewer candidates than
      // asked for. An IVF probe returns whatever the collector keeps, so the
      // pool is the candidate count outright and nothing needs widening.
      if (vs.kind == irs::AnnKind::Hnsw) {
        bool chosen_here = false;
        const auto factor = AnnOversample(context, vs, chosen_here);
        // The floor is what one segment must hold, not what the query returns.
        // Asking each of eight segments for a thousand is eight thousand
        // candidates produced to answer with one thousand. This is a floor and
        // never a cap: a wider `ef_search` still wins, and a single-segment
        // index still gets the whole k.
        const auto k = static_cast<double>(*state->top_k);
        const double share = SegmentBeamShare(k, state->total_segments);
        const double seg_pool =
          factor > 0.0 ? std::max(share, std::ceil(factor * share)) : 0.0;
        vs.min_ef = static_cast<uint32_t>(seg_pool > 0.0 ? seg_pool : share);
      }
    }
    state->vector_scorer = &vs;
    state->owned_filter = MakeVectorFilter(vs, where, vs.EffectiveRadius());
    state->filter = state->owned_filter.get();
  } else {
    state->filter = where ? where.get() : &MatchAllFilter();
  }
  state->queries.resize(state->total_segments);

  ClassifySegments(*state);

  state->splittable =
    !(ss.search.filter && HasNested(*ss.search.filter)) &&
    // A vector scan stays one unit. Letting it into the row-group split was
    // measured on wiki-1m, eight segments, one client: p99 got worse on 18 of
    // 25 groups and better on 1, and turning only the split back off returned
    // it to parity (2 better, 4 worse -- noise). These queries are 1.5 to 8 ms,
    // and claiming units, the merge barrier and a fresh set per range cost
    // more than the cores can win back. The exclusion is not an oversight.
    !ss.score.vector;

  if (state->shape == ScanShape::Count) {
    BuildClaimPlan(*state, context);
    return state;
  }

  state->needs_terms = !ss.offsets.requests.empty();

  if (state->shape == ScanShape::TopK || state->shape == ScanShape::Stream) {
    if (ss.score.text) {
      state->scorer_obj = catalog::MakeScorer(*ss.score.text);
    } else if (ss.score.order) {
      state->scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
    }
    state->stats_stage = state->scorer_obj != nullptr &&
                         state->total_segments != 0 && !ss.score.vector;
  }

  if (state->shape == ScanShape::TopK) {
    state->prune_scorer =
      ResolvePruneScorer(bind_data.score.prune, state->scorer_obj.get());
    if (state->score_static_floor >
        std::numeric_limits<irs::score_t>::lowest()) {
      state->topk.global_kth_score.store(state->score_static_floor,
                                         std::memory_order_relaxed);
    }
  } else if (state->shape == ScanShape::Stream) {
    static constinit SettingRef gDisableTopK{"sdb_disable_top_k_optimization"};
    const bool topk_disabled = gDisableTopK.Bool(context);
    const bool dynamic_bound =
      state->score_dynamic_filter &&
      (state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHAN ||
       state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    const bool static_bound =
      state->score_static_floor > std::numeric_limits<irs::score_t>::lowest();
    if (!topk_disabled && ss.score.text && state->ScanScore() &&
        (dynamic_bound || static_bound)) {
      state->prune_scorer =
        ResolvePruneScorer(bind_data.score.prune, state->scorer_obj.get());
    }
  }

  BuildClaimPlan(*state, context);

  if (state->shape == ScanShape::ColScan) {
    BuildDeadRows(*state);
  }

  if (state->scorer_obj && (!ss.score.vector || !ss.score.text)) {
    state->collect_threads = std::max<uint32_t>(1, state->workers);
    state->stats_arena.emplace(duckdb::Allocator::Get(context));
    state->collector.emplace(*state->filter, *state->scorer_obj,
                             *state->stats_arena, state->collect_threads);
    state->stats_scorer = state->collector->GetScorer();
    if (ss.score.vector) {
      state->collector->Finish();
    }
  }
  state->stats_barrier.Reset(static_cast<uint32_t>(state->total_segments));

  if (state->shape == ScanShape::TopK) {
    InitTopKGlobal(*state, context);
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& g = state->Cast<ScanGlobalState>();
  const auto worker = g.worker_count.fetch_add(1, std::memory_order_relaxed);
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> result;
  switch (g.shape) {
    case ScanShape::TsDict: {
      result = MakeTsDictLocal(g, input);
      break;
    }
    case ScanShape::CountFast: {
      auto l = duckdb::make_uniq<CountLocalState>();
      l->local_count = g.reader->live_docs_count();
      l->units_exhausted = true;
      result = std::move(l);
      break;
    }
    case ScanShape::Count: {
      result = duckdb::make_uniq<CountLocalState>();
      break;
    }
    case ScanShape::TopK: {
      auto l = duckdb::make_uniq<TopKLocalState>();
      l->worker = worker;
      InitTopKLocal(g, *l, input);
      result = std::move(l);
      break;
    }
    case ScanShape::ColScan: {
      result = duckdb::make_uniq<ColScanLocalState>();
      break;
    }
    case ScanShape::Stream: {
      auto l = duckdb::make_uniq<StreamLocalState>();
      BuildOffsetsEntries(*l, input, g.Bind());
      result = std::move(l);
      break;
    }
  }
  result->Cast<ScanLocalState>().worker = worker;
  return result;
}

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<ScanGlobalState>();
  const bool reorder = !g.output_projection_ids.empty();
  auto& base = data.local_state->Cast<ScanLocalState>();
  if (reorder) {
    if (base.scan_chunk.ColumnCount() == 0) {
      base.scan_chunk.Initialize(context, g.projected_types);
    }
    base.scan_chunk.Reset();
  }
  auto& out = reorder ? base.scan_chunk : output;
  switch (g.shape) {
    case ScanShape::TsDict:
      RunTsDictScan(context, g, *data.local_state, out);
      break;
    case ScanShape::CountFast:
    case ScanShape::Count:
      RunCountScan(data, g, data.local_state->Cast<CountLocalState>(), out);
      break;
    case ScanShape::TopK:
      RunTopKScan(context, data, g, data.local_state->Cast<TopKLocalState>(),
                  out);
      break;
    case ScanShape::ColScan:
      RunColScan(context, data, g, data.local_state->Cast<ColScanLocalState>(),
                 out);
      break;
    case ScanShape::Stream:
      RunStreamScan(context, data, g,
                    data.local_state->Cast<StreamLocalState>(), out);
      break;
  }
  if (reorder) {
    output.ReferenceColumns(out, g.output_projection_ids);
  }
}

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  auto& g = input.global_state->Cast<ScanGlobalState>();
  input.operator_metrics.rows_scanned =
    g.produced_rows.load(std::memory_order_relaxed);
  input.operator_metrics.row_groups_scanned =
    g.metrics.rg_units.load(std::memory_order_relaxed);
}

double IResearchScanProgress(duckdb::ClientContext&,
                             const duckdb::FunctionData*,
                             const duckdb::GlobalTableFunctionState* gstate_p) {
  const auto& g = gstate_p->Cast<ScanGlobalState>();
  if (g.live_segments == 0) {
    return -1;
  }
  const auto done = std::min<uint64_t>(
    g.done_segments.load(std::memory_order_relaxed), g.live_segments);
  return 100.0 * static_cast<double>(done) /
         static_cast<double>(g.live_segments);
}

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  if (!bind_data || !options || !options->single_order_key ||
      (!options->row_limit.IsValid() && !options->row_limit_expression)) {
    return;
  }
  static constinit SettingRef gDisableTopK{"sdb_disable_top_k_optimization"};
  if (gDisableTopK.Bool(context)) {
    return;
  }
  auto& bd = bind_data->Cast<ScanBindData>();
  const auto order_col = options->column_idx.GetPrimaryIndex();
  if (order_col >= bd.columns.ids.size()) {
    return;
  }
  const auto col_id = bd.columns.ids[order_col];
  if (col_id != catalog::kInvertedIndexScoreId) {
    const auto* info = bd.relation.IsIndexRelation()
                         ? bd.relation.ScannedIndex().FindColumnInfo(col_id)
                         : nullptr;
    const bool stored =
      bd.relation.IsSearchTable() || (info != nullptr && info->IsStored());
    if (stored && !bd.scan_order) {
      bd.scan_order =
        ScanOrderSpec{col_id, options->order_type, options->null_order,
                      options->order_by, options->column_type};
    }
    return;
  }
  if (bd.score.top_k || bd.score.top_k_expr) {
    return;
  }
  // A constant LIMIT is the top-k the plan carries; a parameterized one is
  // kept as the expression the scan evaluates at execution.
  const auto take_limit = [&] {
    if (options->row_limit.IsValid()) {
      bd.score.top_k = options->row_limit.GetIndex();
    } else {
      bd.score.top_k_expr = options->row_limit_expression;
    }
  };
  if (bd.score.text) {
    if (options->order_type != duckdb::OrderType::DESCENDING) {
      return;
    }
    take_limit();
    return;
  }
  if (bd.score.vector) {
    if (options->order_type != bd.score.vector->natural_order) {
      return;
    }
    take_limit();
  }
}

bool IResearchConsumeTopN(duckdb::ClientContext&,
                          duckdb::FunctionData& bind_data, duckdb::idx_t limit,
                          duckdb::idx_t offset) {
  auto& bd = bind_data.Cast<ScanBindData>();
  // A parameterized LIMIT is not a number here, so the offset cannot be
  // checked against it; the scan applies both at execution.
  if (bd.score.top_k_expr) {
    if (offset != 0) {
      return false;
    }
  } else if (!bd.score.top_k || *bd.score.top_k != limit + offset) {
    return false;
  }
  bd.score.top_offset = offset;
  bd.score.top_n_consumed = true;
  return true;
}

namespace {

duckdb::BindInfo ScanGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& bind = const_cast<ScanBindData&>(bind_data->Cast<ScanBindData>());
  if (bind.relation.table_entry) {
    return duckdb::BindInfo(*bind.relation.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

duckdb::unique_ptr<duckdb::NodeStatistics> ScanCardinality(
  duckdb::ClientContext& context, const duckdb::FunctionData* bind_data) {
  if (!bind_data) {
    return nullptr;
  }
  return bind_data->Cast<ScanBindData>().Cardinality(context);
}

duckdb::virtual_column_map_t ScanGetVirtualColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData> bind_p) {
  duckdb::virtual_column_map_t result;
  if (!bind_p) {
    return result;
  }
  const auto& bind = bind_p->Cast<ScanBindData>();
  if (bind.relation.table_entry) {
    result = bind.relation.table_entry->GetVirtualColumns();
  }
  return result;
}

duckdb::vector<duckdb::column_t> ScanGetRowIdColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData> bind_p) {
  duckdb::vector<duckdb::column_t> result;
  if (!bind_p) {
    return result;
  }
  const auto& bind = bind_p->Cast<ScanBindData>();
  if (bind.relation.table_entry) {
    result = bind.relation.table_entry->GetRowIdColumns();
  }
  return result;
}

void ScanSerialize(duckdb::Serializer&,
                   const duckdb::optional_ptr<duckdb::FunctionData>,
                   const duckdb::TableFunction&) {
  throw duckdb::NotImplementedException(
    "iresearch_scan serialization not implemented");
}

duckdb::unique_ptr<duckdb::FunctionData> ScanDeserialize(
  duckdb::Deserializer&, duckdb::TableFunction&) {
  throw duckdb::NotImplementedException(
    "iresearch_scan deserialization not implemented");
}

}  // namespace

duckdb::TableFunction CreateIResearchScanFunction() {
  duckdb::TableFunction func{
    "iresearch_scan",        {}, IResearchScanFunction, ScanBind,
    IResearchScanInitGlobal,
  };
  func.init_local = IResearchScanInitLocal;
  func.cardinality = ScanCardinality;
  func.get_metrics = IResearchScanGetMetrics;
  func.to_string_value = ScanToStringValue;
  func.table_scan_progress = IResearchScanProgress;
  func.get_bind_info = ScanGetBindInfo;
  func.get_virtual_columns = ScanGetVirtualColumns;
  func.get_row_id_columns = ScanGetRowIdColumns;
  func.pushdown_complex_filter = &optimizer::IResearchPushdownComplexFilter;
  func.pushdown_expression = &IResearchPushdownExpression;
  func.set_scan_order = &IResearchSetScanOrder;
  func.consume_top_n = &IResearchConsumeTopN;
  func.supports_pushdown_extract = &IResearchSupportsPushdownExtract;
  func.supports_pushdown_filter = &IResearchSupportsPushdownFilter;
  func.statistics_extended = &IResearchScanStatistics;
  func.serialize = ScanSerialize;
  func.deserialize = ScanDeserialize;
  func.verify_serialization = false;
  func.projection_pushdown = true;
  func.filter_pushdown = true;
  func.filter_prune = true;
  func.order_preservation_type = duckdb::OrderPreservationType::NO_ORDER;
  return func;
}

void RegisterIResearchScanFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(CreateIResearchScanFunction());
}

}  // namespace sdb::connector
