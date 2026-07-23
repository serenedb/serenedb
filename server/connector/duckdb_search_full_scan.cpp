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
#include <array>
#include <cmath>
#include <duckdb/common/projection_index.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar/generic_common.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/conjunction.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter_visitor.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/vector_similarity_query.hpp>
#include <iresearch/search/vector_similarity_scorer.hpp>
#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <mutex>
#include <ranges>
#include <span>
#include <type_traits>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "connector/index_source_factory.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_pk_lookup.h"
#include "iresearch/index/hit_batcher.hpp"
#include "iresearch/index/pk_batch_helpers.hpp"
#include "iresearch/index/table_filter_iterator.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {

// Per-worker scan state, one family per ScanMode. Base holds what every mode
// shares (claim bookkeeping + per-segment filter classification);
// SegDocBufferedScanLocalState adds the HitBatcher machinery of the
// doc-emitting modes (TopK / Stream / ColScan).
struct IResearchScanLocalState : public duckdb::LocalTableFunctionState {
  // Holds the scanned column_ids when output_projection_ids is set; the output
  // then references the projected subset out of this (a reorder, not a copy).
  duckdb::DataChunk scan_chunk;

  // This worker's collector slot for the scorer prepare phase; written by
  // EnsureSegmentQuery only when a scorer runs (inert for count/ts_dict).
  irs::PrepareCollector* prepare_collector = nullptr;

  // Whole-file filter classification of the segment this worker currently
  // scans: computed exactly once per claimed segment (at the claim site) and
  // consumed by StartSegment (HitBatcher binding) and the bulk FullScanner.
  uint32_t classified_seg = std::numeric_limits<uint32_t>::max();
  TableFilterDocIterator::SegmentClassification seg_cls;
  // Per-worker filter-evaluation state (ExpressionExecutor + decode scratch),
  // built once per pushed filter and reused across segments and engines.
  ColFilterStateCache filter_states;
};

struct SegDocBufferedScanLocalState : public IResearchScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  std::unique_ptr<HitBatcher> hit_batcher;
  // The scorer prepare phase ran (TopK / scored Stream dispatch).
  bool prepared = false;

  // ts_offsets() output state (streaming + top-k paths); empty when not
  // requested.
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void EnsureHitBatcher(const IResearchScanGlobalState& g) {
    if (!hit_batcher) {
      hit_batcher = std::make_unique<HitBatcher>(
        g.cs_projections,
        g.needs_lookup ? catalog::term_dict::kPKFieldId
                       : irs::field_limits::invalid(),
        g.ScanScore());
    }
  }

  std::pair<const irs::ColReader*, const irs::ColumnReader*> PkColumnFor(
    const irs::IndexReader& reader, uint32_t seg_idx) {
    if (seg_idx != _pk_col_cached_seg) {
      std::tie(_pk_col_reader, _pk_column) = SegmentPkColumn(reader, seg_idx);
      _pk_col_cached_seg = seg_idx;
    }
    return {_pk_col_reader, _pk_column};
  }

 private:
  const irs::ColReader* _pk_col_reader = nullptr;
  const irs::ColumnReader* _pk_column = nullptr;
  uint32_t _pk_col_cached_seg = std::numeric_limits<uint32_t>::max();
};

struct TopKScanLocalState : public SegDocBufferedScanLocalState {
  bool segments_exhausted = false;
  std::vector<irs::ScoreDoc> hit_buf;
  std::span<irs::ScoreDoc> hit_slice;
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::lowest();
  irs::ColumnArgsFetcher score_fetcher;
  using Collector = irs::NthPartitionScoreCollector;
  std::variant<std::monostate, Collector> collector;
  std::span<const irs::ScoreDoc> top_hits;
  size_t emit_idx = 0;
  bool emit_prepared = false;

  void PrepareEmitBuffer(IResearchScanGlobalState& g);
};

struct StreamScanLocalState : public SegDocBufferedScanLocalState {
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
struct ColScanLocalState : public StreamScanLocalState {
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

struct CountScanLocalState : public IResearchScanLocalState {
  bool segments_exhausted = false;
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;
};

struct ColFilterCtx {
  std::span<const TableFilterDocIterator::FilterSpec> active;
  IResearchScanGlobalState* g = nullptr;
  ColFilterStateCache* states = nullptr;
};

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
  TableFilterDocIterator::SegmentClassification seg_cls;
  ColFilterStateCache filter_states;

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
  ColFilterCtx _cf;
  bool _null_pending = false;
  const FieldState* _field = nullptr;
  const FieldState* _next_field = nullptr;
  CountMode _cursor_mode = CountMode::Meta;
  irs::TermIterator::ptr _cursor;
};

void RunCountScan(IResearchScanGlobalState& g, CountScanLocalState& l,
                  duckdb::DataChunk& output);
void RunTopKScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                 TopKScanLocalState& l, duckdb::DataChunk& output);
void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                      StreamScanLocalState& l, duckdb::DataChunk& output);
void RunColScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                ColScanLocalState& l, duckdb::DataChunk& output);
void RunTsDictScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                   TsDictLocalState& l, duckdb::DataChunk& output);

struct HitsChunk {
  std::span<const irs::doc_id_t> docs;
  std::span<const float> scores;
  std::span<const uint32_t> segs;
  uint32_t fixed_seg = 0;

  size_t size() const noexcept { return docs.size(); }
  uint32_t seg(size_t i) const noexcept {
    return segs.empty() ? fixed_seg : segs[i];
  }
};

inline void SortScoreDocsBySegDoc(std::span<irs::ScoreDoc> hits) {
  std::ranges::sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             IResearchScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output);
duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx,
                            IResearchScanGlobalState& g,
                            SegDocBufferedScanLocalState& l,
                            duckdb::DataChunk& output, duckdb::idx_t collected);
bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx,
                           IResearchScanGlobalState& g, TopKScanLocalState& l,
                           duckdb::DataChunk& output);

namespace {

// Scan-computed virtual index columns and their output types; the score
// column additionally records its output slot (filters on it apply to the
// computed score vector).
std::optional<duckdb::LogicalType> VirtualIndexColumnType(
  catalog::Column::Id col_id) {
  if (col_id == catalog::Column::kInvertedIndexScoreId ||
      col_id == catalog::Column::kInvertedIndexTermScoreId) {
    return duckdb::LogicalType::FLOAT;
  }
  if (col_id == catalog::Column::kInvertedIndexOffsetsId) {
    return catalog::Column::MakeOffsetsType();
  }
  if (col_id == catalog::Column::kInvertedIndexTermId) {
    return duckdb::LogicalType::VARCHAR;
  }
  if (col_id == catalog::Column::kInvertedIndexTermRawId) {
    return duckdb::LogicalType::BLOB;
  }
  if (col_id == catalog::Column::kInvertedIndexTermCountId) {
    return duckdb::LogicalType::INTEGER;
  }
  if (col_id == catalog::Column::kInvertedIndexTermFreqId) {
    return duckdb::LogicalType::BIGINT;
  }
  return std::nullopt;
}

}  // namespace

void BuildTableFilter(IResearchScanGlobalState& state,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters);

void InitScanState(IResearchScanGlobalState& state,
                   duckdb::ClientContext& context,
                   const SereneDBScanBindData& bind_data,
                   duckdb::TableFunctionInitInput& input) {
  const auto in_output = [&](duckdb::idx_t proj) {
    return input.projection_ids.empty() ||
           absl::c_find(input.projection_ids, proj) !=
             input.projection_ids.end();
  };
  // Determine which columns DuckDB actually wants (projection pushdown).
  const auto num_bind_columns = bind_data.column_ids.size();
  for (auto col_id : input.column_ids) {
    const auto proj = state.projected_columns.size();
    if (col_id == kColumnIdentifierGeneratedPk) {
      if (!bind_data.IsSearchTableEntry()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("projecting the rowid through an inverted-index scan is not "
                  "supported"));
      }
      // Search table: the rowid IS the generated PK, materialized from `.col`.
      state.generated_pk_output_idx = proj;
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::ROW_TYPE);
    } else if (col_id == kColumnIdentifierTableOid) {
      state.tableoid_output_idx = proj;
      state.tableoid_value = bind_data.RelationId().id();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      // The empty virtual column emits no values, so it does not make the
      // output real (count(*) shapes express filter-only scans through it).
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BOOLEAN);
      continue;
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      SDB_ASSERT(!bind_data.IsViewBacked(),
                 "virtual PK columns are not used for view-backed scans");
      auto cat_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(cat_idx != duckdb::DConstants::INVALID_INDEX);
      const auto& tbd = bind_data.As<TableScanBindData>();
      const auto& catalog_cols = tbd.table->Columns();
      SDB_ASSERT(cat_idx < catalog_cols.size());
      const auto catalog_col_id = catalog_cols[cat_idx].GetId();
      duckdb::idx_t bind_idx = duckdb::DConstants::INVALID_INDEX;
      for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
        if (bind_data.column_ids[i] == catalog_col_id) {
          bind_idx = i;
          break;
        }
      }
      SDB_ASSERT(bind_idx != duckdb::DConstants::INVALID_INDEX);
      state.projected_columns.push_back(bind_idx);
      state.projected_types.push_back(bind_data.column_types[bind_idx]);
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.column_ids[col_id];
      if (const auto virtual_type = VirtualIndexColumnType(catalog_col_id)) {
        if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
          state.score_output_idx = proj;
        }
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(*virtual_type);
      } else {
        state.projected_columns.push_back(col_id);
        const auto& col_index = input.column_indexes[proj];
        if (col_index.IsPushdownExtract() && col_index.HasChildren()) {
          state.projected_types.push_back(col_index.GetScanType());
        } else {
          state.projected_types.push_back(bind_data.column_types[col_id]);
        }
      }
    } else {
      continue;
    }
    if (in_output(proj)) {
      state.has_output_column = true;
    }
  }

  state.lookup_projected_columns = state.projected_columns;
  state.has_real_column = absl::c_any_of(state.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });

  state.client_context = &context;
  state.projected_column_indexes = input.column_indexes;
  SDB_ASSERT(state.projected_column_indexes.size() ==
             state.projected_columns.size());

  if (!input.projection_ids.empty()) {
    state.output_projection_ids = input.projection_ids;
  }

  // The floor consumed from a dropped static score filter (see
  // IResearchSupportsPushdownFilter); partial folds of still-pushed score
  // filters max into it below.
  state.score_static_floor = bind_data.score_static_floor;

  state.pushed_filters = input.filters.get();
  if (input.filters && input.filters->HasFilters()) {
    BuildTableFilter(state, bind_data, *input.filters);
  }
}

// IS NOT NULL replacement for the TRUE_OR_NULL segment classification,
// mirroring duckdb's propagate_get: null when the filter is a ConstantOrNull
// whose children the replacement cannot represent.
duckdb::unique_ptr<duckdb::TableFilter> MakeNotNullReplacement(
  const duckdb::TableFilter& filter, const duckdb::LogicalType& type) {
  const auto& expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(
    filter, "MakeNotNullReplacement");
  if (expr_filter.expr->GetExpressionType() ==
      duckdb::ExpressionType::BOUND_FUNCTION) {
    auto& func = expr_filter.expr->Cast<duckdb::BoundFunctionExpression>();
    if (duckdb::ConstantOrNull::IsConstantOrNull(
          func, duckdb::Value::BOOLEAN(true))) {
      for (auto child = ++func.GetChildren().begin();
           child != func.GetChildren().end(); ++child) {
        switch (child->get()->GetExpressionType()) {
          case duckdb::ExpressionType::BOUND_REF:
          case duckdb::ExpressionType::VALUE_CONSTANT:
            continue;
          default:
            return nullptr;
        }
      }
    }
  }
  auto not_null = duckdb::ExpressionFilter::CreateNullCheckExpression(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0ULL),
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL);
  return duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(not_null));
}

// Lower bound implied by a static score filter (`score > c` / `score >= c`,
// AND-conjunctions take the max; casts around the column unwrap): the largest
// float T such that every score passing the filter exceeds T. `exact` is set
// when the whole expression IS that bound (every conjunct folded), so the
// filter can be consumed: enforcing `score > T` replaces evaluating it.
// lowest() when the expression implies no usable lower bound.
float StaticScoreFloor(const duckdb::Expression& expr, bool& exact) {
  static constexpr auto kNone = std::numeric_limits<float>::lowest();
  exact = false;
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    if (expr.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_AND) {
      return kNone;
    }
    float floor = kNone;
    bool all_exact = true;
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      bool child_exact = false;
      floor = std::max(floor, StaticScoreFloor(*child, child_exact));
      all_exact &= child_exact;
    }
    exact = all_exact;
    return floor;
  }
  if (!duckdb::BoundComparisonExpression::IsComparison(expr)) {
    return kNone;
  }
  const auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto* ref = &duckdb::BoundComparisonExpression::Left(cmp);
  const auto* cst = &duckdb::BoundComparisonExpression::Right(cmp);
  auto type = expr.GetExpressionType();
  if (ref->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    std::swap(ref, cst);
    type = duckdb::FlipComparisonExpression(type);
  }
  if (type != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
      type != duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
    return kNone;
  }
  while (ref->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    ref = &ref->Cast<duckdb::BoundCastExpression>().Child();
  }
  if (ref->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF ||
      cst->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return kNone;
  }
  const auto& val = cst->Cast<duckdb::BoundConstantExpression>().GetValue();
  if (val.IsNull() || !val.type().IsNumeric()) {
    return kNone;
  }
  const double c = val.GetValue<double>();
  const bool strict = type == duckdb::ExpressionType::COMPARE_GREATERTHAN;
  float t = static_cast<float>(c);
  if (strict ? static_cast<double>(t) > c : static_cast<double>(t) >= c) {
    t = std::nextafterf(t, kNone);
  }
  exact = true;
  return t;
}

// Bare IS [NOT] NULL over (non-try casts of) the column reference: the
// validity child alone answers it, so the scan can skip the data decode. A
// try-cast is not unwrapped -- it turns cast failures into NULLs, so its
// nullness differs from the column's.
irs::NullCheckKind DetectNullCheck(const duckdb::Expression& expr) {
  const auto type = expr.GetExpressionType();
  if ((type != duckdb::ExpressionType::OPERATOR_IS_NULL &&
       type != duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) ||
      expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_OPERATOR) {
    return irs::NullCheckKind::None;
  }
  const auto& children =
    expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
  if (children.size() != 1) {
    return irs::NullCheckKind::None;
  }
  const auto* ref = children.front().get();
  while (ref->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST &&
         !ref->Cast<duckdb::BoundCastExpression>().IsTryCast()) {
    ref = &ref->Cast<duckdb::BoundCastExpression>().Child();
  }
  if (ref->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
    return irs::NullCheckKind::None;
  }
  return type == duckdb::ExpressionType::OPERATOR_IS_NULL
           ? irs::NullCheckKind::IsNull
           : irs::NullCheckKind::IsNotNull;
}

namespace {

// Vectorized score-emit op for the pushed score filter -- maps a FLOAT column
// of raw "larger = nearer" scores to the user-facing value for `E`.
template<ScoreEmit E>
void EmitScoreFilterExec(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<float, float>(
    args.data[0], result, args.size(),
    [](float score) { return ApplyScoreEmit(E, score); });
}

// Wraps a raw score reference in the emit op, so a predicate written in the
// user-facing score evaluates correctly against the raw score vector.
duckdb::unique_ptr<duckdb::Expression> WrapScoreEmit(
  duckdb::unique_ptr<duckdb::Expression> score_ref, ScoreEmit emit) {
  duckdb::scalar_function_t exec;
  switch (emit) {
    case ScoreEmit::SqrtNeg:
      exec = EmitScoreFilterExec<ScoreEmit::SqrtNeg>;
      break;
    case ScoreEmit::OneMinus:
      exec = EmitScoreFilterExec<ScoreEmit::OneMinus>;
      break;
    case ScoreEmit::Negate:
      exec = EmitScoreFilterExec<ScoreEmit::Negate>;
      break;
    case ScoreEmit::Identity:
      return score_ref;
  }
  duckdb::ScalarFunction fn(duckdb::Identifier{"sdb_score_emit"},
                            {duckdb::LogicalType::FLOAT},
                            duckdb::LogicalType::FLOAT, std::move(exec));
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(std::move(score_ref));
  return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children), nullptr);
}

// A pushed score-column filter references the (single) score column via
// BoundReferenceExpression; wrap each such reference so `emit` is baked into
// the predicate.
void WrapScoreRefsWithEmit(duckdb::unique_ptr<duckdb::Expression>& expr,
                           ScoreEmit emit) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
    expr = WrapScoreEmit(std::move(expr), emit);
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      WrapScoreRefsWithEmit(child, emit);
    });
}

}  // namespace

// Classifies the pushed filters: score-column filters and covered `.col`
// filters are applied in-scan (`col_filters`); a filter on a lookup
// (source-only) column sets `has_lookup_filter` -- it is forwarded to the
// source's native lookup scan via `pushed_filters` and forces the streaming
// path (the lookup can't run per collector candidate).
void BuildTableFilter(IResearchScanGlobalState& state,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters) {
  const catalog::InvertedIndex* index_meta =
    bind_data.IsInvertedIndexEntry() ? bind_data.inverted_index.get() : nullptr;
  // Score-column filters, applied on the computed score vector (whatever
  // HandleScoreFilter left pushed: on top-k the collector-enforced conjuncts
  // were stripped; the floor was recorded on the bind data there). The
  // dynamic TOP_N boundary's shared runtime bound is captured for WAND
  // seeding -- it may sit alone or AND-combined with other score predicates.
  const auto score_emit = bind_data.vector_scorer
                            ? bind_data.vector_scorer->score_emit
                            : ScoreEmit::Identity;
  const auto push_score_filter = [&](const duckdb::TableFilter& filter) {
    // The scan holds raw "larger = nearer" scores, but a score predicate is
    // written in the user-facing score. For a non-identity emit, bake the emit
    // into a copy of the predicate so FilterScores evaluates it in the right
    // space -- the raw scores stay intact for the collector's ranking.
    const duckdb::TableFilter* pushed = &filter;
    if (score_emit != ScoreEmit::Identity) {
      auto adjusted = duckdb::ExpressionFilter::GetExpressionFilter(
                        filter, "BuildTableFilter")
                        .expr->Copy();
      WrapScoreRefsWithEmit(adjusted, score_emit);
      auto owned =
        duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(adjusted));
      pushed = owned.get();
      state.emit_score_filters.push_back(std::move(owned));
    }
    state.col_filters.push_back(
      {.field = 0, .filter = pushed, .is_score = true});
    const auto& expr = *duckdb::ExpressionFilter::GetExpressionFilter(
                          *pushed, "BuildTableFilter")
                          .expr;
    if (auto dyn =
          duckdb::ExpressionFilter::GetOptionalDynamicFilterData(expr)) {
      state.score_dynamic_filter = std::move(dyn);
      return;
    }
    if (expr.GetExpressionClass() ==
          duckdb::ExpressionClass::BOUND_CONJUNCTION &&
        expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
      for (const auto& child :
           expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
        if (auto dyn =
              duckdb::ExpressionFilter::GetOptionalDynamicFilterData(*child)) {
          state.score_dynamic_filter = std::move(dyn);
          return;
        }
      }
    }
  };
  for (const auto& entry : filters) {
    const duckdb::idx_t proj_idx = entry.GetIndex();
    // A filter on the computed score column is applied on the score vector by
    // TableFilterDocIterator (no columnstore field). Detect it either as the
    // projected score slot or -- when the score is filter-only -- by its
    // resolved column id.
    if (proj_idx == state.score_output_idx) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto bind_index = state.projected_columns[proj_idx];
    if (bind_index == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.column_ids[bind_index];
    if (col_id == catalog::Column::kInvertedIndexScoreId) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto* info =
      index_meta ? index_meta->FindColumnInfo(col_id) : nullptr;
    const bool index_stored = !index_meta || (info && info->IsStored());
    if (!index_stored) {
      state.has_lookup_filter = true;
    } else if (index_meta != nullptr || bind_data.IsSearchTableEntry()) {
      // Covered columnstore column filtered in-scan (codec Filter + zonemap),
      // keyed by the columnstore field id: an INCLUDE'd inverted-index column,
      // or -- for a search table, where every column lives in `.col` -- any
      // column.
      auto& cf = state.col_filters.emplace_back();
      cf.field = static_cast<irs::field_id>(col_id.id());
      cf.filter = &entry.Filter();
      cf.is_dynamic = duckdb::ExpressionFilter::ContainsInternalFunction(
        *duckdb::ExpressionFilter::GetExpressionFilter(entry.Filter(),
                                                       "BuildTableFilter")
           .expr,
        duckdb::DynamicFilterScalarFun::NAME);
      cf.zonemap_only =
        duckdb::ExpressionFilter::IsRootNonSelectivityOptionalFilter(
          entry.Filter());
      const auto& expr = *duckdb::ExpressionFilter::GetExpressionFilter(
                            entry.Filter(), "BuildTableFilter")
                            .expr;
      cf.null_check = DetectNullCheck(expr);
      cf.type = bind_data.column_types[bind_index];
      cf.not_null = MakeNotNullReplacement(entry.Filter(),
                                           bind_data.column_types[bind_index]);
    }
  }
}

void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out) {
  // col->field1 -- exactly one child assumed.
  SDB_ASSERT(column_index.ChildIndexCount() == 1);
  const duckdb::ColumnIndex* node = &column_index.GetChildIndex(0);
  const duckdb::LogicalType* cur_type = &root_type;
  while (true) {
    if (node->HasPrimaryIndex()) {
      SDB_ASSERT(cur_type->id() == duckdb::LogicalTypeId::STRUCT,
                 "Numeric identifiers are only in structs");
      const auto node_index = node->GetPrimaryIndex();
      const auto& children = duckdb::StructType::GetChildTypes(*cur_type);
      SDB_ASSERT(node_index < children.size(), "Invalid index node");
      out.emplace_back(children[node_index].first.GetIdentifierName());
      cur_type = &children[node_index].second;
    } else {
      out.emplace_back(node->GetFieldName());
    }
    if (!node->HasChildren()) {
      break;
    }
    // col->field1->field2->... etc. Each indirection has exactly one child.
    SDB_ASSERT(node->ChildIndexCount() == 1);
    node = &node->GetChildIndex(0);
  }
}

// Disposition of every scanned real column: a column in the output is
// materialized (from `.col` when covered, else through the lookup source); a
// filter-only column (kept in the scanned chunk by filter_prune, dropped from
// the output by projection_ids) is evaluated in scratch by the covered chain
// or applied natively by the lookup source's own scan; a column needed by
// neither -- left dangling by a statistics-eliminated filter -- is read
// nowhere. `needs_lookup` therefore holds exactly when a lookup column is
// needed for the output or for a pushed filter.
void ClassifyColumnstoreProjections(IResearchScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data) {
  const auto in_output = [&](duckdb::idx_t proj) {
    return state.output_projection_ids.empty() ||
           absl::c_find(state.output_projection_ids, proj) !=
             state.output_projection_ids.end();
  };
  if (bind_data.IsSearchTableEntry()) {
    // Search table: every column lives in `.col`, so all projections are
    // covered and there is no lookup source.
    std::vector<std::string_view> path;
    for (duckdb::idx_t proj = 0; proj < state.projected_columns.size();
         ++proj) {
      const auto bind_col = state.projected_columns[proj];
      if (bind_col == duckdb::DConstants::INVALID_INDEX) {
        continue;
      }
      state.lookup_projected_columns[proj] = duckdb::DConstants::INVALID_INDEX;
      if (!in_output(proj)) {
        continue;
      }
      const auto col_id = bind_data.column_ids[bind_col];
      ColumnstoreProjection cp{.output_slot = proj, .column_id = col_id.id()};
      if (proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.column_types[bind_col],
                            path);
          if (!path.empty()) {
            cp.extract_path = std::move(path);
            cp.extract_scan_type = column_index.GetScanType();
          }
        }
      }
      state.cs_projections.emplace_back(std::move(cp));
    }
    if (state.generated_pk_output_idx != duckdb::DConstants::INVALID_INDEX) {
      state.cs_projections.emplace_back(ColumnstoreProjection{
        .output_slot = state.generated_pk_output_idx,
        .column_id = catalog::Column::kGeneratedPKId.id()});
    }
    return;
  }
  if (!bind_data.IsInvertedIndexEntry() || !bind_data.inverted_index) {
    // Nothing is covered: every output column materializes through the source
    // (base-table scans never receive pushed filters, so output columns are
    // the only real ones scanned).
    state.needs_lookup = state.has_real_column;
    return;
  }

  std::vector<std::string_view> path;
  for (duckdb::idx_t proj = 0; proj < state.projected_columns.size(); ++proj) {
    const auto bind_col = state.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.column_ids[bind_col];
    const auto* info = bind_data.inverted_index->FindColumnInfo(col_id);
    if (info && info->IsStored()) {
      state.lookup_projected_columns[proj] = duckdb::DConstants::INVALID_INDEX;
      if (!in_output(proj)) {
        continue;
      }
      ColumnstoreProjection cp{.output_slot = proj, .column_id = col_id.id()};
      if (info->store_values && proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.column_types[bind_col],
                            path);
          if (!path.empty()) {
            cp.extract_path = std::move(path);
            cp.extract_scan_type = column_index.GetScanType();
          }
        }
      }
      state.cs_projections.emplace_back(std::move(cp));
      continue;
    }
    if (in_output(proj) ||
        (state.pushed_filters != nullptr &&
         state.pushed_filters->HasFilter(duckdb::ProjectionIndex{proj}))) {
      state.needs_lookup = true;
    } else {
      state.lookup_projected_columns[proj] = duckdb::DConstants::INVALID_INDEX;
    }
  }
}

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  auto& gstate = input.global_state->Cast<IResearchScanGlobalState>();
  input.operator_metrics.rows_scanned =
    gstate.produced_rows.load(std::memory_order_relaxed);
}

void ApplyScoreEmit(const IResearchScanGlobalState& gstate, float* scores,
                    duckdb::idx_t n) {
  // Map in place at the emit boundary so the output vector sees the user-facing
  // value. Text scores are already user-facing (no vector scorer).
  if (gstate.vector_scorer == nullptr) {
    return;
  }
  const auto emit = gstate.vector_scorer->score_emit;
  if (emit == ScoreEmit::Identity) {
    return;
  }
  for (duckdb::idx_t i = 0; i < n; ++i) {
    scores[i] = ApplyScoreEmit(emit, scores[i]);
  }
}

void AccountAndWriteVirtualColumns(IResearchScanGlobalState& gstate,
                                   duckdb::idx_t num_rows,
                                   duckdb::Vector* scores,
                                   duckdb::DataChunk& output) {
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  if (gstate.tableoid_output_idx != duckdb::DConstants::INVALID_INDEX) {
    auto* tableoid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.tableoid_output_idx]);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      tableoid_data[i] = gstate.tableoid_value;
    }
  }
  if (!gstate.ScanScore()) {
    return;
  }
  // Scores arrive already mapped to the user-facing value (ApplyScoreEmit runs
  // at the emit boundary) in the batcher's staged vector: reference it.
  SDB_ASSERT(scores != nullptr);
  output.data[gstate.score_output_idx].Reference(*scores);
}

namespace {

const irs::Filter& MatchAllFilter() {
  static const irs::All kInstance;
  return kInstance;
}

uint32_t ReadRerankFactor(duckdb::ClientContext& context) {
  return ReadBoundedIntSetting(context, "sdb_rerank_factor", 0, 4);
}

void RerankHits(IResearchScanGlobalState& g, std::span<irs::ScoreDoc> hits) {
  SDB_ASSERT(g.vector_scorer != nullptr);
  SDB_ASSERT(g.reader != nullptr);
  const auto& vs = *g.vector_scorer;
  const std::span<const float> query{vs.query_vector};
  const auto d = static_cast<uint32_t>(vs.query_vector.size());
  size_t i = 0;
  while (i < hits.size()) {
    const uint32_t seg = hits[i].segment_idx;
    size_t j = i + 1;
    while (j < hits.size() && hits[j].segment_idx == seg) {
      ++j;
    }
    const auto& sub = (*g.reader)[seg];
    if (const auto* vec_col = sub.Column(vs.field_id); vec_col != nullptr) {
      irs::RerankExactDistances(sub, *vec_col, d, query, vs.metric,
                                hits.subspan(i, j - i));
    }
    i = j;
  }
}

// Current lower-bound score from the dynamic TOP_N boundary, or min() when it
// is not yet initialized or is not a lower bound (text-only path: WAND scores
// are strictly positive). Seeds the streaming WAND threshold; the exact
// boundary is still enforced by the HitBatcher score filter, so an over-loose
// threshold only skips fewer blocks (never wrong). WAND pruning is strict
// (skips `block_max <= threshold`), so a `>=` boundary steps one ulp down --
// docs scoring exactly the boundary are still needed for tie-breaking.
irs::score_t CurrentWandThreshold(duckdb::DynamicFilterData& dyn) {
  std::lock_guard<duckdb::mutex> guard(dyn.lock);
  if (!dyn.initialized.load(std::memory_order_relaxed) ||
      dyn.constant.IsNull() ||
      (dyn.comparison_type != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
       dyn.comparison_type !=
         duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO)) {
    return std::numeric_limits<irs::score_t>::min();
  }
  const auto boundary = dyn.constant.GetValue<float>();
  if (dyn.comparison_type ==
      duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
    return std::nextafter(boundary, std::numeric_limits<irs::score_t>::min());
  }
  return boundary;
}

// The fastest mode that can apply every pushed filter and materialize every
// output column, decided once against the column dispositions
// (ClassifyColumnstoreProjections runs first). `.col` filters run in every
// mode (codec filter + zonemap); a score filter only runs where scores are
// computed and the lookup source only materializes on the top-k/streaming
// paths -- and is engaged exactly when a lookup column is needed
// (needs_lookup), for a filter or for the output. CountFast additionally
// requires no predicate and no filters at all -- the whole-reader live count
// is the answer.
ScanMode DecideScanMode(const IResearchScanGlobalState& g,
                        const SereneDBScanBindData& ss) {
  if (ss.TsDictMode()) {
    return ScanMode::TsDict;
  }
  const bool score_filter = absl::c_any_of(
    g.col_filters,
    [](const IResearchScanGlobalState::ColFilter& f) { return f.is_score; });
  if (!g.has_output_column && !g.needs_lookup && !score_filter) {
    return ss.IsMatchAll() && g.col_filters.empty() ? ScanMode::CountFast
                                                    : ScanMode::Count;
  }
  // Vector (ANN) search tolerates a lookup filter on the Top: path: the
  // collector over-fetches a pool, the lookup filter drops non-matches during
  // materialization, and the TOP_N above trims to the exact k. A text lookup
  // filter still forces streaming (exact), so it never reaches here anyway
  // (score_top_k stays unset -- AfterLimit).
  if (ss.score_top_k && (ss.text_scorer || ss.score_order) &&
      (!g.has_lookup_filter || ss.vector_scorer)) {
    return ScanMode::TopK;
  }
  // The FullScanner case: match-all with everything needed covered in `.col`.
  if (ss.IsMatchAll() && !ss.EmitOffsets() && !ss.text_scorer &&
      !g.ScanScore() && !g.needs_lookup && g.has_real_column) {
    return ScanMode::ColScan;
  }
  return ScanMode::Stream;
}

}  // namespace
namespace {

// Best-first scheduling key: a segment or unit index and the order column's
// most optimistic statistic for it. Sorting is scheduling only (the TopN
// above still sorts), so entries with unusable statistics simply fall to the
// null end of the walk.
struct ScanOrderKey {
  uint32_t id;
  duckdb::Value value;
};

void SortScanOrderKeys(std::vector<ScanOrderKey>& keys,
                       const SereneDBScanBindData::ScanOrder& order) {
  const bool nulls_first =
    order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  absl::c_sort(keys, [&](const ScanOrderKey& l, const ScanOrderKey& r) {
    const bool ln = l.value.IsNull();
    const bool rn = r.value.IsNull();
    if (ln || rn) {
      return ln != rn ? (ln ? nulls_first : !nulls_first) : l.id < r.id;
    }
    const auto& lo = asc ? l.value : r.value;
    const auto& hi = asc ? r.value : l.value;
    return lo != hi ? lo < hi : l.id < r.id;
  });
}

}  // namespace

// ORDER BY <covered column> LIMIT: order segments best-first by the order
// key's per-file statistics -- the whole-file analogue of duckdb's
// RowGroupReorderer.
void BuildSegmentScanOrder(IResearchScanGlobalState& g,
                           const SereneDBScanBindData::ScanOrder& order) {
  const auto field = static_cast<irs::field_id>(order.column.id());
  std::vector<ScanOrderKey> keys;
  keys.reserve(g.claimable_segments);
  for (uint32_t claimed = 0; claimed < g.claimable_segments; ++claimed) {
    const auto si = g.SegmentAt(claimed);
    duckdb::Value v;
    const auto* col_reader = (*g.reader)[si].GetColReader();
    const auto* reader = col_reader ? col_reader->Column(field) : nullptr;
    if (reader) {
      v = duckdb::RowGroupReorderer::RetrieveStat(
        reader->MergedStatistics(), order.order_by, order.column_type);
    }
    keys.push_back({si, std::move(v)});
  }
  SortScanOrderKeys(keys, order);
  g.segment_order.clear();
  g.segment_order.reserve(keys.size());
  for (const auto& k : keys) {
    g.segment_order.push_back(k.id);
  }
}

namespace {

// The order key's most optimistic statistic inside [begin, end): the best
// per-column-segment stat endpoint for the requested direction. Null when no
// column segment in the range has a usable statistic.
duckdb::Value UnitOrderKey(const irs::ColumnReader& reader,
                           const SereneDBScanBindData::ScanOrder& order,
                           uint64_t begin, uint64_t end) {
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  duckdb::Value best;
  irs::BlockWindow w;
  for (uint64_t row = begin; row < end; row = w.end) {
    w = reader.Locate(row, w);
    auto v = duckdb::RowGroupReorderer::RetrieveStat(
      reader.RowGroupStatistics(w.block), order.order_by, order.column_type);
    if (v.IsNull()) {
      continue;
    }
    if (best.IsNull() || (asc ? v < best : best < v)) {
      best = std::move(v);
    }
  }
  return best;
}

// ORDER BY <covered column> LIMIT, one level below BuildSegmentScanOrder:
// claim a segment's bulk units best-first by the order key's per-column-
// segment statistics, so the TopN dynamic bound tightens on the best unit
// first and DeadUntil kills the rest -- rows within a file are laid out in
// insert order, which correlates with the key exactly when it matters.
// Reorders col_scan.units[first_unit..] in place (they all belong to one
// segment).
void OrderSegmentScanUnits(IResearchScanGlobalState& g,
                           const irs::SubReader& seg,
                           const SereneDBScanBindData::ScanOrder& order,
                           size_t first_unit) {
  const auto field = static_cast<irs::field_id>(order.column.id());
  const auto* col_reader = seg.GetColReader();
  const auto* reader = col_reader ? col_reader->Column(field) : nullptr;
  if (reader == nullptr) {
    return;
  }
  const std::span units{g.col_scan.units.data() + first_unit,
                        g.col_scan.units.size() - first_unit};
  std::vector<ScanOrderKey> keys;
  keys.reserve(units.size());
  for (uint32_t i = 0; i < units.size(); ++i) {
    keys.push_back({i, UnitOrderKey(*reader, order, units[i].begin,
                                    units[i].begin + units[i].count)});
  }
  SortScanOrderKeys(keys, order);
  std::vector<IResearchScanGlobalState::ScanUnit> reordered;
  reordered.reserve(units.size());
  for (const auto& k : keys) {
    reordered.push_back(units[k.id]);
  }
  absl::c_copy(reordered, units.begin());
}

}  // namespace

void ClassifySegmentColFilters(
  const irs::SubReader& seg, IResearchScanGlobalState& g,
  ColFilterStateCache& states,
  TableFilterDocIterator::SegmentClassification& out);

irs::DocIterator::ptr MaybeWrapColFilter(
  irs::DocIterator::ptr inner, const irs::SubReader& seg,
  std::span<const TableFilterDocIterator::FilterSpec> active,
  IResearchScanGlobalState& g, ColFilterStateCache& states);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<IResearchScanGlobalState>();

  InitScanState(*state, context, bind_data, input);

  auto& ss = bind_data;
  if (!ss.offsets.empty() && !ss.stored_filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"));
  }
  state->scan = &ss;
  state->reader = &ss.snapshot->reader;
  state->total_segments = ss.snapshot->reader.size();
  state->claimable_segments = static_cast<uint32_t>(state->total_segments);
  state->vector_scorer = ss.vector_scorer ? &*ss.vector_scorer : nullptr;

  ClassifyColumnstoreProjections(*state, bind_data);
  state->mode = DecideScanMode(*state, ss);
  if (state->mode == ScanMode::TsDict) {
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
    if (ss.stored_filter) {
      state->filter = ss.stored_filter.get();
      state->queries.resize(ss.snapshot->reader.size());
    }
    return duckdb::unique_ptr_cast<IResearchScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }

  if (state->mode == ScanMode::CountFast) {
    // The whole-reader live count answers at init (see InitLocal); no
    // queries, one thread, no segment is touched.
    return duckdb::unique_ptr_cast<IResearchScanGlobalState,
                                   duckdb::GlobalTableFunctionState>(
      std::move(state));
  }
  if (state->needs_lookup && ss.IsInvertedIndexEntry() && ss.inverted_index) {
    const auto pk_kind = ss.inverted_index->GetOptions().pk_column;
    if (pk_kind == catalog::PkColumnKind::None) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("inverted index \"", ss.inverted_index->GetName(),
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
  if (ss.vector_scorer) {
    state->owned_filter = MakeVectorFilter(*ss.vector_scorer, ss.stored_filter,
                                           ss.vector_scorer->EffectiveRadius());
    state->filter = state->owned_filter.get();
  } else {
    state->filter =
      ss.stored_filter ? ss.stored_filter.get() : &MatchAllFilter();
  }
  state->queries.resize(ss.snapshot->reader.size());

  if (!state->col_filters.empty() && state->total_segments != 0) {
    ColFilterStateCache init_states;
    TableFilterDocIterator::SegmentClassification cls;
    state->segment_order.reserve(state->total_segments);
    for (uint32_t si = 0; si < state->total_segments; ++si) {
      ClassifySegmentColFilters((*state->reader)[si], *state, init_states, cls);
      if (!cls.segment_dead) {
        state->segment_order.push_back(si);
      }
    }
    if (state->segment_order.size() == state->total_segments) {
      state->segment_order.clear();
    } else {
      state->claimable_segments =
        static_cast<uint32_t>(state->segment_order.size());
    }
  }

  if (state->mode == ScanMode::Count) {
    return state;
  }

  if (state->mode == ScanMode::TopK || state->mode == ScanMode::Stream) {
    if (ss.text_scorer) {
      state->scorer_obj = catalog::MakeScorer(*ss.text_scorer);
    } else if (ss.score_order) {
      state->scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
    }
    if (state->scorer_obj) {
      state->collectors.resize(state->MaxThreads());
    }
  }

  if (state->mode == ScanMode::TopK) {
    if (state->score_static_floor >
        std::numeric_limits<irs::score_t>::lowest()) {
      // Static score floor (Lucene min_score): the collectors start at the
      // bound and enforce it -- the stripped filter's replacement
      // (HandleScoreFilter recorded the floor only where it is the
      // collector's raw space) -- and WAND skips below-floor blocks from the
      // first window.
      state->topk.global_kth_score.store(state->score_static_floor,
                                         std::memory_order_relaxed);
    }
    if (ss.vector_scorer &&
        (ss.vector_scorer->quant != irs::VectorQuantization::None ||
         state->has_lookup_filter)) {
      state->topk.rerank_pool =
        ReadRerankFactor(context) * static_cast<uint32_t>(*ss.score_top_k);
    }
  } else if (state->mode == ScanMode::Stream) {
    // Streaming text-score WAND: a pushed dynamic TOP_N score boundary (score
    // DESC TOP_N -- only a lower bound can seed block-max skipping) or a
    // static score floor on a WAND-enabled text scorer lets the streaming
    // DocIterator skip below-threshold blocks (its ScoreThresholdAttr is
    // seeded before each emit); the HitBatcher score filter still enforces
    // the exact boundary. Honors the same kill switch as the in-scan top-k
    // rule (IResearchSetScanOrder): with it set, WAND must not engage
    // anywhere.
    duckdb::Value disable_topk;
    const bool topk_disabled =
      context.TryGetCurrentSetting("sdb_disable_top_k_optimization",
                                   disable_topk) &&
      !disable_topk.IsNull() && disable_topk.GetValue<bool>();
    const bool dynamic_bound =
      state->score_dynamic_filter &&
      (state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHAN ||
       state->score_dynamic_filter->comparison_type ==
         duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    const bool static_bound =
      state->score_static_floor > std::numeric_limits<irs::score_t>::lowest();
    state->wand_streaming =
      !topk_disabled && ss.text_scorer && state->ScanScore() &&
      (dynamic_bound || static_bound) &&
      WandEnabled(bind_data.inverted_index.get(), ss.text_scorer);
  }

  if (ss.scan_order &&
      (state->mode == ScanMode::Stream || state->mode == ScanMode::ColScan)) {
    BuildSegmentScanOrder(*state, *ss.scan_order);
  }

  if (state->mode == ScanMode::ColScan) {
    // Search tables carry no inverted_index; fall back to the default unit.
    uint64_t rg_rows = bind_data.inverted_index
                         ? bind_data.inverted_index->GetOptions().row_group_size
                         : 0;
    if (rg_rows == 0) {
      rg_rows = DEFAULT_ROW_GROUP_SIZE;
    }
    const uint64_t unit_rows = rg_rows >= DEFAULT_ROW_GROUP_SIZE
                                 ? rg_rows
                                 : DEFAULT_ROW_GROUP_SIZE / rg_rows * rg_rows;
    // Walking claim order keeps units grouped whole-segment best-first; with
    // a scan order, a segment's bulk units are additionally claimed best-first
    // by the order key's per-column-segment statistics (a deleted segment is
    // one forward masked-streaming unit -- doc iterators cannot reorder).
    for (uint32_t claimed = 0; claimed < state->claimable_segments; ++claimed) {
      const auto si = state->SegmentAt(claimed);
      const auto& seg = (*state->reader)[si];
      const uint64_t docs = seg.docs_count();
      if (docs == 0) {
        continue;
      }
      if (seg.live_docs_count() != docs) {
        state->col_scan.units.push_back({si, 0, docs, /*bulk=*/false});
        continue;
      }
      const auto first_unit = state->col_scan.units.size();
      for (uint64_t begin = 0; begin < docs; begin += unit_rows) {
        state->col_scan.units.push_back(
          {si, begin, std::min(unit_rows, docs - begin), /*bulk=*/true});
      }
      if (ss.scan_order && state->col_scan.units.size() - first_unit > 1) {
        OrderSegmentScanUnits(*state, seg, *ss.scan_order, first_unit);
      }
    }
  }

  return duckdb::unique_ptr_cast<IResearchScanGlobalState,
                                 duckdb::GlobalTableFunctionState>(
    std::move(state));
}

namespace {

const irs::QueryBuilder& EnsureSegmentQuery(IResearchScanGlobalState& g,
                                            IResearchScanLocalState& l,
                                            const irs::SubReader& seg,
                                            uint32_t seg_idx) {
  auto& q = g.queries[seg_idx];
  if (!q) {
    irs::PrepareCollector* collector = nullptr;
    if (g.scorer_obj) {
      if (!l.prepare_collector) {
        const uint32_t slot =
          g.collector_slots.fetch_add(1, std::memory_order_relaxed);
        SDB_ASSERT(slot < g.collectors.size());
        g.collectors[slot] = g.filter->MakeCollector(g.scorer_obj.get());
        l.prepare_collector = g.collectors[slot].get();
      }
      collector = l.prepare_collector;
    }
    q = g.filter->PrepareSegment(seg, {.collector = collector});
  }
  return *q;
}

void PreparePhase(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                  IResearchScanLocalState& l) {
  for (;;) {
    const auto seg = g.prepare_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg >= g.total_segments) {
      break;
    }
    SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
    EnsureSegmentQuery(g, l, (*g.reader)[seg], seg);
    if (g.prepare_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
        g.total_segments) {
      const uint32_t used = g.collector_slots.load(std::memory_order_relaxed);
      auto& merged = *g.collectors[0];
      merged.MergeAll([&](irs::PrepareCollector::MergeSink sink) {
        for (uint32_t i = 1; i < used; ++i) {
          sink(*g.collectors[i]);
        }
      });
      g.stats.emplace(merged.Finish(irs::IResourceManager::gNoop));
      g.prepare_finished.Notify();
      return;
    }
  }
  g.prepare_finished.WaitForNotification();
}

void WriteChunkOffsets(std::vector<FieldEntry>& offsets_entries,
                       uint32_t& offsets_prepped_seg,
                       std::vector<highlight::HitRange>& offsets_doc_scratch,
                       const IResearchScanGlobalState& g, const HitsChunk& view,
                       duckdb::DataChunk& output) {
  if (offsets_entries.empty()) {
    return;
  }
  for (const auto& entry : offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  const irs::IndexReader& reader = *g.reader;
  const irs::SubReader* cached_seg = nullptr;
  for (size_t i = 0; i < view.size(); ++i) {
    const uint32_t seg_idx = view.seg(i);
    if (seg_idx != offsets_prepped_seg) {
      for (auto& entry : offsets_entries) {
        entry.state.Clear();
      }
      cached_seg = &reader[seg_idx];
      OffsetsCollector visitor{offsets_entries};
      const auto& seg_query = g.queries[seg_idx];
      SDB_ASSERT(seg_query);
      seg_query->Visit(visitor, irs::kNoBoost);
      offsets_prepped_seg = seg_idx;
    }
    for (auto& entry : offsets_entries) {
      FillRowOffsets(entry.state, *cached_seg, view.docs[i], entry.limit,
                     offsets_doc_scratch);
      WriteRowOffsets(output.data[entry.output_idx],
                      static_cast<duckdb::idx_t>(i), offsets_doc_scratch);
    }
  }
}

template<typename Lstate>
void BuildOffsetsEntries(Lstate& lstate, duckdb::TableFunctionInitInput& input,
                         const SereneDBScanBindData& bd) {
  const auto& ss = bd;
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
      entry.id = ss.offsets[ss_idx].column_id;
      lstate.offsets_entries.push_back(std::move(entry));
    }
    ++out_slot;
  }
}

uint32_t CountDocs(irs::DocIterator::ptr docs, const irs::SubReader& seg,
                   bool count_all, const ColFilterCtx& cf) {
  docs = MaybeWrapColFilter(std::move(docs), seg, cf.active, *cf.g, *cf.states);
  if (count_all) {
    return static_cast<uint32_t>(docs->count());
  }
  return irs::doc_limits::eof(docs->advance()) ? 0 : 1;
}

uint32_t WhereLiveDocs(irs::TermIterator& it, const irs::SubReader& seg,
                       const irs::QueryBuilder& where, bool count_all,
                       const ColFilterCtx& cf) {
  std::vector<irs::ScoreAdapter> itrs;
  itrs.reserve(2);
  itrs.emplace_back(it.postings(irs::IndexFeatures::None));
  itrs.emplace_back(where.Execute({}, irs::StatsBuffer::Empty()));
  return CountDocs(irs::MakeConjunction(irs::ScoreMergeType::Noop, {},
                                        seg.docs_count(), std::move(itrs)),
                   seg, count_all, cf);
}

uint32_t MaskedLiveDocs(irs::TermIterator& it, const irs::SubReader& seg,
                        bool count_all, const ColFilterCtx& cf) {
  return CountDocs(seg.mask(it.postings(irs::IndexFeatures::None)), seg,
                   count_all, cf);
}

class MinMaxTermsIterator : public irs::TermIterator {
 public:
  MinMaxTermsIterator(const std::array<irs::bytes_view, 2>& terms,
                      size_t count) noexcept
    : _terms{terms}, _count{count} {}

  bool next() final {
    if (_next == _count) {
      return false;
    }
    ++_next;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _terms[_next - 1]; }

  void read() final {}

  irs::DocIterator::ptr postings(irs::IndexFeatures /*features*/) const final {
    return irs::DocIterator::empty();
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id /*id*/) noexcept final {
    return nullptr;
  }

 private:
  std::array<irs::bytes_view, 2> _terms;
  size_t _count;
  size_t _next = 0;
};

uint32_t NullFieldLiveCount(const irs::SubReader& seg, irs::field_id field,
                            TsDictLocalState::CountMode count_mode,
                            const irs::QueryBuilder* where_query,
                            const ColFilterCtx& cf) {
  using Mode = TsDictLocalState::CountMode;
  const auto* reader = seg.field(field);
  if (!reader) {
    return 0;
  }
  if (count_mode == Mode::Meta && cf.active.empty() &&
      seg.live_docs_count() == seg.docs_count()) {
    return static_cast<uint32_t>(reader->docs_count());
  }
  auto it = reader->iterator(irs::SeekMode::NORMAL);
  SDB_ASSERT(it);
  if (!it->next()) {
    return 0;
  }
  const auto live = count_mode == Mode::Where
                      ? WhereLiveDocs(*it, seg, *where_query, true, cf)
                      : MaskedLiveDocs(*it, seg, true, cf);
  SDB_ASSERT(!it->next());
  return static_cast<uint32_t>(live);
}

// One walk over the scanned columns: the n-th occurrence of a ts-dict virtual
// column maps to the n-th request that asked for that column kind.
void BuildTsDictSlots(TsDictLocalState& lstate,
                      duckdb::TableFunctionInitInput& input,
                      const SereneDBScanBindData& bd) {
  using duckdb::DConstants;
  using Req = SereneDBScanBindData::TsDictRequest;
  using Field = TsDictLocalState::FieldState;

  struct SlotKind {
    catalog::Column::Id cat;
    duckdb::idx_t Req::* req;
    duckdb::idx_t Field::* slot;
    size_t next = 0;
  };
  std::array<SlotKind, 5> kinds{{
    {catalog::Column::kInvertedIndexTermId, &Req::term_col_idx,
     &Field::term_slot},
    {catalog::Column::kInvertedIndexTermRawId, &Req::term_raw_col_idx,
     &Field::term_raw_slot},
    {catalog::Column::kInvertedIndexTermCountId, &Req::count_col_idx,
     &Field::count_slot},
    {catalog::Column::kInvertedIndexTermFreqId, &Req::freq_col_idx,
     &Field::freq_slot},
    {catalog::Column::kInvertedIndexTermScoreId, &Req::score_col_idx,
     &Field::score_slot},
  }};

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
    const auto cat = bd.column_ids[col_id];
    for (auto& kind : kinds) {
      if (cat != kind.cat) {
        continue;
      }
      while (bd.ts_dicts[kind.next].*kind.req == DConstants::INVALID_INDEX) {
        ++kind.next;
      }
      lstate.fields[kind.next++].*kind.slot = out_slot;
      break;
    }
    ++out_slot;
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& /*context*/, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<IResearchScanGlobalState>();
  const auto& bd = input.bind_data->Cast<SereneDBScanBindData>();
  if (gstate.mode == ScanMode::TsDict) {
    auto lstate = duckdb::make_uniq<TsDictLocalState>();
    const auto& ss = bd;
    lstate->fields.resize(ss.ts_dicts.size());
    for (size_t i = 0; i < ss.ts_dicts.size(); ++i) {
      lstate->fields[i].field_id = ss.ts_dicts[i].field_id;
      lstate->fields[i].null_field_id = ss.ts_dicts[i].null_field_id;
      lstate->fields[i].term_uses = ss.ts_dicts[i].term_uses;
      lstate->fields[i].having_filter = ss.ts_dicts[i].having_filter.get();
    }
    BuildTsDictSlots(*lstate, input, bd);
    return lstate;
  }
  if (gstate.mode == ScanMode::CountFast || gstate.mode == ScanMode::Count) {
    auto lstate = duckdb::make_uniq<CountScanLocalState>();
    if (gstate.mode == ScanMode::CountFast) {
      lstate->local_count = gstate.reader->live_docs_count();
      lstate->segments_exhausted = true;
    }
    return lstate;
  }
  if (gstate.mode == ScanMode::TopK) {
    auto lstate = duckdb::make_uniq<TopKScanLocalState>();
    if (!gstate.vector_scorer) {
      lstate->local_threshold = std::numeric_limits<irs::score_t>::min();
    }
    const size_t k =
      gstate.topk.rerank_pool ? gstate.topk.rerank_pool : *bd.score_top_k;
    lstate->hit_buf.resize(irs::BlockSize(k));
    lstate->hit_slice = std::span<irs::ScoreDoc>{lstate->hit_buf};
    BuildOffsetsEntries(*lstate, input, bd);
    return lstate;
  }
  if (gstate.mode == ScanMode::ColScan) {
    return duckdb::make_uniq<ColScanLocalState>();
  }
  auto lstate = duckdb::make_uniq<StreamScanLocalState>();
  BuildOffsetsEntries(*lstate, input, bd);
  return lstate;
}

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<IResearchScanGlobalState>();
  // filter_prune: fill the scan's own column_ids-sized chunk (filter-only
  // columns included, read for the filter), then reference just the projected
  // subset into output -- a reorder that drops filter-only columns, exactly
  // like PhysicalTableScan does with all_columns/projection_ids.
  duckdb::DataChunk* target = &output;
  if (!gstate.output_projection_ids.empty()) {
    auto& base = data.local_state->Cast<IResearchScanLocalState>();
    if (base.scan_chunk.ColumnCount() == 0) {
      duckdb::vector<duckdb::LogicalType> types(gstate.projected_types.begin(),
                                                gstate.projected_types.end());
      base.scan_chunk.Initialize(context, types);
    }
    base.scan_chunk.Reset();
    target = &base.scan_chunk;
  }
  auto& out = *target;
  switch (gstate.mode) {
    case ScanMode::TsDict: {
      auto& l = data.local_state->Cast<TsDictLocalState>();
      RunTsDictScan(context, gstate, l, out);
      break;
    }
    case ScanMode::CountFast:
    case ScanMode::Count: {
      auto& l = data.local_state->Cast<CountScanLocalState>();
      RunCountScan(gstate, l, out);
      break;
    }
    case ScanMode::TopK: {
      auto& l = data.local_state->Cast<TopKScanLocalState>();
      if (!l.prepared) {
        if (gstate.total_segments != 0 && !gstate.vector_scorer) {
          PreparePhase(context, gstate, l);
        }
        l.prepared = true;
      }
      RunTopKScan(context, gstate, l, out);
      break;
    }
    case ScanMode::ColScan: {
      auto& l = data.local_state->Cast<ColScanLocalState>();
      RunColScan(context, gstate, l, out);
      break;
    }
    case ScanMode::Stream: {
      auto& l = data.local_state->Cast<StreamScanLocalState>();
      if (!l.prepared) {
        if (gstate.scorer_obj && gstate.total_segments != 0 &&
            !gstate.vector_scorer) {
          PreparePhase(context, gstate, l);
        }
        l.prepared = true;
      }
      RunStreamingScan(context, gstate, l, out);
      break;
    }
  }
  if (target != &output) {
    output.ReferenceColumns(*target, gstate.output_projection_ids);
  }
}

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  if (!bind_data || !options || !options->row_limit.IsValid() ||
      !options->single_order_key) {
    return;
  }
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_disable_top_k_optimization", v) &&
      !v.IsNull() && v.GetValue<bool>()) {
    return;
  }
  auto& bd = bind_data->Cast<SereneDBScanBindData>();
  const auto order_col = options->column_idx.GetPrimaryIndex();
  if (order_col >= bd.column_ids.size()) {
    return;
  }
  const auto col_id = bd.column_ids[order_col];
  if (col_id != catalog::Column::kInvertedIndexScoreId) {
    // ORDER BY <covered .col column> LIMIT: iterate segments best-first by the
    // column's per-file statistics (duckdb's row-group reorder, one level up).
    // Only covered columns have `.col` statistics.
    const auto* info =
      bd.inverted_index ? bd.inverted_index->FindColumnInfo(col_id) : nullptr;
    const bool stored =
      bd.IsSearchTableEntry() || (info != nullptr && info->IsStored());
    if (stored && !bd.scan_order) {
      bd.scan_order = SereneDBScanBindData::ScanOrder{
        col_id, options->order_type, options->null_order, options->order_by,
        options->column_type};
    }
    return;
  }
  auto& search_scan = bd;
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

// Wraps `inner` so it yields only docs whose covered `.col` values pass the
// segment's active filters (codec Filter + zonemap). An empty `active` set
// (nothing pushed, or everything ALWAYS_TRUE for this segment) returns the
// inner iterator unwrapped -- the non-filtered path.
// Transparent to every DocIterator consumer (count/Collect/EmitScoredDocs).
irs::DocIterator::ptr MaybeWrapColFilter(
  irs::DocIterator::ptr inner, const irs::SubReader& seg,
  std::span<const TableFilterDocIterator::FilterSpec> active,
  IResearchScanGlobalState& g, ColFilterStateCache& states) {
  if (active.empty()) {
    return inner;
  }
  const auto* col_reader = seg.GetColReader();
  SDB_ASSERT(col_reader != nullptr,
             "`.col` table filter requires a columnstore segment");
  return irs::memory::make_managed<TableFilterDocIterator>(
    std::move(inner), *col_reader, active, *g.client_context, states);
}

// Whole-segment classification of the pushed `.col`/score filters against the
// segment's per-column file statistics -- duckdb RowGroup::CheckZonemap one
// stats level up. ALWAYS_FALSE/FALSE_OR_NULL for any filter kills the segment
// before its postings are iterated (NULL evaluations don't pass a WHERE filter
// either); ALWAYS_TRUE drops the filter for this segment (zonemap-only filters
// stay: their bound can tighten later); TRUE_OR_NULL swaps in the pre-built
// IS NOT NULL filter (duckdb propagate_get policy). A column absent from the
// segment's columnstore is all NULL there: the filter evaluates once against a
// single NULL -- failing kills the segment, passing drops the filter (nothing
// to prune, and there is no reader to bind). Score specs (computed, no stored
// stats) stay active. Writes into `out` so the caller's buffers are reused
// across segments.
void ClassifySegmentColFilters(
  const irs::SubReader& seg, IResearchScanGlobalState& g,
  ColFilterStateCache& states,
  TableFilterDocIterator::SegmentClassification& out) {
  out.segment_dead = false;
  out.active.clear();
  if (g.col_filters.empty()) {
    return;
  }
  const auto* col_reader = seg.GetColReader();
  for (const auto& cf : g.col_filters) {
    TableFilterDocIterator::FilterSpec spec{
      .field = cf.field,
      .filter = cf.filter,
      .is_score = cf.is_score,
      .is_dynamic = cf.is_dynamic,
      .zonemap_only = cf.zonemap_only,
      .null_check = cf.null_check,
      .not_null = cf.not_null.get(),
    };
    if (spec.is_score) {
      out.active.push_back(spec);
      continue;
    }
    const auto* reader =
      col_reader != nullptr ? col_reader->Column(spec.field) : nullptr;
    if (reader == nullptr) {
      duckdb::Vector null_row{duckdb::Value{cf.type}, duckdb::count_t{1}};
      duckdb::SelectionVector sel;
      duckdb::idx_t approved = 1;
      duckdb::ColumnSegment::FilterSelection(
        sel, null_row, states.State(*g.client_context, *spec.filter), 1,
        approved);
      if (approved == 0) {
        out.segment_dead = true;
        out.active.clear();
        return;
      }
      continue;
    }
    const auto& stats = reader->MergedStatistics();
    const auto verdict =
      spec.filter->Cast<duckdb::ExpressionFilter>().CheckStatistics(
        stats, states.State(*g.client_context, *spec.filter));
    switch (verdict) {
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE:
      case duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL:
        out.segment_dead = true;
        out.active.clear();
        return;
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE:
        if (spec.zonemap_only) {
          out.active.push_back(spec);
        }
        break;
      case duckdb::FilterPropagateResult::FILTER_TRUE_OR_NULL:
        if (!spec.zonemap_only && spec.not_null != nullptr) {
          // The replacement is a bare IS NOT NULL, so it evaluates on the
          // validity child alone.
          spec.filter = spec.not_null;
          spec.null_check = irs::NullCheckKind::IsNotNull;
        }
        out.active.push_back(spec);
        break;
      case duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE:
        out.active.push_back(spec);
        break;
    }
  }
}

namespace {

void CollectSegmentTopK(TopKScanLocalState& s, const irs::SubReader& seg,
                        uint32_t seg_idx, IResearchScanGlobalState& g) {
  ClassifySegmentColFilters(seg, g, s.filter_states, s.seg_cls);
  const auto& cls = s.seg_cls;
  if (cls.segment_dead) {
    return;
  }
  using C = irs::NthPartitionScoreCollector;
  const auto& search = *g.scan;
  if (!std::holds_alternative<C>(s.collector)) {
    const size_t k =
      g.topk.rerank_pool ? g.topk.rerank_pool : *search.score_top_k;
    s.collector.template emplace<C>(s.local_threshold, k, s.hit_slice);
  }
  auto& collector = std::get<C>(s.collector);

  s.score_fetcher.Clear();
  collector.SetSegment(seg_idx);

  const auto seen_global =
    g.topk.global_kth_score.load(std::memory_order_relaxed);
  if (seen_global > s.local_threshold) {
    s.local_threshold = seen_global;
  }

  const auto& seg_query = EnsureSegmentQuery(g, s, seg, seg_idx);
  const irs::StatsBuffer& stats =
    g.stats ? *g.stats : irs::StatsBuffer::Empty();

  const bool wand_enabled =
    WandEnabled(search.inverted_index.get(), search.text_scorer);
  irs::DocIterator::ptr it = seg.mask(seg_query.Execute(
    {.wand = {.wand_enabled = wand_enabled},
     .top_k_collect = search.vector_scorer.has_value() && cls.active.empty()},
    stats));
  // Filter the collected docs by the covered `.col` values, so top-k is
  // selected over survivors (codec Filter + zonemap in the wrapper).
  it = MaybeWrapColFilter(std::move(it), seg, cls.active, g, s.filter_states);
  auto score_func = it->PrepareScore({
    .scorer = g.scorer_obj.get(),
    .segment = &seg,
    .fetcher = &s.score_fetcher,
  });
  if (auto* it_threshold = irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
    collector.SetScoreThreshold(it_threshold->value);
  }
  it->Collect(score_func, s.score_fetcher, collector);
  collector.SetScoreThreshold(s.local_threshold);

  const irs::score_t kth = s.local_threshold;
  auto cur = g.topk.global_kth_score.load(std::memory_order_relaxed);
  while (kth > cur && !g.topk.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

}  // namespace

void RunTopKScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                 TopKScanLocalState& l, duckdb::DataChunk& output) {
  while (!l.segments_exhausted) {
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      l.segments_exhausted = true;
      break;
    }
    const auto seg = g.SegmentAt(claimed);
    const auto& sub = (*g.reader)[seg];
    SDB_ASSERT(sub.live_docs_count() != 0);
    CollectSegmentTopK(l, sub, seg, g);
  }
  if (!l.emit_prepared) {
    l.PrepareEmitBuffer(g);
  }
  if (!EmitBufferedScoreDocs(ctx, g, l, output)) {
    output.SetChildCardinality(0);
  }
}

void TopKScanLocalState::PrepareEmitBuffer(IResearchScanGlobalState& g) {
  emit_prepared = true;
  if (std::holds_alternative<std::monostate>(collector)) {
    return;  // no segments claimed by this thread
  }

  const size_t accepted = std::visit(
    [](auto& c) -> size_t {
      if constexpr (std::is_same_v<std::decay_t<decltype(c)>, std::monostate>) {
        return 0;
      } else {
        return c.AcceptedCount();
      }
    },
    collector);
  auto accepted_slice = hit_slice.subspan(0, accepted);
  size_t kept = accepted;
  if (g.topk.rerank_pool > 0 && g.vector_scorer != nullptr) {
    SortScoreDocsBySegDoc(accepted_slice);
    // Rerank exact distances only when the collector's scores are approximate
    // (quantized); a non-quantized pool (over-fetched only to survive a lookup
    // filter) already carries exact distances.
    if (g.vector_scorer->quant != irs::VectorQuantization::None) {
      RerankHits(g, accepted_slice);
    }
    const size_t kreal = *g.scan->score_top_k;
    // Trim the over-fetched pool to the exact k only when nothing downstream
    // drops rows. With a lookup filter, keep the whole pool so the lookup can
    // discard non-matches and still yield up to k survivors (the TOP_N above
    // trims to the exact k).
    if (!g.has_lookup_filter && kept > kreal) {
      std::nth_element(accepted_slice.begin(), accepted_slice.begin() + kreal,
                       accepted_slice.end(),
                       [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
                         return l.score > r.score;
                       });
      kept = kreal;
      accepted_slice = hit_slice.subspan(0, kept);
    }
  }
  SortScoreDocsBySegDoc(accepted_slice);
  top_hits = hit_slice.subspan(0, kept);
}

void StreamScanLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                        const irs::SubReader& seg,
                                        uint32_t seg_idx,
                                        IResearchScanGlobalState& g) {
  const auto& seg_query = EnsureSegmentQuery(g, *this, seg, seg_idx);
  // The `.col`/score filters run inside the HitBatcher (RowGroup::Scan-style
  // filter+materialize), so the DocIterator streams unfiltered -- no
  // TableFilterDocIterator on this path. When a dynamic TOP_N score boundary is
  // pushed on a WAND-enabled text scorer, run WAND so below-threshold blocks
  // are skipped (PushHits seeds the threshold from the boundary before each
  // emit).
  streaming_doc =
    seg.mask(seg_query.Execute({.wand = {.wand_enabled = g.wand_streaming}},
                               g.stats ? *g.stats : irs::StatsBuffer::Empty()));
  if (g.needs_lookup && !PkColumnFor(*g.reader, seg_idx).second) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("inverted-index segment has no stored PK column but the query "
              "needs to map hits back to source rows"));
  }
  if (g.ScanScore()) {
    score_fetcher.Clear();
    streaming_score_function = streaming_doc->PrepareScore({
      .scorer = g.scorer_obj.get(),
      .segment = &seg,
      .fetcher = &score_fetcher,
    });
  }
  EnsureHitBatcher(g);
  SDB_ASSERT(classified_seg == seg_idx,
             "segment filters are classified at the claim site");
  hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context,
                            &filter_states, seg_cls.active);
}

void ColScanLocalState::StartUnit(
  duckdb::ClientContext& ctx, const IResearchScanGlobalState::ScanUnit& unit,
  IResearchScanGlobalState& g) {
  if (unit.bulk) {
    current_seg_idx = unit.seg;
    bulk_doc_in_seg = unit.begin;
    bulk_seg_doc_count = unit.begin + unit.count;
    streaming_doc.reset();
    // A scan order can hand out a segment's bulk units out of row order, so a
    // unit may start behind the reused per-segment scanner, whose column
    // cursors only move forward (GatherFilter/Skip never rewind). Only then
    // drop the scanner so OpenScanner rebuilds it fresh at `unit.begin`;
    // ascending units keep reusing it.
    if (current_seg_idx < full_scanners.size()) {
      auto& scanner = full_scanners[current_seg_idx];
      if (scanner && bulk_doc_in_seg < scanner->ScannedEnd()) {
        scanner.reset();
      }
    }
    return;
  }
  bulk_doc_in_seg = 0;
  bulk_seg_doc_count = 0;
  StartSegment(ctx, (*g.reader)[unit.seg], unit.seg, g);
}

FullScanner* ColScanLocalState::OpenScanner(const IResearchScanGlobalState& g) {
  const auto& reader = *g.reader;
  if (full_scanners.size() < reader.size()) {
    full_scanners.resize(reader.size());
  }
  auto& slot = full_scanners[current_seg_idx];
  if (!slot) {
    const auto* col_reader = reader[current_seg_idx].GetColReader();
    if (!col_reader) {
      return nullptr;
    }
    SDB_ASSERT(classified_seg == current_seg_idx,
               "segment filters are classified at the claim site");
    slot = std::make_unique<FullScanner>(*col_reader, g.cs_projections,
                                         seg_cls.active, g.client_context,
                                         filter_states);
  }
  return slot.get();
}

void StreamScanLocalState::PushHits(IResearchScanGlobalState& g) {
  if (!streaming_doc) {
    if (!hit_batcher->Empty()) {
      hit_batcher->Finalize();
    }
    return;
  }
  for (;;) {
    auto next = streaming_doc->value();
    // Before the first window value() is unpositioned; the self-positioning
    // Emit skips to the first match itself (no advance() prime). Afterwards
    // value() is the postcondition next match. `max = cursor + span` caps each
    // window to the row-group boundary, so a cursor that starts ahead of the
    // first match just yields an empty window and value() jumps to the match.
    auto cursor = irs::doc_limits::valid(next) ? next : irs::doc_limits::min();
    if (!irs::doc_limits::eof(next) && !hit_batcher->Filters().Empty()) {
      // Zonemap skip: raise the emit floor past definitely-dead blocks (the
      // self-positioning Emit skips to it) instead of staging and dropping
      // their windows; when everything left is dead, the segment is done.
      const auto rows = hit_batcher->SegmentRowCount();
      auto row = static_cast<uint64_t>(cursor - irs::doc_limits::min());
      for (auto dead = hit_batcher->Filters().DeadUntil(row);
           dead != 0 && row < rows;
           dead = hit_batcher->Filters().DeadUntil(row)) {
        row = dead;
      }
      if (row >= rows) {
        next = irs::doc_limits::eof();
      } else {
        cursor = irs::doc_limits::min() + static_cast<irs::doc_id_t>(row);
      }
    }
    if (irs::doc_limits::eof(next)) {
      streaming_score_function = {};
      streaming_doc.reset();
      if (!hit_batcher->Ready() && !hit_batcher->Empty()) {
        hit_batcher->Finalize();
      }
      return;
    }
    const auto span = hit_batcher->OpenWindow(cursor - irs::doc_limits::min());
    if (span == 0) {
      return;
    }
    if (g.ScanScore()) {
      if (g.wand_streaming) {
        // Seed the WAND threshold from the static score floor and the dynamic
        // TOP_N boundary's current value; blocks that cannot beat it are
        // skipped this window.
        if (auto* t =
              irs::GetMutable<irs::ScoreThresholdAttr>(streaming_doc.get())) {
          auto v = g.score_static_floor;
          if (g.score_dynamic_filter) {
            v = std::max(v, CurrentWandThreshold(*g.score_dynamic_filter));
          }
          t->value = v;
        }
      }
      const auto n = streaming_doc->EmitScoredDocs(
        hit_batcher->WindowHead(), hit_batcher->ScoreHead(), cursor + span,
        streaming_score_function, &score_fetcher, cursor);
      // User-facing scores in the batcher: the score-column filter (applied on
      // _scores in EmitFiltered) and the output vector both see the one value.
      ApplyScoreEmit(g, hit_batcher->ScoreHead(), n);
      hit_batcher->CommitWindow(n);
    } else {
      const auto n = streaming_doc->EmitDocs(hit_batcher->WindowHead(), cursor,
                                             cursor + span);
      hit_batcher->CommitWindow(n);
    }
  }
}

duckdb::idx_t ColScanLocalState::EmitChunk(duckdb::ClientContext& ctx,
                                           IResearchScanGlobalState& g,
                                           duckdb::DataChunk& output) {
  // Bulk columnstore chunks: a chunk whose rows are all dropped by the `.col`
  // filters produces 0 survivors while the unit still has rows to scan -- keep
  // scanning within the unit rather than returning 0 (which RunColScan reads
  // as "unit drained" and would skip the rest of the unit).
  while (bulk_doc_in_seg < bulk_seg_doc_count) {
    auto* scanner = OpenScanner(g);
    SDB_ENSURE(scanner, "bulk cs scan: segment has no columnstore reader");
    // Zonemap skip: jump the cursor past a definitely-dead block in one step
    // instead of scanning it vector by vector.
    const auto dead_end = scanner->DeadUntil(bulk_doc_in_seg);
    if (dead_end > bulk_doc_in_seg) {
      bulk_doc_in_seg = std::min<uint64_t>(dead_end, bulk_seg_doc_count);
      continue;
    }
    const auto take = std::min<duckdb::idx_t>(
      STANDARD_VECTOR_SIZE, bulk_seg_doc_count - bulk_doc_in_seg);
    const auto produced = scanner->Scan(bulk_doc_in_seg, take, output);
    bulk_doc_in_seg += take;
    if (produced != 0) {
      AccountAndWriteVirtualColumns(g, produced, nullptr, output);
      return produced;
    }
    output.Reset();
  }
  // A non-bulk unit (segment with deletes) runs the masked streaming walk.
  return StreamScanLocalState::EmitChunk(ctx, g, output);
}

duckdb::idx_t StreamScanLocalState::EmitChunk(duckdb::ClientContext& ctx,
                                              IResearchScanGlobalState& g,
                                              duckdb::DataChunk& output) {
  // A window entirely dropped by the filters must not read as "segment
  // drained" -- pull the next ready batch until one has survivors or the
  // batcher is genuinely empty.
  for (;;) {
    if (!hit_batcher || (!streaming_doc && hit_batcher->Empty())) {
      return 0;
    }
    if (!hit_batcher->Ready()) {
      PushHits(g);
      if (!hit_batcher->Ready()) {
        return 0;
      }
    }
    SDB_IF_FAILURE("SearchLookupFault") {
      if (g.needs_lookup) {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    const auto produced = EmitReadyBatch(ctx, g, *this, output);
    if (produced != 0) {
      return produced;
    }
    output.Reset();
  }
}

void RunCountScan(IResearchScanGlobalState& g, CountScanLocalState& l,
                  duckdb::DataChunk& output) {
  while (!l.segments_exhausted) {
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      l.segments_exhausted = true;
      break;
    }
    const auto seg = g.SegmentAt(claimed);
    const auto& sub = (*g.reader)[seg];
    SDB_ASSERT(sub.live_docs_count() != 0);
    ClassifySegmentColFilters(sub, g, l.filter_states, l.seg_cls);
    if (l.seg_cls.segment_dead) {
      continue;
    }
    if (l.seg_cls.active.empty() && !g.scan->stored_filter &&
        !g.vector_scorer) {
      // No predicate and the whole-file statistics settled every pushed
      // filter for this segment: the live count answers without touching the
      // postings.
      l.local_count += sub.live_docs_count();
      continue;
    }
    const auto& seg_query = EnsureSegmentQuery(g, l, sub, seg);
    auto doc = MaybeWrapColFilter(
      sub.mask(seg_query.Execute({}, irs::StatsBuffer::Empty())), sub,
      l.seg_cls.active, g, l.filter_states);
    l.local_count += doc->count();
  }
  if (l.local_emitted >= l.local_count) {
    output.SetChildCardinality(0);
    return;
  }
  const auto batch = std::min<duckdb::idx_t>(l.local_count - l.local_emitted,
                                             STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  l.local_emitted += batch;
}

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             IResearchScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output) {
  if (!g.cs_projections.empty()) {
    SDB_IF_FAILURE("SearchIncludeFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  if (g.needs_lookup) {
    if (!l.index_source) {
      l.index_source =
        MakeIndexSource(ctx, *g.scan, g.lookup_projected_columns,
                        g.projected_types, g.scan->column_ids,
                        const_cast<duckdb::TableFilterSet*>(g.pushed_filters));
    }
    if (l.pk_batch.kind == PrimaryKeyBatch::Kind::None) {
      l.pk_batch.kind = l.index_source->PkKind();
    }
    l.pk_batch.Reset();
  }
  const auto batch = l.hit_batcher->Emit(output);
  if (batch.pk != nullptr) {
    SDB_IF_FAILURE("SearchPkFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    batch.pk->Flatten(batch.count);
    AppendPrimaryKeysFromVector(l.pk_batch, *batch.pk, batch.count);
  }
  const HitsChunk view{
    .docs = batch.docs,
    .scores = batch.scores,
    .fixed_seg = batch.seg,
  };
  WriteChunkOffsets(l.offsets_entries, l.offsets_prepped_seg,
                    l.offsets_doc_scratch, g, view, output);
  AccountAndWriteVirtualColumns(g, batch.count, batch.score_vec, output);
  return batch.count;
}

duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx,
                            IResearchScanGlobalState& g,
                            SegDocBufferedScanLocalState& l,
                            duckdb::DataChunk& output,
                            duckdb::idx_t collected) {
  if (collected == 0 || !g.needs_lookup) {
    return collected;
  }
  SDB_ASSERT(l.index_source);
  SDB_ASSERT(l.pk_batch.Size() == collected);
  // Returns the survivor count: the lookup applies pushed lookup-column filters
  // natively and compacts to survivors (== collected when no lookup filter).
  return l.index_source->Materialize(ctx, l.pk_batch, 0, collected, output);
}

bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx,
                           IResearchScanGlobalState& g, TopKScanLocalState& l,
                           duckdb::DataChunk& output) {
  const std::span<const irs::ScoreDoc> hits = l.top_hits;
  size_t& current_idx = l.emit_idx;
  if (!l.hit_batcher) {
    l.EnsureHitBatcher(g);
    l.hit_batcher->BeginSegment(std::numeric_limits<uint32_t>::max(), nullptr,
                                g.client_context);
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.needs_lookup) {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  auto& batcher = *l.hit_batcher;
  // Emit one materialized batch at a time; a batch fully dropped by a lookup
  // filter loops for the next one instead of surfacing an empty chunk.
  for (;;) {
    while (!batcher.Ready()) {
      if (current_idx == hits.size()) {
        if (batcher.Empty()) {
          return false;
        }
        batcher.Finalize();
        if (!batcher.Ready()) {
          return false;
        }
        break;
      }
      const auto& hit = hits[current_idx];
      // Hits are sorted by (segment, doc); a segment change drains the batch.
      if (hit.segment_idx != batcher.Segment()) {
        if (!batcher.Empty()) {
          batcher.Finalize();
          continue;
        }
        batcher.BeginSegment(hit.segment_idx,
                             (*g.reader)[hit.segment_idx].GetColReader(),
                             g.client_context);
      }
      // Bulk-stage the ascending run of same-segment hits that fall in one
      // columnstore window (OpenWindow bounds it to a row group / output
      // vector) and commit it in one shot -- the window API the streaming path
      // uses. A full batcher yields span 0, so we emit the ready batch and
      // retry this hit after the drain.
      const auto row = hit.doc - irs::doc_limits::min();
      const auto span = batcher.OpenWindow(row);
      if (span == 0) {
        break;
      }
      auto* out_docs = batcher.WindowHead();
      auto* out_scores = g.ScanScore() ? batcher.ScoreHead() : nullptr;
      const auto seg = batcher.Segment();
      duckdb::idx_t n = 0;
      while (current_idx != hits.size() &&
             hits[current_idx].segment_idx == seg &&
             (hits[current_idx].doc - irs::doc_limits::min()) < row + span) {
        out_docs[n] = hits[current_idx].doc;
        if (out_scores != nullptr) {
          out_scores[n] = hits[current_idx].score;
        }
        ++n;
        ++current_idx;
      }
      // Map the collector's raw scores to the user-facing value in the batcher,
      // symmetric with the streaming path (AccountAndWriteVirtualColumns copies
      // them straight out).
      if (out_scores != nullptr) {
        ApplyScoreEmit(g, out_scores, n);
      }
      batcher.CommitWindow(n);
    }
    const auto emitted = EmitReadyBatch(ctx, g, l, output);
    const auto kept = FinalizeBatch(ctx, g, l, output, emitted);
    if (kept != 0) {
      output.SetChildCardinality(kept);
      return true;
    }
    output.Reset();
  }
}

void RunStreamingScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                      StreamScanLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    const auto added = l.EmitChunk(ctx, g, output);
    SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
    if (added != 0) {
      // The lookup fetches the source columns for the materialized pks and
      // applies any pushed lookup-column filters natively (FilterSelection +
      // late materialization), returning the survivor count (== added when no
      // lookup filter applies).
      const auto kept = FinalizeBatch(ctx, g, l, output, added);
      if (kept != 0) {
        output.SetChildCardinality(kept);
        return;
      }
      output.Reset();
      continue;
    }
    const auto claimed = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (claimed >= g.claimable_segments) {
      break;
    }
    const auto seg_idx = g.SegmentAt(claimed);
    const auto& seg = (*g.reader)[seg_idx];
    SDB_ASSERT(seg.live_docs_count() != 0);
    ClassifySegmentColFilters(seg, g, l.filter_states, l.seg_cls);
    l.classified_seg = seg_idx;
    if (l.seg_cls.segment_dead) {
      continue;
    }
    l.StartSegment(ctx, seg, seg_idx, g);
  }
  output.SetChildCardinality(0);
}

void RunColScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                ColScanLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    const auto added = l.EmitChunk(ctx, g, output);
    SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
    if (added != 0) {
      output.SetChildCardinality(added);
      return;
    }
    const auto ui =
      g.col_scan.next_unit.fetch_add(1, std::memory_order_relaxed);
    if (ui >= g.col_scan.units.size()) {
      break;
    }
    const auto& unit = g.col_scan.units[ui];
    // Units subdivide a segment; the whole-file classification is computed
    // once per segment and reused across its units.
    if (l.classified_seg != unit.seg) {
      ClassifySegmentColFilters((*g.reader)[unit.seg], g, l.filter_states,
                                l.seg_cls);
      l.classified_seg = unit.seg;
    }
    if (l.seg_cls.segment_dead) {
      continue;
    }
    l.StartUnit(ctx, unit, g);
  }
  output.SetChildCardinality(0);
}

void RunTsDictScan(duckdb::ClientContext& ctx, IResearchScanGlobalState& g,
                   TsDictLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    duckdb::idx_t collected = 0;
    bool exhausted = false;
    while (collected < STANDARD_VECTOR_SIZE) {
      const auto added = l.EmitChunk(ctx, g, output, collected);
      SDB_ASSERT(collected + added <= STANDARD_VECTOR_SIZE);
      collected += added;
      if (added != 0) {
        continue;
      }
      const auto seg_idx =
        g.next_segment.fetch_add(1, std::memory_order_relaxed);
      if (seg_idx >= g.total_segments) {
        exhausted = true;
        break;
      }
      const auto& seg = (*g.reader)[seg_idx];
      SDB_ASSERT(seg.live_docs_count() != 0);
      l.StartSegment(ctx, seg, seg_idx, g);
    }
    if (collected != 0 || exhausted) {
      output.SetChildCardinality(collected);
      return;
    }
    output.Reset();
  }
}

void TsDictLocalState::StartSegment(duckdb::ClientContext& /*ctx*/,
                                    const irs::SubReader& seg, uint32_t seg_idx,
                                    IResearchScanGlobalState& g) {
  _seg = &seg;
  ClassifySegmentColFilters(seg, g, filter_states, seg_cls);
  _cf = {seg_cls.active, &g, &filter_states};
  if (seg_cls.segment_dead) {
    _next_field = nullptr;
    return;
  }
  count_mode = seg.live_docs_count() != seg.docs_count() ? CountMode::Masked
                                                         : CountMode::Meta;
  where_query = nullptr;
  _next_field = fields.empty() ? nullptr : fields.data();
  if (g.filter) {
    const auto& query = EnsureSegmentQuery(g, *this, seg, seg_idx);
    auto probe =
      MaybeWrapColFilter(query.Execute({}, irs::StatsBuffer::Empty()), seg,
                         seg_cls.active, g, filter_states);
    if (irs::doc_limits::eof(probe->advance())) {
      _next_field = nullptr;
      return;
    }
    where_query = &query;
    count_mode = CountMode::Where;
  }
  if (!seg_cls.active.empty() && count_mode == CountMode::Meta) {
    count_mode = CountMode::Masked;
  }
}

irs::TermIterator::ptr TsDictLocalState::MakeTermSource(
  const FieldState& field, const irs::TermReader& reader) {
  if (field.having_filter) {
    auto cursor = field.having_filter->CompileTermIterator(reader);
    SDB_ENSURE(cursor,
               "ts_dict: claimed having filter failed to compile a term "
               "iterator");
    return cursor;
  }
  const bool max_only = field.term_uses == TsDictTermUses::kMax;
  const bool min_max =
    field.term_uses == (TsDictTermUses::kMin | TsDictTermUses::kMax);
  if ((max_only || min_max) && count_mode != CountMode::Masked) {
    std::array<irs::bytes_view, 2> terms;
    size_t count = 0;
    const auto max = reader.max();
    if (count_mode == CountMode::Meta) {
      if (min_max) {
        terms[count++] = reader.min();
      }
      terms[count++] = max;
    } else {
      auto it = reader.iterator(irs::SeekMode::RandomOnly);
      const auto pin = [&](irs::bytes_view term) {
        if (!it->seek(term) ||
            WhereLiveDocs(*it, *_seg, *where_query, false, _cf) == 0) {
          return false;
        }
        terms[count++] = term;
        return true;
      };
      if (!(min_max ? pin(reader.min()) && pin(max) : pin(max))) {
        count = 0;
      }
    }
    if (count != 0) {
      _cursor_mode = CountMode::Meta;
      return irs::memory::make_managed<MinMaxTermsIterator>(terms, count);
    }
  }
  return reader.iterator(irs::SeekMode::NORMAL);
}

bool TsDictLocalState::NextField() {
  while (_next_field) {
    const auto& field = *_next_field++;
    if (_next_field == fields.data() + fields.size()) {
      _next_field = nullptr;
    }
    _field = &field;
    _null_pending = irs::field_limits::valid(field.null_field_id);
    _cursor_mode = count_mode;
    _cursor.reset();
    if (const auto* reader = _seg->field(field.field_id);
        reader && reader->size() != 0) {
      _cursor = MakeTermSource(field, *reader);
    }
    if (_cursor || _null_pending) {
      return true;
    }
  }
  return false;
}

namespace {

struct TsDictEmitContext {
  duckdb::string_t* term_data;
  duckdb::string_t* raw_data;
  int32_t* count_data;
  int64_t* freq_data;
  float* score_data;
  duckdb::Vector* term_vec;
  duckdb::Vector* raw_vec;
  const irs::TermMeta* meta;
  const irs::TermBoost* boost;
  const irs::SubReader* seg;
  TsDictLocalState::CountMode count_mode;
  const irs::QueryBuilder* where_query;
  ColFilterCtx cf;
  duckdb::idx_t row;
  duckdb::idx_t end_row;
};

struct TsDictEmitter {
  explicit TsDictEmitter(TsDictEmitContext ctx) noexcept : ctx{ctx} {}

  void Emit(irs::bytes_view term, uint32_t docs) {
    const auto row = ctx.row;
    const auto* p = reinterpret_cast<const char*>(term.data());
    if (ctx.term_data) {
      ctx.term_data[row] =
        duckdb::StringVector::AddString(*ctx.term_vec, p, term.size());
    }
    if (ctx.raw_data) {
      ctx.raw_data[row] =
        duckdb::StringVector::AddStringOrBlob(*ctx.raw_vec, p, term.size());
    }
    if (ctx.count_data) {
      ctx.count_data[row] = static_cast<int32_t>(docs);
    }
    if (ctx.freq_data) {
      ctx.freq_data[row] = static_cast<int64_t>(ctx.meta->freq);
    }
    if (ctx.score_data) {
      ctx.score_data[row] = ctx.boost ? ctx.boost->value : irs::kNoBoost;
    }
    ++ctx.row;
  }

  uint32_t LiveDocs(irs::TermIterator& it) const {
    using Mode = TsDictLocalState::CountMode;
    switch (ctx.count_mode) {
      case Mode::Meta:
        return ctx.meta ? ctx.meta->docs_count : 1;
      case Mode::Masked:
        return MaskedLiveDocs(it, *ctx.seg, ctx.count_data, ctx.cf);
      case Mode::Where:
        return WhereLiveDocs(it, *ctx.seg, *ctx.where_query, ctx.count_data,
                             ctx.cf);
    }
    return 0;
  }

  void OnTerm(irs::TermIterator& it) {
    if (ctx.meta) {
      it.read();
    }
    const auto live_docs = LiveDocs(it);
    if (live_docs != 0) {
      Emit(it.value(), live_docs);
    }
  }

  TsDictEmitContext ctx;
};

}  // namespace

duckdb::idx_t TsDictLocalState::EmitField(duckdb::DataChunk& output,
                                          duckdb::idx_t output_start,
                                          duckdb::idx_t capacity) {
  using duckdb::DConstants;

  if (!_cursor && !_null_pending) {
    return 0;
  }
  const auto& field = *_field;
  const auto vec = [&](duckdb::idx_t slot) -> duckdb::Vector* {
    return slot == DConstants::INVALID_INDEX ? nullptr : &output.data[slot];
  };
  const auto data = [&]<typename T>(duckdb::idx_t slot) -> T* {
    auto* v = vec(slot);
    return v ? duckdb::FlatVector::GetDataMutable<T>(*v) : nullptr;
  };

  auto* term_vec = vec(field.term_slot);
  auto* raw_vec = vec(field.term_raw_slot);
  auto* term_data = data.operator()<duckdb::string_t>(field.term_slot);
  auto* raw_data = data.operator()<duckdb::string_t>(field.term_raw_slot);
  auto* count_data = data.operator()<int32_t>(field.count_slot);
  auto* freq_data = data.operator()<int64_t>(field.freq_slot);
  auto* score_data = data.operator()<float>(field.score_slot);

  const bool min_only = field.term_uses == TsDictTermUses::kMin;
  const auto field_capacity = min_only ? duckdb::idx_t{1} : capacity;

  duckdb::idx_t n = 0;
  if (_cursor && field_capacity != 0) {
    const auto* meta = [&]() -> const irs::TermMeta* {
      if (!count_data && !freq_data) {
        return nullptr;
      }
      const auto* meta = irs::get<irs::TermMeta>(*_cursor);
      SDB_ENSURE(meta, "ts_dict: term iterator has no term_meta");
      return meta;
    }();
    TsDictEmitter emitter{TsDictEmitContext{
      .term_data = term_data,
      .raw_data = raw_data,
      .count_data = count_data,
      .freq_data = freq_data,
      .score_data = score_data,
      .term_vec = term_vec,
      .raw_vec = raw_vec,
      .meta = meta,
      .boost = score_data ? irs::get<irs::TermBoost>(*_cursor) : nullptr,
      .seg = _seg,
      .count_mode = _cursor_mode,
      .where_query = where_query,
      .cf = _cf,
      .row = output_start,
      .end_row = output_start + field_capacity}};
    while (emitter.ctx.row < emitter.ctx.end_row) {
      if (!_cursor->next()) {
        _cursor.reset();
        break;
      }
      emitter.OnTerm(*_cursor);
    }
    n = emitter.ctx.row - output_start;
  }

  if (min_only && n != 0) {
    _cursor.reset();
  }
  if (_null_pending && n < field_capacity && !_cursor) {
    _null_pending = false;
    n += AppendNullRow(output, field, output_start + n);
  }
  if (n == 0) {
    return 0;
  }

  // The other fields' columns are NULL on rows produced for this field.
  for (const auto& other : fields) {
    if (&other == _field) {
      continue;
    }
    for (const auto slot :
         {other.term_slot, other.term_raw_slot, other.count_slot,
          other.freq_slot, other.score_slot}) {
      if (slot == DConstants::INVALID_INDEX) {
        continue;
      }
      auto& validity = duckdb::FlatVector::ValidityMutable(output.data[slot]);
      for (duckdb::idx_t i = 0; i < n; ++i) {
        validity.SetInvalid(output_start + i);
      }
    }
  }
  return n;
}

duckdb::idx_t TsDictLocalState::AppendNullRow(duckdb::DataChunk& output,
                                              const FieldState& field,
                                              duckdb::idx_t row) {
  const auto nulls = NullFieldLiveCount(*_seg, field.null_field_id, count_mode,
                                        where_query, _cf);
  if (nulls == 0) {
    return 0;
  }
  const auto set_null = [&](duckdb::idx_t slot) {
    if (slot != duckdb::DConstants::INVALID_INDEX) {
      duckdb::FlatVector::SetNull(output.data[slot], row, true);
    }
  };
  set_null(field.term_slot);
  set_null(field.term_raw_slot);
  set_null(field.freq_slot);
  set_null(field.score_slot);
  if (field.count_slot != duckdb::DConstants::INVALID_INDEX) {
    duckdb::FlatVector::GetDataMutable<int32_t>(
      output.data[field.count_slot])[row] = static_cast<int32_t>(nulls);
  }
  return 1;
}

duckdb::idx_t TsDictLocalState::EmitChunk(duckdb::ClientContext& /*ctx*/,
                                          IResearchScanGlobalState& g,
                                          duckdb::DataChunk& output,
                                          duckdb::idx_t output_start) {
  const auto capacity = STANDARD_VECTOR_SIZE - output_start;
  do {
    if (const auto n = EmitField(output, output_start, capacity); n != 0) {
      g.produced_rows.fetch_add(n, std::memory_order_relaxed);
      return n;
    }
  } while (NextField());
  return 0;
}

}  // namespace sdb::connector
