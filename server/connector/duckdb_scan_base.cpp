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

#include "connector/duckdb_scan_base.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/projection_index.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/function/scalar/generic_common.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/store/directory.hpp>
#include <limits>
#include <numeric>
#include <ranges>
#include <string_view>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "iresearch/index/column_extract.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

void InitScanState(IResearchScanGlobalState& state,
                   duckdb::ClientContext& context,
                   const SereneDBScanBindData& bind_data,
                   duckdb::TableFunctionInitInput& input) {
  // Determine which columns DuckDB actually wants (projection pushdown).
  const auto num_bind_columns = bind_data.column_ids.size();
  for (auto col_id : input.column_ids) {
    if (col_id == kColumnIdentifierGeneratedPk) {
      if (!bind_data.IsSearchTableEntry()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("projecting the rowid through an inverted-index scan is not "
                  "supported"));
      }
      // Search table: the rowid IS the generated PK, materialized from `.col`.
      state.generated_pk_output_idx = state.projected_columns.size();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::ROW_TYPE);
    } else if (col_id == kColumnIdentifierTableOid) {
      state.scan_tableoid = true;
      state.tableoid_output_idx = state.projected_columns.size();
      state.tableoid_value = bind_data.RelationId().id();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BOOLEAN);
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
      if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
        state.scan_score = true;
        state.score_output_idx = state.projected_columns.size();
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexOffsetsId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(catalog::Column::MakeOffsetsType());
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::VARCHAR);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermRawId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::BLOB);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermCountId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::INTEGER);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermFreqId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::BIGINT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermScoreId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else {
        state.projected_columns.push_back(col_id);
        const auto& col_index =
          input.column_indexes[state.projected_columns.size() - 1];
        if (col_index.IsPushdownExtract() && col_index.HasChildren()) {
          state.projected_types.push_back(col_index.GetScanType());
        } else {
          state.projected_types.push_back(bind_data.column_types[col_id]);
        }
      }
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
    BuildTableFilter(state, context, bind_data, *input.filters);
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

// Classifies the pushed filters: score-column filters and covered `.col`
// filters are applied in-scan (`col_filters`); a filter on a lookup
// (source-only) column sets `has_lookup_filter` -- it is forwarded to the
// source's native lookup scan via `pushed_filters` and forces the streaming
// path (the lookup can't run per collector candidate).
void BuildTableFilter(IResearchScanGlobalState& state,
                      duckdb::ClientContext& /*context*/,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters) {
  const catalog::InvertedIndex* index_meta =
    bind_data.IsInvertedIndexEntry() ? bind_data.inverted_index.get() : nullptr;
  // Score-column filters, applied on the computed score vector (whatever
  // HandleScoreFilter left pushed: on top-k the collector-enforced conjuncts
  // were stripped; the floor was recorded on the bind data there). The
  // dynamic TOP_N boundary's shared runtime bound is captured for WAND
  // seeding -- it may sit alone or AND-combined with other score predicates.
  const auto push_score_filter = [&](const duckdb::TableFilter& filter) {
    state.col_filters.push_back(
      {.field = 0, .filter = &filter, .is_score = true});
    const auto& expr =
      *duckdb::ExpressionFilter::GetExpressionFilter(filter, "BuildTableFilter")
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
    if (state.scan_score && proj_idx == state.score_output_idx) {
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
  if (!gstate.vector_scorer) {
    // Text scores are already the user-facing value (Identity).
    return;
  }
  // The raw score is "larger = nearer" (ResolveScoringDistance negates distance
  // kernels); map it in place to the user value at the emit boundary, so the
  // score-column filter, the output vector (and any WAND threshold) all see the
  // one user-facing value.
  switch (gstate.vector_scorer->score_emit) {
    case ScoreEmit::Identity:
      break;
    case ScoreEmit::SqrtNeg:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = std::sqrt(-scores[i]);
      }
      break;
    case ScoreEmit::OneMinus:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = 1.0f - scores[i];
      }
      break;
    case ScoreEmit::Negate:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = -scores[i];
      }
      break;
  }
}

void AccountAndWriteVirtualColumns(IResearchScanGlobalState& gstate,
                                   duckdb::idx_t num_rows,
                                   duckdb::Vector* scores,
                                   duckdb::DataChunk& output) {
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  if (gstate.scan_tableoid) {
    auto* tableoid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.tableoid_output_idx]);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      tableoid_data[i] = gstate.tableoid_value;
    }
  }
  if (!gstate.scan_score) {
    return;
  }
  // Scores arrive already mapped to the user-facing value (ApplyScoreEmit runs
  // at the emit boundary) in the batcher's staged vector: reference it.
  SDB_ASSERT(scores != nullptr);
  output.data[gstate.score_output_idx].Reference(*scores);
}

}  // namespace sdb::connector
