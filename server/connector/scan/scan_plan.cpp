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

#include "connector/scan/scan_plan.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
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
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/column_id.h"
#include "connector/term_dict.h"
#include "connector/index_source_factory.h"
#include "connector/offsets_writer.hpp"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

std::optional<duckdb::LogicalType> VirtualIndexColumnType(
  ColumnId col_id) {
  if (col_id == kInvertedIndexScoreId ||
      col_id == kInvertedIndexTermScoreId) {
    return duckdb::LogicalType::FLOAT;
  }
  if (col_id == kInvertedIndexOffsetsId) {
    return MakeOffsetsType();
  }
  if (col_id == kInvertedIndexTermId) {
    return duckdb::LogicalType::VARCHAR;
  }
  if (col_id == kInvertedIndexTermRawId) {
    return duckdb::LogicalType::BLOB;
  }
  if (col_id == kInvertedIndexTermCountId) {
    return duckdb::LogicalType::INTEGER;
  }
  if (col_id == kInvertedIndexTermFreqId) {
    return duckdb::LogicalType::BIGINT;
  }
  return std::nullopt;
}

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

template<ScoreEmit E>
void EmitScoreFilterExec(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<float, float>(
    args.data[0], result, args.size(),
    [](float score) { return ApplyScoreEmit(E, score); });
}

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

void BuildTableFilter(ScanGlobalState& state, const ScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters) {
  const catalog::InvertedIndexConfig* index_meta =
    bind_data.relation.IsInvertedIndex() ? &bind_data.relation.ScannedIndex()
                                         : nullptr;
  const auto score_emit = bind_data.score.vector
                            ? bind_data.score.vector->score_emit
                            : ScoreEmit::Identity;
  const auto push_score_filter = [&](const duckdb::TableFilter& filter) {
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
    if (proj_idx == state.score_output_idx) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto bind_index = state.projected_columns[proj_idx];
    if (bind_index == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.columns.ids[bind_index];
    if (col_id == kInvertedIndexScoreId) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto* info =
      index_meta ? index_meta->FindColumnInfo(col_id) : nullptr;
    const bool index_stored = !index_meta ||
                              bind_data.relation.IsSearchTable() ||
                              (info && info->IsStored());
    if (!index_stored) {
      state.has_lookup_filter = true;
    } else {
      auto& cf = state.col_filters.emplace_back();
      cf.field = col_id;
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
      cf.type = bind_data.columns.types[bind_index];
      cf.not_null = MakeNotNullReplacement(entry.Filter(),
                                           bind_data.columns.types[bind_index]);
    }
  }
}

}  // namespace

const irs::Filter& MatchAllFilter() {
  static const irs::All kInstance;
  return kInstance;
}

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

void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out) {
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
    SDB_ASSERT(node->ChildIndexCount() == 1);
    node = &node->GetChildIndex(0);
  }
}

void InitScanState(ScanGlobalState& state, duckdb::ClientContext* context,
                   const ScanBindData& bind_data,
                   duckdb::TableFunctionInitInput& input) {
  const auto in_output = [&](duckdb::idx_t proj) {
    return input.projection_ids.empty() ||
           absl::c_find(input.projection_ids, proj) !=
             input.projection_ids.end();
  };
  const auto num_bind_columns = bind_data.columns.ids.size();
  for (auto col_id : input.column_ids) {
    const auto proj = state.projected_columns.size();
    if (col_id == kColumnIdentifierGeneratedPk) {
      auto pk_type = GeneratedPkTypeOf(bind_data);
      if (!pk_type) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("projecting the rowid through an inverted-index scan is not "
                  "supported"));
      }
      state.generated_pk_output_idx = proj;
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(std::move(*pk_type));
    } else if (col_id == kColumnIdentifierTableOid) {
      state.tableoid_output_idx = proj;
      state.tableoid_value = bind_data.RelationId();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id ==
                 duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX ||
               col_id == kColumnIdentifierPkRowNumber) {
      const bool file_index =
        col_id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
      const auto spec = ViewPkSpecOf(bind_data);
      if (!spec || !IsGlobPK(*spec)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("column \"", file_index ? "file_index" : "row_number",
                  "\" is only served by glob-backed inverted indexes"));
      }
      (file_index ? state.file_index_output_idx : state.row_number_output_idx) =
        proj;
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(file_index ? duckdb::LogicalType::UBIGINT
                                                 : duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BOOLEAN);
      continue;
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("projecting virtual column ", col_id,
                " through an inverted-index scan is not supported"));
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.columns.ids[col_id];
      if (const auto virtual_type = VirtualIndexColumnType(catalog_col_id)) {
        if (catalog_col_id == kInvertedIndexScoreId) {
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
          state.projected_types.push_back(bind_data.columns.types[col_id]);
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

  state.client_context = context;
  state.projected_column_indexes = input.column_indexes;
  SDB_ASSERT(state.projected_column_indexes.size() ==
             state.projected_columns.size());

  if (!input.projection_ids.empty()) {
    state.output_projection_ids = input.projection_ids;
  }

  state.score_static_floor = bind_data.score.static_floor;

  state.pushed_filters = input.filters.get();
  if (input.filters && input.filters->HasFilters()) {
    BuildTableFilter(state, bind_data, *input.filters);
  }
}

void ClassifyColumnstoreProjections(ScanGlobalState& state,
                                    const ScanBindData& bind_data) {
  if (state.generated_pk_output_idx != duckdb::DConstants::INVALID_INDEX) {
    state.cs_projections.emplace_back(
      irs::ColumnstoreProjection{.output_slot = state.generated_pk_output_idx,
                                 .column_id = term_dict::kPKFieldId});
  }
  if (state.row_number_output_idx != duckdb::DConstants::INVALID_INDEX) {
    state.cs_projections.emplace_back(irs::ColumnstoreProjection{
      .output_slot = state.row_number_output_idx,
      .column_id = term_dict::kPKFieldId,
      .extract_path = {"row_number"},
      .extract_scan_type = duckdb::LogicalType::BIGINT});
  }
  if (state.file_index_output_idx != duckdb::DConstants::INVALID_INDEX) {
    state.cs_projections.emplace_back(irs::ColumnstoreProjection{
      .output_slot = state.file_index_output_idx,
      .column_id = term_dict::kPKFieldId,
      .extract_path = {"file_index"},
      .extract_scan_type = duckdb::LogicalType::UBIGINT});
  }
  const auto in_output = [&](duckdb::idx_t proj) {
    return state.output_projection_ids.empty() ||
           absl::c_find(state.output_projection_ids, proj) !=
             state.output_projection_ids.end();
  };
  if (bind_data.relation.IsSearchTable()) {
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
      const auto col_id = bind_data.columns.ids[bind_col];
      irs::ColumnstoreProjection cp{.output_slot = proj,
                                    .column_id = col_id};
      if (proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.columns.types[bind_col],
                            path);
          if (!path.empty()) {
            cp.extract_path = std::move(path);
            cp.extract_scan_type = column_index.GetScanType();
          }
        }
      }
      state.cs_projections.emplace_back(std::move(cp));
    }
    return;
  }
  std::vector<std::string_view> path;
  for (duckdb::idx_t proj = 0; proj < state.projected_columns.size(); ++proj) {
    const auto bind_col = state.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.columns.ids[bind_col];
    const auto* info = bind_data.relation.ScannedIndex().FindColumnInfo(col_id);
    if (info && info->IsStored()) {
      state.lookup_projected_columns[proj] = duckdb::DConstants::INVALID_INDEX;
      if (!in_output(proj)) {
        continue;
      }
      irs::ColumnstoreProjection cp{.output_slot = proj,
                                    .column_id = col_id};
      if (info->store_values && proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.columns.types[bind_col],
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

ScanShape DecideShape(const ScanGlobalState& g, const ScanBindData& ss) {
  if (ss.ts_dict.Active()) {
    return ScanShape::TsDict;
  }
  const bool score_filter = absl::c_any_of(
    g.col_filters,
    [](const ScanGlobalState::ColFilter& f) { return f.is_score; });
  if (!g.has_output_column && !g.needs_lookup && !score_filter) {
    return ss.IsMatchAll() && g.col_filters.empty() ? ScanShape::CountFast
                                                    : ScanShape::Count;
  }
  if (ss.score.top_k && (ss.score.text || ss.score.order) &&
      (!g.has_lookup_filter || ss.score.vector)) {
    return ScanShape::TopK;
  }
  if (ss.IsMatchAll() && !ss.offsets.Active() && !ss.score.text &&
      !g.ScanScore() && !g.needs_lookup && g.has_real_column) {
    return ScanShape::ColScan;
  }
  return ScanShape::Stream;
}

void AccountAndWriteVirtualColumns(ScanGlobalState& g, duckdb::idx_t num_rows,
                                   duckdb::Vector* scores,
                                   duckdb::DataChunk& output) {
  g.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  if (g.tableoid_output_idx != duckdb::DConstants::INVALID_INDEX) {
    auto* tableoid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[g.tableoid_output_idx]);
    std::fill_n(tableoid_data, num_rows, g.tableoid_value);
  }
  if (!g.ScanScore()) {
    return;
  }
  SDB_ASSERT(scores != nullptr);
  auto& score_out = output.data[g.score_output_idx];
  const auto emit = ScoreEmitOf(g);
  if (emit == ScoreEmit::Identity) {
    score_out.Reference(*scores);
    return;
  }
  SDB_ASSERT(score_out.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  const auto* raw = duckdb::FlatVector::GetData<float>(*scores);
  auto* mapped = duckdb::FlatVector::GetDataMutable<float>(score_out);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    mapped[i] = ApplyScoreEmit(emit, raw[i]);
  }
}

void FetchLocalState::EnsureHitBatcher(const ScanGlobalState& g) {
  if (!hit_batcher) {
    hit_batcher = std::make_unique<irs::HitBatcher>(
      g.cs_projections,
      g.needs_lookup ? term_dict::kPKFieldId
                     : irs::field_limits::invalid(),
      g.ScanScore());
  }
}

void WriteChunkOffsets(FetchLocalState& f, const ScanGlobalState& g,
                       uint32_t seg, std::span<const irs::doc_id_t> docs,
                       duckdb::DataChunk& output) {
  if (f.offsets_entries.empty()) {
    return;
  }
  for (const auto& entry : f.offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  if (seg != f.offsets_prepped_seg) {
    for (auto& entry : f.offsets_entries) {
      entry.state.Clear();
    }
    OffsetsCollector visitor{f.offsets_entries};
    const auto& seg_query = g.queries[seg];
    SDB_ASSERT(seg_query);
    seg_query->Visit(visitor, irs::kNoBoost);
    f.offsets_prepped_seg = seg;
  }
  for (size_t i = 0; i < docs.size(); ++i) {
    for (auto& entry : f.offsets_entries) {
      FillRowOffsets(entry.state, docs[i], entry.limit, f.offsets_doc_scratch);
      WriteRowOffsets(output.data[entry.output_idx],
                      static_cast<duckdb::idx_t>(i), f.offsets_doc_scratch);
    }
  }
}

void BuildOffsetsEntries(FetchLocalState& f,
                         duckdb::TableFunctionInitInput& input,
                         const ScanBindData& bd) {
  if (bd.offsets.requests.empty()) {
    return;
  }
  std::vector<size_t> ss_idx_at_bind(bd.columns.ids.size(),
                                     std::numeric_limits<size_t>::max());
  size_t k = 0;
  for (size_t i = 0; i < bd.columns.ids.size(); ++i) {
    if (bd.columns.ids[i] == kInvertedIndexOffsetsId) {
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
    if (col_id >= bd.columns.ids.size()) {
      continue;
    }
    if (bd.columns.ids[col_id] == kInvertedIndexOffsetsId) {
      const auto ss_idx = ss_idx_at_bind[col_id];
      SDB_ASSERT(ss_idx < bd.offsets.requests.size());
      FieldEntry entry;
      entry.output_idx = out_slot;
      entry.limit = bd.offsets.requests[ss_idx].limit;
      entry.id = bd.offsets.requests[ss_idx].column_id;
      f.offsets_entries.push_back(std::move(entry));
    }
    ++out_slot;
  }
}

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                             FetchLocalState& f, duckdb::DataChunk& output) {
  SDB_IF_FAILURE("SearchIncludeFetchFault") {
    if (!g.cs_projections.empty()) {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  f.pk_column = nullptr;
  const auto batch = f.hit_batcher->Emit(output);
  if (batch.pk != nullptr) {
    SDB_IF_FAILURE("SearchPkFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    batch.pk->Flatten(batch.count);
    f.pk_column = batch.pk;
  }
  WriteChunkOffsets(f, g, batch.seg, batch.docs, output);
  AccountAndWriteVirtualColumns(g, batch.count, batch.score_vec, output);
  return batch.count;
}

duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                            FetchLocalState& f, duckdb::DataChunk& output,
                            duckdb::idx_t collected) {
  if (collected == 0 || !g.needs_lookup) {
    return collected;
  }
  if (!f.index_source) {
    f.index_source =
      MakeIndexSource(ctx, g.Bind(), g.lookup_projected_columns,
                      g.projected_types, g.Bind().columns.ids,
                      const_cast<duckdb::TableFilterSet*>(g.pushed_filters));
  }
  SDB_ASSERT(f.pk_column);
  const auto rows =
    f.index_source->Materialize(ctx, *f.pk_column, collected, output);
  g.metrics.rows_looked_up.fetch_add(collected, std::memory_order_relaxed);
  return rows;
}

}  // namespace sdb::connector
