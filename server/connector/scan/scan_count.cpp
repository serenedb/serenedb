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

#include <algorithm>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/queries/posting_heads.hpp>
#include <optional>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

bool IsDefaultValue(const duckdb::Value& value) {
  if (value.IsNull()) {
    return false;
  }
  const auto& type = value.type();
  switch (type.id()) {
    case duckdb::LogicalTypeId::VARCHAR:
      return duckdb::StringValue::Get(value).empty();
    case duckdb::LogicalTypeId::BOOLEAN:
      return !duckdb::BooleanValue::Get(value);
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
      return value.GetValue<double>() == 0;
    default:
      return type.IsIntegral() && value == duckdb::Value::Numeric(type, 0);
  }
}

std::optional<uint64_t> CountFromDefaults(const ScanGlobalState& g,
                                          const CountLocalState& l) {
  if (g.ClaimedWhere() || g.vector_scorer || l.seg_cls.active.size() != 1) {
    return std::nullopt;
  }
  const auto& spec = l.seg_cls.active.front();
  if (spec.is_score || spec.is_dynamic || spec.zonemap_only ||
      spec.row_gather || spec.null_check != irs::NullCheckKind::None ||
      !spec.extract_path.empty()) {
    return std::nullopt;
  }
  const auto& sub = (*g.reader)[l.unit.seg];
  const auto* col_reader = sub.GetColReader();
  const auto* column = col_reader ? col_reader->Column(spec.field) : nullptr;
  if (sub.docs_mask() || !column || column->HasValidity()) {
    return std::nullopt;
  }
  const auto kind = DefaultComparison(*spec.filter, column->Type());
  if (!kind) {
    return std::nullopt;
  }
  const auto rows = g.RowsOf(l.unit);
  uint64_t count = 0;
  irs::BlockWindow w;
  for (auto row = rows.begin; row < rows.end; row = w.end) {
    w = column->Locate(row, w);
    const auto defaults = column->BlockDefaults(w.block);
    if (w.begin < rows.begin || w.end > rows.end ||
        defaults == irs::kUnknownDefaults) {
      return std::nullopt;
    }
    count += *kind == duckdb::ExpressionType::COMPARE_EQUAL
               ? defaults
               : w.end - w.begin - defaults;
  }
  return count;
}

}  // namespace

std::optional<duckdb::ExpressionType> DefaultComparison(
  const duckdb::TableFilter& filter, const duckdb::LogicalType& column_type) {
  if (filter.filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
    return std::nullopt;
  }
  const auto& expr =
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "DefaultComparison")
       .expr;
  const auto kind = expr.GetExpressionType();
  if (!duckdb::BoundComparisonExpression::IsComparison(expr) ||
      (kind != duckdb::ExpressionType::COMPARE_EQUAL &&
       kind != duckdb::ExpressionType::COMPARE_NOTEQUAL)) {
    return std::nullopt;
  }
  const auto& comparison = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& left = duckdb::BoundComparisonExpression::Left(comparison);
  const auto& right = duckdb::BoundComparisonExpression::Right(comparison);
  const auto* constant =
    left.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF
      ? &right
      : (right.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF
           ? &left
           : nullptr);
  if (!constant ||
      constant->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_CONSTANT ||
      constant->GetReturnType() != column_type ||
      !IsDefaultValue(
        constant->Cast<duckdb::BoundConstantExpression>().GetValue())) {
    return std::nullopt;
  }
  return kind;
}

namespace {

void FinishRoot(CountLocalState& l) {
  if (l.root) {
    l.local_count +=
      irs::utils::downCast<irs::count::Root>(l.root.get())->Finish();
    l.root.reset();
    l.root_seg = std::numeric_limits<uint32_t>::max();
  }
}

}  // namespace

void RunCountScan(duckdb::TableFunctionInput&, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output) {
  while (NextLiveUnit(g, l)) {
    const auto& unit = l.unit;
    const auto& sub = (*g.reader)[unit.seg];
    if (unit.whole && l.seg_cls.active.empty() && !g.ClaimedWhere() &&
        !g.vector_scorer) {
      l.local_count += sub.live_docs_count();
    } else if (const auto counted = CountFromDefaults(g, l)) {
      l.local_count += *counted;
    } else {
      const auto range = g.RangeOf(unit);
      if (l.root_seg != unit.seg || range.begin < l.root_end) {
        FinishRoot(l);
        const auto& seg_query = EnsureSegmentQuery(g, l, unit.seg);
        if (g.remote) {
          irs::PrefetchPostingHeads(seg_query, 2);
        }
        auto* table = BeginVerify(l.col_verify, sub, g, l);
        auto plan = irs::count::MakeRoot(
          seg_query,
          {.table = table, .span = g.UnitSpan(unit), .partial = true});
        EnsurePlanned(plan != nullptr);
        l.root = std::move(plan);
        l.root_seg = unit.seg;
      }
      l.local_count += irs::utils::downCast<irs::count::Root>(l.root.get())
                         ->Run(range.begin, range.end);
      l.root_end = range.end;
    }
    FinishUnit(g, l);
  }
  FinishRoot(l);
  if (l.local_emitted >= l.local_count) {
    output.SetChildCardinality(0);
    return;
  }
  const auto batch = std::min<duckdb::idx_t>(l.local_count - l.local_emitted,
                                             STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  l.local_emitted += batch;
}

}  // namespace sdb::connector
