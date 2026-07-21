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

#include "connector/optimizer/ts_dict_plan.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_unnest_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_cross_product.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_unnest.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_query_codec.h"
#include "connector/index_expression.hpp"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/search_filter_builder.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

enum class TsDictColKind { Term, TermRaw, Count, Freq, Score };

constexpr TsDictColKind kAllTsDictColKinds[]{
  TsDictColKind::Term, TsDictColKind::TermRaw, TsDictColKind::Count,
  TsDictColKind::Freq, TsDictColKind::Score};

using TsDictReq = connector::SereneDBScanBindData::TsDictRequest;

struct TsDictColInfo {
  catalog::Column::Id virtual_id;
  duckdb::LogicalTypeId type;
  duckdb::idx_t TsDictReq::* col_idx;
  std::string_view prefix;
};

constexpr TsDictColInfo TsDictColFor(TsDictColKind kind) {
  using C = catalog::Column;
  switch (kind) {
    case TsDictColKind::Term:
      return {C::kInvertedIndexTermId, duckdb::LogicalTypeId::VARCHAR,
              &TsDictReq::term_col_idx, C::kTermName};
    case TsDictColKind::TermRaw:
      return {C::kInvertedIndexTermRawId, duckdb::LogicalTypeId::BLOB,
              &TsDictReq::term_raw_col_idx, C::kTermRawName};
    case TsDictColKind::Count:
      return {C::kInvertedIndexTermCountId, duckdb::LogicalTypeId::INTEGER,
              &TsDictReq::count_col_idx, C::kTermCountName};
    case TsDictColKind::Freq:
      return {C::kInvertedIndexTermFreqId, duckdb::LogicalTypeId::BIGINT,
              &TsDictReq::freq_col_idx, C::kTermFreqName};
    case TsDictColKind::Score:
      return {C::kInvertedIndexTermScoreId, duckdb::LogicalTypeId::FLOAT,
              &TsDictReq::score_col_idx, C::kTermScoreName};
  }
  return {};
}

std::string TsDictColName(const connector::SereneDBScanBindData& bind_data,
                          irs::field_id field_id, TsDictColKind kind) {
  return absl::StrCat(
    TsDictColFor(kind).prefix,
    bind_data.DisplayColumnName(static_cast<catalog::Column::Id>(field_id)));
}

constexpr std::optional<std::pair<TsDictColKind, std::string_view>> TsDictFnFor(
  std::string_view fn) {
  using Fn = std::pair<TsDictColKind, std::string_view>;
  if (fn == connector::kTsDictAgg) {
    return Fn{TsDictColKind::Term, "list"};
  }
  if (fn == connector::kTsDictRawAgg) {
    return Fn{TsDictColKind::TermRaw, "list"};
  }
  if (fn == connector::kTsDictCount) {
    return Fn{TsDictColKind::Count, "list"};
  }
  if (fn == connector::kTsDictFreq) {
    return Fn{TsDictColKind::Freq, "list"};
  }
  if (fn == connector::kTsDictScore) {
    return Fn{TsDictColKind::Score, "list"};
  }
  if (fn == connector::kTsDictMin) {
    return Fn{TsDictColKind::Term, "min"};
  }
  if (fn == connector::kTsDictMax) {
    return Fn{TsDictColKind::Term, "max"};
  }
  return std::nullopt;
}

duckdb::idx_t EnsureTsDictCol(connector::SereneDBScanBindData& bind_data,
                              duckdb::LogicalGet& get, TsDictReq& req,
                              TsDictColKind kind) {
  const auto col = TsDictColFor(kind);
  auto& col_idx = req.*col.col_idx;
  if (col_idx == duckdb::DConstants::INVALID_INDEX) {
    col_idx = AppendVirtualGetColumn(
      bind_data, get, col.virtual_id, duckdb::LogicalType{col.type},
      TsDictColName(bind_data, req.field_id, kind));
  }
  return col_idx;
}

duckdb::unique_ptr<duckdb::Expression> BuildTsDictAggregate(
  duckdb::ClientContext& context, std::string_view agg_name,
  duckdb::unique_ptr<duckdb::Expression> child) {
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto& entry = sys.GetEntry<duckdb::AggregateFunctionCatalogEntry>(
    context, duckdb::QualifiedName(sys.GetName(), DEFAULT_SCHEMA,
                                   duckdb::Identifier{std::string{agg_name}}));
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(std::move(child));
  duckdb::FunctionBinder binder{context};
  duckdb::ErrorData error;
  const auto best =
    binder.BindFunction(duckdb::Identifier{std::string{agg_name}},
                        entry.functions, children, {}, error);
  if (!best.IsValid()) {
    error.Throw();
  }
  auto fn = entry.functions.GetFunctionByOffset(best.GetIndex());
  return binder.BindAggregateFunction(fn, std::move(children));
}

// With several fields the scan emits each field's terms in its own rows and
// leaves other fields' columns NULL; the list aggregate must skip those.
// count/min/max already ignore NULLs.
duckdb::unique_ptr<duckdb::Expression> BuildTsDictListAggregate(
  duckdb::ClientContext& context, std::string_view name,
  const duckdb::LogicalType& type, duckdb::ColumnBinding binding) {
  auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{std::string{name}}, type, binding);
  auto result = BuildTsDictAggregate(context, "list", std::move(ref));
  auto filter_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{std::string{name}}, type, binding);
  auto is_not_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, duckdb::LogicalType::BOOLEAN);
  is_not_null->GetChildrenMutable().push_back(std::move(filter_ref));
  result->Cast<duckdb::BoundAggregateExpression>().GetFilterMutable() =
    std::move(is_not_null);
  return result;
}

duckdb::unique_ptr<duckdb::Expression> MakeTsDictAggregate(
  duckdb::LogicalOperator& root, duckdb::ClientContext& context,
  FoundScan& found, irs::field_id field_id, TsDictColKind kind,
  std::string_view agg_name, duckdb::TableIndex table_index,
  const duckdb::Identifier& alias) {
  auto& req = found.bind_data->TsDictFor(field_id);
  const duckdb::LogicalType col_type{TsDictColFor(kind).type};
  const auto col_idx = EnsureTsDictCol(*found.bind_data, *found.get, req, kind);
  using enum connector::TsDictTermUses;
  req.term_uses |= agg_name == "min" ? kMin : agg_name == "max" ? kMax : kFull;
  const auto name = TsDictColName(*found.bind_data, field_id, kind);
  const auto binding =
    ExposeGetColumnAt(root, table_index, *found.get, col_idx, name, col_type);
  auto result = agg_name == "list"
                  ? BuildTsDictListAggregate(context, name, col_type, binding)
                  : BuildTsDictAggregate(
                      context, agg_name,
                      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
                        duckdb::Identifier{name}, col_type, binding));
  result->SetAlias(alias);
  return result;
}

const duckdb::BoundColumnRefExpression* SingleColumnRefChild(
  const duckdb::BoundAggregateExpression& agg) {
  const auto& children = agg.GetChildren();
  if (children.size() != 1 || children[0]->GetExpressionClass() !=
                                duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &children[0]->Cast<duckdb::BoundColumnRefExpression>();
}

duckdb::unique_ptr<duckdb::Expression> PushdownTsDictCall(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context) {
  const auto& fn = agg.Function().GetName().GetIdentifierName();
  const auto fn_info = TsDictFnFor(fn);
  SDB_ASSERT(fn_info.has_value());
  const auto* col_ref = SingleColumnRefChild(agg);
  if (!col_ref) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "() argument must be an indexed column"));
  }
  auto resolved = ResolveIResearchScanColumn(root, col_ref->Binding());
  if (!resolved) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, "() requires an inverted index scan in the same sub-query"));
  }
  auto& found = resolved->found;
  const auto col_id =
    ResolveColumnId(resolved->binding, *found.bind_data, *found.get);
  if (col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "(): column not found in index"));
  }
  const auto* info = found.bind_data->inverted_index->FindColumnInfo(col_id);
  const auto& col_type = col_ref->GetReturnType();
  const auto text_type = [&] {
    switch (col_type.id()) {
      case duckdb::LogicalTypeId::VARCHAR:
        return true;
      case duckdb::LogicalTypeId::LIST:
        return duckdb::ListType::GetChildType(col_type).id() ==
               duckdb::LogicalTypeId::VARCHAR;
      case duckdb::LogicalTypeId::ARRAY:
        return duckdb::ArrayType::GetChildType(col_type).id() ==
               duckdb::LogicalTypeId::VARCHAR;
      default:
        return false;
    }
  }();
  if (!info || !info->IsTermDict() || !text_type) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(fn, "(): column has no text term dictionary"),
      ERR_HINT("ts_dict_agg requires a text-tokenized or keyword VARCHAR "
               "column, or an array of such"));
  }

  const auto [kind, agg_name] = *fn_info;
  return MakeTsDictAggregate(root, context, found,
                             static_cast<irs::field_id>(col_id), kind, agg_name,
                             col_ref->Binding().table_index, agg.GetAlias());
}

struct TsDictColRef {
  size_t req_index;
  TsDictColKind kind;
};

std::optional<TsDictColRef> ClassifyTsDictGetCol(
  const connector::SereneDBScanBindData& ss, duckdb::idx_t get_col_idx) {
  for (size_t r = 0; r < ss.ts_dicts.size(); ++r) {
    for (const auto kind : kAllTsDictColKinds) {
      if (get_col_idx == ss.ts_dicts[r].*TsDictColFor(kind).col_idx) {
        return TsDictColRef{r, kind};
      }
    }
  }
  return std::nullopt;
}

void CollectTsDictScans(duckdb::LogicalOperator& op,
                        std::vector<FoundScan>& out) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    if (auto f = AsSearchScan(op); f && f->bind_data->TsDictMode()) {
      out.push_back(*f);
    }
    return;
  }
  for (auto& child : op.children) {
    CollectTsDictScans(*child, out);
  }
}

std::optional<FoundScan> FindTsDictFoundScan(duckdb::LogicalOperator& op) {
  std::vector<FoundScan> scans;
  CollectTsDictScans(op, scans);
  if (scans.empty()) {
    return std::nullopt;
  }
  return scans.front();
}

template<typename F>
void WalkColumnRefs(duckdb::unique_ptr<duckdb::Expression>& expr, F&& fn) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    fn(expr);
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      WalkColumnRefs(child, fn);
    });
}

struct TsDictGroupEntry {
  duckdb::ColumnBinding source;
  duckdb::LogicalType source_type;
  TsDictColKind kind;
  size_t req_index;
  duckdb::ColumnBinding output;
  duckdb::LogicalType output_type;
};

duckdb::unique_ptr<duckdb::LogicalOperator> InjectTsDictGroupBy(
  duckdb::unique_ptr<duckdb::LogicalOperator> child,
  std::span<duckdb::unique_ptr<duckdb::Expression>> consumer_roots,
  duckdb::Binder& binder, duckdb::ClientContext& context) {
  auto found = FindTsDictFoundScan(*child);
  SDB_ASSERT(found.has_value());
  const auto get_ti = found->get->table_index;
  auto& ss = *found->bind_data;

  std::vector<TsDictGroupEntry> entries;
  const auto find_entry =
    [&](const duckdb::ColumnBinding& b) -> TsDictGroupEntry* {
    for (auto& e : entries) {
      if (e.source == b) {
        return &e;
      }
    }
    return nullptr;
  };

  for (auto& root : consumer_roots) {
    WalkColumnRefs(root, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
      auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
      if (find_entry(ref.Binding())) {
        return;
      }
      const auto resolved =
        ResolveBindingThroughProjections(*child, ref.Binding());
      if (resolved.table_index != get_ti) {
        return;
      }
      const auto col =
        ClassifyTsDictGetCol(ss, resolved.column_index.GetIndex());
      if (!col) {
        return;
      }
      entries.push_back({.source = ref.Binding(),
                         .source_type = ref.GetReturnType(),
                         .kind = col->kind,
                         .req_index = col->req_index});
    });
  }
  SDB_ASSERT(!entries.empty());

  for (size_t r = 0; r < ss.ts_dicts.size(); ++r) {
    bool referenced = false;
    bool has_group = false;
    for (const auto& e : entries) {
      if (e.req_index != r) {
        continue;
      }
      referenced = true;
      if (e.kind == TsDictColKind::Term || e.kind == TsDictColKind::TermRaw) {
        has_group = true;
      }
    }
    if (!referenced || has_group) {
      continue;
    }
    auto& req = ss.ts_dicts[r];
    EnsureTsDictCol(*found->bind_data, *found->get, req, TsDictColKind::Term);
    req.term_uses |= connector::TsDictTermUses::kFull;
    const auto anchor =
      child->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION
        ? child->Cast<duckdb::LogicalProjection>().table_index
        : get_ti;
    const auto source = ExposeGetColumnAt(
      *child, anchor, *found->get, req.term_col_idx,
      TsDictColName(*found->bind_data, req.field_id, TsDictColKind::Term),
      duckdb::LogicalType::VARCHAR);
    entries.push_back({.source = source,
                       .source_type = duckdb::LogicalType::VARCHAR,
                       .kind = TsDictColKind::Term,
                       .req_index = r});
  }

  const auto group_index = binder.GenerateTableIndex();
  const auto agg_index = binder.GenerateTableIndex();
  const auto groupings_index = binder.GenerateTableIndex();

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> groups;
  for (auto& e : entries) {
    if (e.kind != TsDictColKind::Term && e.kind != TsDictColKind::TermRaw) {
      continue;
    }
    e.output = {group_index, duckdb::ProjectionIndex{groups.size()}};
    e.output_type = e.source_type;
    groups.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      e.source_type, e.source));
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  for (auto& e : entries) {
    if (e.kind == TsDictColKind::Term || e.kind == TsDictColKind::TermRaw) {
      continue;
    }
    auto agg = BuildTsDictAggregate(
      context, e.kind == TsDictColKind::Score ? "max" : "sum",
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(e.source_type,
                                                          e.source));
    e.output = {agg_index, duckdb::ProjectionIndex{select_list.size()}};
    e.output_type = agg->GetReturnType();
    select_list.push_back(std::move(agg));
  }

  auto aggregate = duckdb::make_uniq<duckdb::LogicalAggregate>(
    group_index, agg_index, std::move(select_list));
  aggregate->groupings_index = groupings_index;
  aggregate->groups = std::move(groups);
  aggregate->children.push_back(std::move(child));
  aggregate->ResolveOperatorTypes();
  aggregate->group_stats.resize(aggregate->groups.size());

  for (auto& root : consumer_roots) {
    WalkColumnRefs(root, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
      auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
      auto* e = find_entry(ref.Binding());
      if (!e) {
        return;
      }
      duckdb::unique_ptr<duckdb::Expression> repl =
        duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
          ref.GetAlias(), e->output_type, e->output);
      if (e->output_type != e->source_type) {
        repl = duckdb::BoundCastExpression::AddCastToType(
          context, std::move(repl), e->source_type);
      }
      ref_expr = std::move(repl);
    });
  }

  return aggregate;
}

}  // namespace

// `unnest(ts_dict_agg(f))` builds a per-group LIST then explodes it back to one
// row per term -- an identity round-trip over the rows the term-dict scan
// already emits. Collapse `Unnest( Aggregate[list(col)...] )` into a plain
// projection of those columns when the subtree is a term-dict scan.
void CollapseTsDictUnnest(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto& unnest = plan->Cast<duckdb::LogicalUnnest>();
  if (plan->children.size() != 1 ||
      plan->children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return;
  }
  auto& agg = plan->children[0]->Cast<duckdb::LogicalAggregate>();
  if (!agg.groups.empty() || agg.children.size() != 1) {
    return;
  }
  for (auto& a : agg.expressions) {
    if (a->GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
      return;
    }
    auto& ba = a->Cast<duckdb::BoundAggregateExpression>();
    if (ba.Function().GetName() != "list" || ba.GetChildren().size() != 1) {
      return;
    }
    const auto* inner = ba.GetChildren()[0].get();
    if (inner->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
      inner = &inner->Cast<duckdb::BoundCastExpression>().Child();
    }
    if (inner->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return;
    }
  }
  // Only safe for a single enumerated field of a single dict scan: with
  // several fields (or several scans) the terms live in disjoint rows, so
  // unnest must positionally zip the lists (DuckDB's normal Unnest) rather
  // than read the scan rows directly.
  std::vector<FoundScan> scans;
  CollectTsDictScans(*agg.children[0], scans);
  if (scans.size() != 1 || scans.front().bind_data->ts_dicts.size() != 1) {
    return;
  }

  std::vector<duckdb::idx_t> list_indexes;
  list_indexes.reserve(unnest.expressions.size());
  for (auto& u : unnest.expressions) {
    if (u->GetExpressionClass() != duckdb::ExpressionClass::BOUND_UNNEST) {
      return;
    }
    auto& bu = u->Cast<duckdb::BoundUnnestExpression>();
    if (bu.Child()->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return;
    }
    const auto& ref = bu.Child()->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index != agg.aggregate_index) {
      return;
    }
    const auto k = ref.Binding().column_index.GetIndex();
    if (k >= agg.expressions.size()) {
      return;
    }
    list_indexes.push_back(k);
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  proj_exprs.reserve(list_indexes.size());
  for (const auto k : list_indexes) {
    auto& ba = agg.expressions[k]->Cast<duckdb::BoundAggregateExpression>();
    proj_exprs.push_back(ba.GetChildren()[0]->Copy());
  }

  auto child = std::move(agg.children[0]);
  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    unnest.unnest_index, std::move(proj_exprs));
  for (auto& e : proj->expressions) {
    proj->types.push_back(e->GetReturnType());
  }
  proj->children.push_back(std::move(child));
  plan = std::move(proj);
}

namespace {

struct KeywordDictAgg {
  duckdb::ColumnBinding binding;
  irs::field_id field_id;
  std::string_view agg;
};

// Classifies every column reference in a predicate against a set of
// enumerated fields: the matched key (or several -> multi_enum), the
// term virtual column, and everything else (non_enum).
struct EnumFieldRefs {
  std::optional<size_t> matched_key;
  bool multi_enum = false;
  bool term_virtual = false;
  bool non_enum = false;
  bool any_ref = false;

  bool OnlyField() const { return (matched_key || term_virtual) && !non_enum; }
};

void CollectEnumFieldRefs(
  const duckdb::Expression& expr,
  const containers::FlatHashMap<irs::field_id, size_t>& key_by_field,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get, EnumFieldRefs& refs) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    refs.any_ref = true;
    const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
    if (col_id == catalog::Column::kInvertedIndexTermId) {
      refs.term_virtual = true;
      return;
    }
    if (col_id != catalog::Column::kInvalidId) {
      const auto it = key_by_field.find(static_cast<irs::field_id>(col_id));
      if (it != key_by_field.end()) {
        if (!refs.matched_key) {
          refs.matched_key = it->second;
        } else if (*refs.matched_key != it->second) {
          refs.multi_enum = true;
        }
        return;
      }
    }
    refs.non_enum = true;
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      CollectEnumFieldRefs(child, key_by_field, bind_data, get, refs);
    });
}

bool IsCoveredColumnResidual(const duckdb::Expression& expr,
                             const connector::SereneDBScanBindData& bind_data,
                             const duckdb::LogicalGet& get,
                             const catalog::InvertedIndex& index) {
  using C = duckdb::ExpressionClass;
  using T = duckdb::ExpressionType;
  auto col = catalog::Column::kInvalidId;
  const auto walk = [&](this auto& self, const duckdb::Expression& e) -> bool {
    const auto cls = e.GetExpressionClass();
    if (cls == C::BOUND_COLUMN_REF) {
      const auto id = ResolveColumnId(
        e.Cast<duckdb::BoundColumnRefExpression>().Binding(), bind_data, get);
      if (id == catalog::Column::kInvalidId ||
          id == catalog::Column::kInvertedIndexTermId) {
        return false;
      }
      const auto* info = index.FindColumnInfo(id);
      if (!info || !info->IsStored()) {
        return false;
      }
      if (col != catalog::Column::kInvalidId && col != id) {
        return false;
      }
      col = id;
      return true;
    }
    if (cls == C::BOUND_SUBQUERY) {
      return false;
    }
    const auto type = e.GetExpressionType();
    if (type == T::COMPARE_IN || type == T::COMPARE_NOT_IN ||
        type == T::CONJUNCTION_OR) {
      return false;
    }
    bool ok = true;
    duckdb::ExpressionIterator::EnumerateChildren(
      e, [&](const duckdb::Expression& child) { ok = ok && self(child); });
    return ok;
  };
  return walk(expr) && col != catalog::Column::kInvalidId;
}

bool ResidualComputesOverColumn(const duckdb::Expression& expr) {
  using C = duckdb::ExpressionClass;
  using T = duckdb::ExpressionType;
  const auto cls = expr.GetExpressionClass();
  if (cls == C::BOUND_COLUMN_REF || cls == C::BOUND_CONSTANT) {
    return false;
  }
  const auto type = expr.GetExpressionType();
  const bool plain_comparison =
    type == T::COMPARE_EQUAL || type == T::COMPARE_NOTEQUAL ||
    type == T::COMPARE_LESSTHAN || type == T::COMPARE_GREATERTHAN ||
    type == T::COMPARE_LESSTHANOREQUALTO ||
    type == T::COMPARE_GREATERTHANOREQUALTO || type == T::CONJUNCTION_AND;
  if (!plain_comparison) {
    return !expr.IsFoldable();
  }
  bool found = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      found = found || ResidualComputesOverColumn(child);
    });
  return found;
}

struct TsDictFacetKey {
  irs::field_id field_id = irs::field_limits::invalid();
  catalog::Column::Id col_id = catalog::Column::kInvalidId;
  irs::field_id null_field_id = irs::field_limits::invalid();
  duckdb::TableIndex anchor;

  bool nullable() const { return irs::field_limits::valid(null_field_id); }
};

// GROUP BY facet -> term-dictionary scan conversion. Convert() rewrites
// the aggregate in place when every phase accepts and leaves the plan
// untouched otherwise. Phases, in order: grouping shape, group keys
// (indexed keyword columns/expressions of ONE scan over the target),
// aggregates (count_star, or count(<front key column>) on a NOT NULL
// key), WHERE claimability dry-run, then emission: scan term/count
// columns, grouping-set collapse, aggregate rewrite, projection rebuild.
class TsDictFacetPushdown {
 public:
  TsDictFacetPushdown(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                      duckdb::LogicalOperator& root, duckdb::LogicalGet& target,
                      duckdb::LogicalFilter* where_filter,
                      duckdb::Binder& binder, duckdb::ClientContext& context)
    : _plan{plan},
      _root{root},
      _target{target},
      _where{where_filter},
      _binder{binder},
      _context{context},
      _aggr{plan->Cast<duckdb::LogicalAggregate>()},
      _multi_set{_aggr.grouping_sets.size() > 1},
      _snapshot{connector::GetSereneDBContext(context).CatalogSnapshot()} {}

  bool Convert();

 private:
  struct FacetCols {
    std::string term_name;
    std::string count_name;
    duckdb::ColumnBinding count_binding;
  };

  bool ShapeOk() const;
  bool AdoptScan(std::optional<FoundScan> here);
  bool ResolveExpressionKey(const duckdb::Expression& expr,
                            TsDictFacetKey& key);
  bool ResolveKeys();
  bool AggregatesOk() const;
  bool WhereOk();
  void EmitScanColumns();
  void CollapseGroupingSets();
  void RewriteAggregates();
  void RebuildProjection();
  duckdb::unique_ptr<duckdb::Expression> MakeCountRef(size_t g) const;
  bool NotNull() const { return !_keys.front().nullable(); }

  duckdb::unique_ptr<duckdb::LogicalOperator>& _plan;
  duckdb::LogicalOperator& _root;
  duckdb::LogicalGet& _target;
  duckdb::LogicalFilter* _where;
  duckdb::Binder& _binder;
  duckdb::ClientContext& _context;
  duckdb::LogicalAggregate& _aggr;
  const bool _multi_set;
  std::shared_ptr<const catalog::Snapshot> _snapshot;

  FoundScan _found{};
  const catalog::InvertedIndex* _index = nullptr;
  std::vector<TsDictFacetKey> _keys;
  std::vector<FacetCols> _cols;
  std::vector<duckdb::LogicalType> _old_types;
  std::optional<std::vector<catalog::Column::Id>> _projected_ids;
};

// True if the expression tree contains a TSQUERY-typed node, i.e. an
// optimizer-claimed @@ ts_* matcher with no scalar fallback: such a predicate
// can only run as a claimed search filter, never as a post-filter.
bool ContainsTSQueryExpr(const duckdb::Expression& expr) {
  if (connector::IsTSQueryStructType(expr.GetReturnType())) {
    return true;
  }
  bool found = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      found = found || ContainsTSQueryExpr(child);
    });
  return found;
}

// The implicit rewrites below are only sound for a bare `SELECT <aggs> FROM
// idx` shape, optionally with ONE filter whose every conjunct references only
// the enumerated field (the ts_dict claim then turns it into a term acceptor
// or a term post-filter). Anything else between the aggregate and the scan
// consumes document rows, which the term-dict scan no longer produces.
std::pair<duckdb::LogicalGet*, duckdb::LogicalFilter*> ImplicitTsDictTarget(
  duckdb::LogicalOperator& aggr) {
  auto* node = &aggr;
  duckdb::LogicalFilter* filter = nullptr;
  while (node->children.size() == 1) {
    auto* child = node->children[0].get();
    if (child->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      return {&child->Cast<duckdb::LogicalGet>(), filter};
    }
    if (child->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      if (filter) {
        return {nullptr, nullptr};
      }
      filter = &child->Cast<duckdb::LogicalFilter>();
    } else if (child->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return {nullptr, nullptr};
    }
    node = child;
  }
  return {nullptr, nullptr};
}

// `array_agg(DISTINCT col)`, `count(DISTINCT col)`, `min(col)` and `max(col)`
// over a keyword-analyzed inverted-index column reduce exactly the field's
// distinct terms, so they can be served from the term dictionary. count/min/max
// skip NULLs and work on any such column, view-backed indexes included;
// list()/array_agg keeps a NULL element the term dictionary has none for, so it
// converts only for a proven-NOT-NULL table column and otherwise stays a scan.
std::optional<KeywordDictAgg> ClassifyKeywordDictAgg(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  const duckdb::LogicalGet& target, const catalog::Snapshot& snapshot) {
  const auto& name = agg.Function().GetName();
  std::string_view agg_name;
  if (name == "array_agg" || name == "list") {
    agg_name = "list";
  } else if (name == "count") {
    agg_name = "count";
  } else if (name == "min") {
    agg_name = "min";
  } else if (name == "max") {
    agg_name = "max";
  } else {
    return std::nullopt;
  }
  const bool is_list = agg_name == "list";
  if ((is_list || agg_name == "count") && !agg.IsDistinct()) {
    return std::nullopt;
  }
  if (agg.GetFilter() || agg.GetOrderBys()) {
    return std::nullopt;
  }
  const auto* col_ref_ptr = SingleColumnRefChild(agg);
  if (!col_ref_ptr) {
    return std::nullopt;
  }
  const auto& col_ref = *col_ref_ptr;
  if (col_ref.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  const auto resolved = ResolveIResearchScanColumn(root, col_ref.Binding());
  if (!resolved || resolved->found.get != &target) {
    return std::nullopt;
  }
  const auto& found = resolved->found;
  const auto col_id =
    ResolveColumnId(resolved->binding, *found.bind_data, *found.get);
  if (col_id == catalog::Column::kInvalidId) {
    return std::nullopt;
  }
  const auto* index = found.bind_data->inverted_index.get();
  if (!index) {
    return std::nullopt;
  }
  const auto field_id = static_cast<irs::field_id>(col_id);
  if (!index->IsKeywordField(snapshot, field_id)) {
    return std::nullopt;
  }
  if (is_list && !found.bind_data->IsColumnNotNull(col_id)) {
    return std::nullopt;
  }
  return KeywordDictAgg{col_ref.Binding(), field_id, agg_name};
}

duckdb::unique_ptr<duckdb::Expression> BuildKeywordTsDictAggregate(
  const KeywordDictAgg& match, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context, const duckdb::Identifier& alias) {
  auto resolved = ResolveIResearchScanColumn(root, match.binding);
  SDB_ASSERT(resolved.has_value());
  return MakeTsDictAggregate(root, context, resolved->found, match.field_id,
                             TsDictColKind::Term, match.agg,
                             match.binding.table_index, alias);
}

// `SELECT col, count(*) FROM idx GROUP BY col` over a keyword NOT NULL column
// is a facet: the groups are exactly the field's live terms and each group's
// size is the term's live doc count. The group key is repointed at the term
// column, every count(*) / count(col) becomes sum(term_count) (the user's own
// GROUP BY merges the per-segment rows), and a projection above casts the sums
// back to count's BIGINT. A bare GROUP BY with no aggregates converts too.
bool TsDictFacetPushdown::ShapeOk() const {
  if (!_multi_set) {
    return _aggr.groups.size() == 1;
  }
  if (_aggr.groups.size() != _aggr.grouping_sets.size() ||
      !_aggr.grouping_functions.empty()) {
    return false;
  }
  duckdb::GroupingSet covered;
  for (const auto& set : _aggr.grouping_sets) {
    if (set.size() != 1) {
      return false;
    }
    covered.insert(*set.begin());
  }
  return covered.size() == _aggr.groups.size();
}

bool TsDictFacetPushdown::AdoptScan(std::optional<FoundScan> here) {
  if (!here || here->get != &_target) {
    return false;
  }
  if (_index) {
    return true;
  }
  if (!here->bind_data->inverted_index) {
    return false;
  }
  _found = *here;
  _index = here->bind_data->inverted_index.get();
  return true;
}

// Expression keys have no NOT NULL proof, so they always carry the
// null-marker field for the synthesized NULL group.
bool TsDictFacetPushdown::ResolveExpressionKey(const duckdb::Expression& expr,
                                               TsDictFacetKey& key) {
  const auto expr_ti = SingleReferencedTableIndex(expr);
  if (!expr_ti || !AdoptScan(FindIResearchScan(_root, *expr_ti))) {
    return false;
  }
  if (!_projected_ids) {
    _projected_ids = BuildProjectedColumnIds(*_found.get, *_found.bind_data);
  }
  auto normalized = connector::NormalizeBoundExpression(
    expr, _index->GetRelationId(), *_projected_ids, _context);
  const auto serialized = connector::SerializeBoundExpression(*normalized);
  key.field_id = _index->FindFieldIdBySerialized(serialized);
  if (!irs::field_limits::valid(key.field_id)) {
    return false;
  }
  const auto* info = _index->FindEntry(key.field_id);
  if (!info || !irs::field_limits::valid(info->null_field_id)) {
    return false;
  }
  key.null_field_id = info->null_field_id;
  key.anchor = _found.get->table_index;
  return true;
}

// Every group key resolves to an indexed keyword field: plain columns
// through the projection chain, everything else as a serialized indexed
// expression. Nullable keys form a NULL group the dictionary has no term
// for; the scan synthesizes it from the null-marker field. With grouping
// sets a second nullable key's genuine NULL group would collide with the
// first one's all-NULL group key, so at most one key may be nullable.
bool TsDictFacetPushdown::ResolveKeys() {
  _keys.reserve(_aggr.groups.size());
  for (auto& group : _aggr.groups) {
    if (group->GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
      return false;
    }
    TsDictFacetKey key;
    if (group->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      if (!ResolveExpressionKey(*group, key)) {
        return false;
      }
    } else {
      auto& ref = group->Cast<duckdb::BoundColumnRefExpression>();
      const auto walked = WalkProjections(_root, ref.Binding());
      auto col_id = catalog::Column::kInvalidId;
      if (AdoptScan(FindIResearchScan(_root, walked.binding.table_index))) {
        col_id =
          ResolveColumnId(walked.binding, *_found.bind_data, *_found.get);
      }
      if (col_id != catalog::Column::kInvalidId) {
        key.field_id = static_cast<irs::field_id>(col_id);
        key.col_id = col_id;
        if (!_found.bind_data->IsColumnNotNull(col_id)) {
          const auto* col_info = _index->FindColumnInfo(col_id);
          if (!col_info || !irs::field_limits::valid(col_info->null_field_id)) {
            return false;
          }
          key.null_field_id = col_info->null_field_id;
        }
      } else if (!walked.stop_expr ||
                 !ResolveExpressionKey(*walked.stop_expr, key)) {
        return false;
      }
      key.anchor = ref.Binding().table_index;
    }
    if (!_index->IsKeywordField(*_snapshot, key.field_id) ||
        absl::c_any_of(_keys, [&](const TsDictFacetKey& k) {
          return k.field_id == key.field_id;
        })) {
      return false;
    }
    _keys.push_back(key);
  }
  return !_multi_set || absl::c_count_if(_keys, &TsDictFacetKey::nullable) <= 1;
}

// count_star always converts; a single-set facet additionally accepts
// count(<front key column>) on a NOT NULL key (it counts NULL rows as
// zero, which only matches sum(term_count) when there are none).
bool TsDictFacetPushdown::AggregatesOk() const {
  for (const auto& expr : _aggr.expressions) {
    if (expr->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_AGGREGATE) {
      return false;
    }
    auto& agg = expr->Cast<duckdb::BoundAggregateExpression>();
    if (agg.IsDistinct() || agg.GetFilter() || agg.GetOrderBys()) {
      return false;
    }
    const auto& name = agg.Function().GetName();
    if (name == "count_star") {
      continue;
    }
    if (_multi_set || !NotNull()) {
      return false;
    }
    const auto* ref = SingleColumnRefChild(agg);
    if (name != "count" || !ref) {
      return false;
    }
    const auto child = ResolveBindingThroughProjections(_root, ref->Binding());
    if (ResolveColumnId(child, *_found.bind_data, *_found.get) !=
        _keys.front().col_id) {
      return false;
    }
  }
  return true;
}

void TsDictFacetPushdown::EmitScanColumns() {
  _cols.reserve(_aggr.groups.size());
  for (size_t g = 0; g < _aggr.groups.size(); ++g) {
    const auto& key = _keys[g];
    auto& req = _found.bind_data->TsDictFor(key.field_id);
    EnsureTsDictCol(*_found.bind_data, *_found.get, req, TsDictColKind::Term);
    EnsureTsDictCol(*_found.bind_data, *_found.get, req, TsDictColKind::Count);
    req.term_uses |= connector::TsDictTermUses::kFull;
    req.null_field_id = key.null_field_id;

    auto term_name =
      TsDictColName(*_found.bind_data, key.field_id, TsDictColKind::Term);
    auto count_name =
      TsDictColName(*_found.bind_data, key.field_id, TsDictColKind::Count);
    const auto term_binding =
      ExposeGetColumnAt(_root, key.anchor, *_found.get, req.term_col_idx,
                        term_name, duckdb::LogicalType::VARCHAR);
    const auto count_binding =
      ExposeGetColumnAt(_root, key.anchor, *_found.get, req.count_col_idx,
                        count_name, duckdb::LogicalType::INTEGER);

    _aggr.groups[g] = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      duckdb::Identifier{term_name}, duckdb::LogicalType::VARCHAR,
      term_binding);
    _cols.push_back(
      {std::move(term_name), std::move(count_name), count_binding});
  }
}

void TsDictFacetPushdown::CollapseGroupingSets() {
  if (!_multi_set) {
    return;
  }
  duckdb::GroupingSet all;
  for (duckdb::idx_t g = 0; g < _aggr.groups.size(); ++g) {
    all.insert(duckdb::ProjectionIndex{g});
  }
  _aggr.grouping_sets.clear();
  _aggr.grouping_sets.push_back(std::move(all));
}

duckdb::unique_ptr<duckdb::Expression> TsDictFacetPushdown::MakeCountRef(
  size_t g) const {
  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{_cols[g].count_name}, duckdb::LogicalType::INTEGER,
    _cols[g].count_binding);
}

void TsDictFacetPushdown::RewriteAggregates() {
  for (auto& expr : _aggr.expressions) {
    _old_types.push_back(expr->GetReturnType());
    duckdb::unique_ptr<duckdb::Expression> child;
    if (_aggr.groups.size() == 1) {
      child = MakeCountRef(0);
    } else {
      auto coalesce = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
        duckdb::ExpressionType::OPERATOR_COALESCE,
        duckdb::LogicalType::INTEGER);
      for (size_t g = 0; g < _aggr.groups.size(); ++g) {
        coalesce->GetChildrenMutable().push_back(MakeCountRef(g));
      }
      child = std::move(coalesce);
    }
    expr = BuildTsDictAggregate(_context, "sum", std::move(child));
  }
  _aggr.group_stats.clear();
  _aggr.group_stats.resize(_aggr.groups.size());
  _aggr.ResolveOperatorTypes();
}

void TsDictFacetPushdown::RebuildProjection() {
  if (_old_types.empty()) {
    return;
  }
  const auto proj_index = _binder.GenerateTableIndex();
  const auto ngroups = _aggr.groups.size();
  duckdb::ColumnBindingReplacer replacer;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  for (size_t g = 0; g < ngroups; ++g) {
    replacer.replacement_bindings.emplace_back(
      duckdb::ColumnBinding{_aggr.group_index, duckdb::ProjectionIndex{g}},
      duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{g}});
    proj_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      duckdb::Identifier{_cols[g].term_name}, duckdb::LogicalType::VARCHAR,
      duckdb::ColumnBinding{_aggr.group_index, duckdb::ProjectionIndex{g}}));
  }
  for (size_t i = 0; i < _old_types.size(); ++i) {
    replacer.replacement_bindings.emplace_back(
      duckdb::ColumnBinding{_aggr.aggregate_index, duckdb::ProjectionIndex{i}},
      duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{ngroups + i}});
    duckdb::unique_ptr<duckdb::Expression> sum_ref =
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        _aggr.expressions[i]->GetReturnType(),
        duckdb::ColumnBinding{_aggr.aggregate_index,
                              duckdb::ProjectionIndex{i}});
    proj_exprs.push_back(duckdb::BoundCastExpression::AddCastToType(
      _context, std::move(sum_ref), _old_types[i]));
  }
  replacer.stop_operator = _plan.get();
  replacer.VisitOperator(_root);

  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    proj_index, std::move(proj_exprs));
  for (auto& e : proj->expressions) {
    proj->types.push_back(e->GetReturnType());
  }
  proj->children.push_back(std::move(_plan));
  _plan = std::move(proj);
}

bool TsDictFacetPushdown::Convert() {
  if (!ShapeOk() || !ResolveKeys() || !AggregatesOk() || !WhereOk()) {
    return false;
  }
  EmitScanColumns();
  CollapseGroupingSets();
  RewriteAggregates();
  RebuildProjection();
  return true;
}

bool TryPushdownTsDictFacet(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                            duckdb::LogicalOperator& root,
                            duckdb::LogicalGet& target,
                            duckdb::LogicalFilter* where_filter,
                            duckdb::Binder& binder,
                            duckdb::ClientContext& context) {
  return TsDictFacetPushdown{plan, root, target, where_filter, binder, context}
    .Convert();
}

void RemapColumnRefs(
  duckdb::unique_ptr<duckdb::Expression>& expr,
  const std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>>& map) {
  WalkColumnRefs(expr, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
    auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
    for (const auto& [from, to] : map) {
      if (ref.Binding().table_index == from) {
        ref.BindingMutable().table_index = to;
        break;
      }
    }
  });
}

duckdb::unique_ptr<duckdb::LogicalOperator> CopyTsDictSourceChain(
  duckdb::LogicalOperator& top, duckdb::Binder& binder,
  std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>>& index_map) {
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = top.Cast<duckdb::LogicalGet>();
    if (get.table_filters.HasFilters()) {
      return nullptr;
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalGet>(
      binder.GenerateTableIndex(), get.function, get.bind_data->Copy(),
      get.returned_types, get.names, get.virtual_columns);
    copy->GetMutableColumnIds() = get.GetColumnIds();
    index_map.emplace_back(get.table_index, copy->table_index);
    return copy;
  }
  if (top.children.size() != 1) {
    return nullptr;
  }
  auto child = CopyTsDictSourceChain(*top.children[0], binder, index_map);
  if (!child) {
    return nullptr;
  }
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    auto& filter = top.Cast<duckdb::LogicalFilter>();
    if (!filter.projection_map.empty()) {
      return nullptr;
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalFilter>();
    for (const auto& e : filter.expressions) {
      auto expr = e->Copy();
      RemapColumnRefs(expr, index_map);
      copy->expressions.push_back(std::move(expr));
    }
    copy->children.push_back(std::move(child));
    return copy;
  }
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = top.Cast<duckdb::LogicalProjection>();
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
    for (const auto& e : proj.expressions) {
      auto expr = e->Copy();
      RemapColumnRefs(expr, index_map);
      exprs.push_back(std::move(expr));
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalProjection>(
      binder.GenerateTableIndex(), std::move(exprs));
    index_map.emplace_back(proj.table_index, copy->table_index);
    copy->children.push_back(std::move(child));
    return copy;
  }
  return nullptr;
}

// Mixed ts_dict_* + ordinary aggregates over one scan: duplicate the source
// chain for the ordinary aggregates, keep the dict aggregates on the original
// (to be converted to term-dict enumeration by the caller), cross-join the
// two single-row results and restore the original output order.
bool TrySplitMixedTsDictAggregates(
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
  duckdb::LogicalOperator& root, duckdb::Binder& binder,
  std::vector<size_t>& ts_dict_calls) {
  auto& aggr = plan->Cast<duckdb::LogicalAggregate>();
  if (!aggr.groups.empty() || aggr.children.size() != 1) {
    return false;
  }
  std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>> index_map;
  auto doc_child = CopyTsDictSourceChain(*aggr.children[0], binder, index_map);
  if (!doc_child) {
    return false;
  }

  const size_t n = aggr.expressions.size();
  std::vector<bool> is_dict(n, false);
  for (const auto i : ts_dict_calls) {
    is_dict[i] = true;
  }
  std::vector<duckdb::LogicalType> out_types;
  out_types.reserve(n);
  for (const auto& e : aggr.expressions) {
    out_types.push_back(e->GetReturnType());
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> dict_exprs;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> doc_exprs;
  std::vector<duckdb::idx_t> slot_of(n);
  for (size_t i = 0; i < n; ++i) {
    if (is_dict[i]) {
      slot_of[i] = dict_exprs.size();
      dict_exprs.push_back(std::move(aggr.expressions[i]));
    } else {
      slot_of[i] = doc_exprs.size();
      auto expr = std::move(aggr.expressions[i]);
      RemapColumnRefs(expr, index_map);
      doc_exprs.push_back(std::move(expr));
    }
  }
  aggr.expressions = std::move(dict_exprs);
  aggr.types.clear();
  for (const auto& e : aggr.expressions) {
    aggr.types.push_back(e->GetReturnType());
  }
  ts_dict_calls.resize(aggr.expressions.size());
  std::iota(ts_dict_calls.begin(), ts_dict_calls.end(), size_t{0});

  auto doc_agg = duckdb::make_uniq<duckdb::LogicalAggregate>(
    binder.GenerateTableIndex(), binder.GenerateTableIndex(),
    std::move(doc_exprs));
  const auto doc_agg_index = doc_agg->aggregate_index;
  doc_agg->children.push_back(std::move(doc_child));
  for (const auto& e : doc_agg->expressions) {
    doc_agg->types.push_back(e->GetReturnType());
  }

  const auto proj_index = binder.GenerateTableIndex();
  duckdb::ColumnBindingReplacer replacer;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  for (size_t i = 0; i < n; ++i) {
    const auto src_index = is_dict[i] ? aggr.aggregate_index : doc_agg_index;
    proj_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      out_types[i],
      duckdb::ColumnBinding{src_index, duckdb::ProjectionIndex{slot_of[i]}}));
    replacer.replacement_bindings.emplace_back(
      duckdb::ColumnBinding{aggr.aggregate_index, duckdb::ProjectionIndex{i}},
      duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{i}});
  }
  replacer.stop_operator = plan.get();
  replacer.VisitOperator(root);

  auto cross =
    duckdb::LogicalCrossProduct::Create(std::move(plan), std::move(doc_agg));
  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    proj_index, std::move(proj_exprs));
  for (auto& e : proj->expressions) {
    proj->types.push_back(e->GetReturnType());
  }
  proj->children.push_back(std::move(cross));
  plan = std::move(proj);
  return true;
}

// A WHERE clause is convertible when every conjunct is claimable by the
// dict scan: same-field predicates (term level or post-filter) and indexed
// @@ document predicates (where level). Otherwise the conversion is dropped
// and the aggregates stay on the document scan.
bool KeywordAggsConvertible(
  const std::vector<std::pair<size_t, KeywordDictAgg>>& keyword_aggs,
  const duckdb::LogicalGet& target, duckdb::LogicalFilter* filter) {
  if (!filter) {
    return true;
  }
  const auto field = keyword_aggs.front().second.field_id;
  if (!absl::c_all_of(keyword_aggs, [&](const auto& entry) {
        return entry.second.field_id == field;
      })) {
    return false;
  }
  const auto& bind_data =
    target.bind_data->Cast<connector::SereneDBScanBindData>();
  const containers::FlatHashMap<irs::field_id, size_t> key_by_field{{field, 0}};
  for (auto& expr : filter->expressions) {
    EnumFieldRefs refs;
    CollectEnumFieldRefs(*expr, key_by_field, bind_data, target, refs);
    if (!refs.OnlyField() && !ContainsTSQueryExpr(*expr) &&
        !connector::ContainsIndexOnlyPredicate(*expr)) {
      return false;
    }
  }
  return true;
}

// min/max are duplicate-insensitive: the dedup GROUP BY would only bury
// the scan's two-term min/max seek shortcut under a full hash aggregation.
bool IsMinMaxColumnAgg(const duckdb::unique_ptr<duckdb::Expression>& e) {
  if (e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
    return false;
  }
  const auto& agg = e->Cast<duckdb::BoundAggregateExpression>();
  const auto& name = agg.Function().GetName();
  return (name == "min" || name == "max") && SingleColumnRefChild(agg);
}

}  // namespace

void PushdownTsDictAggregates(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                              duckdb::LogicalOperator& root,
                              duckdb::Binder& binder,
                              duckdb::ClientContext& context) {
  auto& aggr = plan->Cast<duckdb::LogicalAggregate>();
  std::vector<size_t> ts_dict_calls;
  std::vector<std::pair<size_t, KeywordDictAgg>> keyword_aggs;
  bool other = false;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  const auto [implicit_target, implicit_filter] = ImplicitTsDictTarget(aggr);
  if (!aggr.groups.empty() && implicit_target &&
      TryPushdownTsDictFacet(plan, root, *implicit_target, implicit_filter,
                             binder, context)) {
    return;
  }
  for (size_t i = 0; i < aggr.expressions.size(); ++i) {
    auto& expr = aggr.expressions[i];
    if (expr->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_AGGREGATE) {
      other = true;
      continue;
    }
    auto& agg = expr->Cast<duckdb::BoundAggregateExpression>();
    if (connector::IsTsDictFunctionName(
          agg.Function().GetName().GetIdentifierName())) {
      ts_dict_calls.push_back(i);
      continue;
    }
    if (aggr.groups.empty() && implicit_target) {
      if (!snapshot) {
        snapshot = connector::GetSereneDBContext(context).CatalogSnapshot();
      }
      if (auto match =
            ClassifyKeywordDictAgg(agg, root, *implicit_target, *snapshot)) {
        keyword_aggs.push_back({i, *match});
        continue;
      }
    }
    other = true;
  }

  if (!keyword_aggs.empty() &&
      !KeywordAggsConvertible(keyword_aggs, *implicit_target,
                              implicit_filter)) {
    keyword_aggs.clear();
    other = true;
  }

  // Converting a ts_dict aggregate switches the scan to term-dict rows, which
  // would silently corrupt any sibling aggregate that still expects document
  // rows (count(*) would count terms). Ungrouped mixed aggregates split into
  // two scans instead; grouped ones (or unsupported source shapes) stay
  // rejected.
  if (!ts_dict_calls.empty() && other) {
    if (!TrySplitMixedTsDictAggregates(plan, root, binder, ts_dict_calls)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ts_dict_* aggregates cannot be combined with other "
                "aggregates over the same scan"),
        ERR_HINT("compute the other aggregates in a separate subquery"));
    }
    keyword_aggs.clear();
    other = false;
  }

  if (!ts_dict_calls.empty() && !aggr.groups.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("ts_dict_* aggregates cannot be combined with GROUP BY"),
      ERR_HINT("compute the ts_dict_* aggregate in a separate subquery"));
  }
  bool converted = false;
  for (const auto i : ts_dict_calls) {
    auto& agg = aggr.expressions[i]->Cast<duckdb::BoundAggregateExpression>();
    aggr.expressions[i] = PushdownTsDictCall(agg, root, context);
    converted = true;
  }

  // Converting an implicit aggregate forces the scan into term-dict
  // enumeration, so it is only safe when every aggregate in the node can be
  // served that way.
  if (!other && !keyword_aggs.empty()) {
    for (const auto& [i, match] : keyword_aggs) {
      const auto alias = aggr.expressions[i]->GetAlias();
      aggr.expressions[i] =
        BuildKeywordTsDictAggregate(match, root, context, alias);
    }
    converted = true;
  }

  const auto only_minmax = absl::c_all_of(aggr.expressions, IsMinMaxColumnAgg);
  if (converted && !only_minmax && !aggr.children.empty()) {
    auto injected = InjectTsDictGroupBy(
      std::move(aggr.children[0]),
      {aggr.expressions.data(), aggr.expressions.size()}, binder, context);
    aggr.children[0] = std::move(injected);
  }
}

namespace {

// Repoint every column reference that resolves to the enumerated field onto the
// term virtual column, so a single same-field predicate the filter builder
// cannot push as a term acceptor still applies as a post-filter over the
// emitted terms (no postings touched -- it filters the dictionary rows).
void RewriteFieldRefsToTerm(duckdb::unique_ptr<duckdb::Expression>& expr,
                            irs::field_id field_id,
                            connector::SereneDBScanBindData& bind_data,
                            duckdb::LogicalGet& get,
                            duckdb::idx_t term_get_col_idx) {
  WalkColumnRefs(expr, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
    auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
    if (col_id != catalog::Column::kInvalidId &&
        static_cast<irs::field_id>(col_id) == field_id) {
      ref_expr = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        duckdb::Identifier{
          TsDictColName(bind_data, field_id, TsDictColKind::Term)},
        duckdb::LogicalType::VARCHAR,
        duckdb::ColumnBinding{get.table_index,
                              duckdb::ProjectionIndex{term_get_col_idx}});
    }
  });
}

// True if `filter` is a single term-acceptor leaf (ByTerm / ByTerms / ByPrefix
// / ByRange / Levenshtein / Automaton) constraining `field`, i.e. a shape the
// term-dict scan can enumerate directly. null, And/Or/Not, other-field leaves
// and ByTerms with min_match != 1 (a shape the enumeration cannot
// express) return false.
template<typename... Fs>
bool IsAcceptorOn(const irs::Filter& filter, irs::field_id field) {
  const auto type = filter.type();
  return ((type == irs::Type<Fs>::id() &&
           basics::downCast<Fs>(filter).field_id() == field) ||
          ...);
}

bool IsSingleAcceptor(const irs::Filter* filter, irs::field_id field) {
  if (!filter) {
    return false;
  }
  if (filter->type() == irs::Type<irs::ByTerms>::id()) {
    const auto& terms = basics::downCast<irs::ByTerms>(*filter);
    return terms.field_id() == field && terms.options().min_match == 1;
  }
  return IsAcceptorOn<irs::ByTerm, irs::ByPrefix, irs::ByRange,
                      irs::LevenshteinAutomatonFilter, irs::AutomatonFilter>(
    *filter, field);
}

bool IsAcceptorTreeOn(irs::Filter& filter, irs::field_id field) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id() ||
      type == irs::Type<irs::Empty>::id()) {
    return true;
  }
  if (type == irs::Type<irs::Exclusion>::id()) {
    auto& exclusion = basics::downCast<irs::Exclusion>(filter);
    const auto& include = exclusion.GetInclude();
    if (include && !IsAcceptorTreeOn(*include, field)) {
      return false;
    }
    const auto excludes = exclusion.GetExcludes();
    if (!include && excludes.empty()) {
      return false;
    }
    return absl::c_all_of(excludes, [&](const irs::Filter::ptr& child) {
      return child && IsAcceptorTreeOn(*child, field);
    });
  }
  if (type == irs::Type<irs::And>::id() || type == irs::Type<irs::Or>::id() ||
      type == irs::Type<irs::Not>::id()) {
    const auto children = filter.GetChildren();
    if (children.empty()) {
      return false;
    }
    return absl::c_all_of(children, [&](const irs::Filter::ptr& child) {
      return child && IsAcceptorTreeOn(*child, field);
    });
  }
  return IsSingleAcceptor(&filter, field);
}

[[noreturn]] void ThrowUnclaimableTsDictConjunct(size_t nfields) {
  if (nfields > 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("multi-field ts_dict_agg() WHERE accepts indexed "
              "document predicates and keyword term acceptors only"),
      ERR_HINT("scalar term conditions are not supported here; "
               "filter in an outer query over "
               "unnest(ts_dict_agg(...))"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("ts_dict_agg() WHERE accepts indexed document predicates "
            "(@@ ts_*, =, IN, LIKE, BETWEEN over indexed columns) "
            "plus scalar post-filters over the enumerated field's "
            "terms"),
    ERR_HINT("this predicate can be neither claimed by the index nor "
             "evaluated over term rows; filter in an outer query "
             "over unnest(ts_dict_agg(...))"));
}

irs::Filter::ptr ClaimOptimizedConjunct(
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  containers::FlatHashSet<irs::field_id>& analyzed_fields,
  containers::FlatHashMap<irs::field_id, irs::field_id>& null_markers,
  duckdb::ClientContext& context) {
  auto one_and = std::make_unique<irs::And>();
  if (!TryClaimIResearchConjunct(*one_and, conjunct, getter, expr_getter,
                                 context)) {
    return nullptr;
  }
  irs::Filter::ptr one = std::move(one_and);
  irs::Optimize(one, {.scored = false,
                      .fuse_seekable_acceptors = true,
                      .analyzed_fields = analyzed_fields,
                      .null_markers = &null_markers});
  return one;
}

bool IsCompilableAcceptorOn(irs::Filter& filter, irs::field_id field) {
  return IsAcceptorTreeOn(filter, field) && filter.CompileTermPredicate();
}

bool ContainsNegation(irs::Filter& filter) {
  const auto type = filter.type();
  if (type == irs::Type<irs::Not>::id() ||
      type == irs::Type<irs::Exclusion>::id()) {
    return true;
  }
  const auto children = filter.GetChildren();
  return absl::c_any_of(children, [](const irs::Filter::ptr& child) {
    return child && ContainsNegation(*child);
  });
}

// Dry-runs every WHERE conjunct: it must reference the scan, claim into
// the index, and (in multi-key mode) route to exactly one non-nullable
// enumerated field as a seekable acceptor. A single-key conjunct on the
// key's own terms suppresses the synthesized NULL group, which is only
// sound when the key is NOT NULL anyway.
bool TsDictFacetPushdown::WhereOk() {
  if (!_where) {
    return true;
  }
  bool term_conjunct = false;
  containers::FlatHashMap<irs::field_id, size_t> key_by_field;
  key_by_field.reserve(_keys.size());
  for (size_t k = 0; k < _keys.size(); ++k) {
    key_by_field[_keys[k].field_id] = k;
  }
  const bool claimable = WithSearchGetters(
    *_found.get, *_found.bind_data, *_index, _snapshot, _context,
    [&](const SearchGetters& getters) {
      auto& [getter, expr_getter, analyzed_fields, null_markers] = getters;
      size_t computed_residuals = 0;
      for (auto& expr : _where->expressions) {
        if (expr->HasParameter()) {
          return false;
        }
        EnumFieldRefs refs;
        CollectEnumFieldRefs(*expr, key_by_field, *_found.bind_data,
                             *_found.get, refs);
        if (!refs.any_ref) {
          return false;
        }
        if (!refs.matched_key && !refs.term_virtual && !refs.multi_enum &&
            IsCoveredColumnResidual(*expr, *_found.bind_data, *_found.get,
                                    *_index)) {
          if (ResidualComputesOverColumn(*expr) && ++computed_residuals > 1) {
            return false;
          }
          continue;
        }
        bool need_acceptor = false;
        auto acceptor_field = _keys.front().field_id;
        if (_keys.size() > 1) {
          if (refs.term_virtual || refs.multi_enum) {
            return false;
          }
          if (refs.matched_key) {
            need_acceptor = !refs.non_enum;
            if (need_acceptor && _keys[*refs.matched_key].nullable()) {
              return false;
            }
            acceptor_field = _keys[*refs.matched_key].field_id;
          }
        } else if ((refs.matched_key || refs.term_virtual) && !refs.non_enum) {
          term_conjunct = true;
          if (!ContainsTSQueryExpr(*expr) &&
              !connector::ContainsIndexOnlyPredicate(*expr)) {
            continue;
          }
          need_acceptor = true;
        }
        auto one = ClaimOptimizedConjunct(
          expr, getter, expr_getter, analyzed_fields, null_markers, _context);
        if (!one) {
          return false;
        }
        if (need_acceptor && !IsCompilableAcceptorOn(*one, acceptor_field)) {
          return false;
        }
      }
      return true;
    });
  return claimable && (NotNull() || !term_conjunct);
}

// ts_dict_agg() WHERE accepts predicates on the enumerated fields; each
// claimable conjunct must sit on exactly one of them and is routed to that
// field's request. The
// claimable conjuncts are fused and, when the optimizer folds them into ONE
// term acceptor, that acceptor drives the enumeration. Otherwise the most
// selective individually-claimable acceptor (ByTerm > ByTerms > ByPrefix /
// ByRange > automatons) drives and every other claimable conjunct -- single
// acceptors and boolean trees over them alike -- compiles into a term
// predicate checked per emitted term inside the scan; with no driver at all
// the full dictionary is enumerated under the predicates. Scalar same-field
// conjuncts post-filter the emitted term column (single-field mode only: on
// multi-field rows the other fields' term columns are NULL, so a scalar
// residue would silently drop them). Cross-field references and
// non-compilable @@ shapes are rejected. Having filters commit before the
// mandatory re-claim validates; a re-claim failure aborts the query, so a
// partially-claimed scan never executes.
// The four execution modes of the where/having model: term-level
// conjuncts drive or predicate the enumeration (having side), the rest
// restrict documents (where side, executed once per segment) or
// post-filter term rows.
enum class TsDictRoute {
  HavingFast,  // term-level acceptor tree -> having_filter
  Where,       // document filter -> where proxy
  PostFilter,  // term-level scalar over the emitted term rows
  Rejected,
};

// Routes every pushed-down conjunct of a ts_dict scan. Claim() runs the
// phases in order: route each conjunct (term-level claims into a
// per-field having conjunction, document-level into the shared where),
// validate the residuals (single-field scalars post-filter, anything
// else throws), fuse the having trees, re-claim keyword-source having
// conjuncts into where (fusion by origin), install the where proxy,
// rewrite post-filter refs onto the term column, and drop the consumed
// conjuncts.
class TsDictFilterClaim {
 public:
  TsDictFilterClaim(
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
    duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
    connector::SereneDBScanBindData& ss, const catalog::InvertedIndex& index,
    const std::shared_ptr<const catalog::Snapshot>& snapshot,
    duckdb::ClientContext& context, const SearchGetters& getters)
    : _filters{filters},
      _get{get},
      _bind_data{bind_data},
      _ss{ss},
      _index{index},
      _snapshot{snapshot},
      _context{context},
      _getters{getters},
      _term_getter{[this](const duckdb::BoundColumnRefExpression& ref) {
        if (auto info = TermVirtualInfo(ref)) {
          return std::optional{std::move(*info)};
        }
        return RawTerms(_getters.getter(ref));
      }},
      _term_expr_getter{[this](const duckdb::Expression& expr) {
        return RawTerms(_getters.expr_getter(expr));
      }},
      _routes(filters.size(), TsDictRoute::Rejected),
      _multi_term(filters.size(), false),
      _row_origin(filters.size(), false),
      _having_and(ss.ts_dicts.size()),
      _where_and{std::make_unique<irs::And>()} {}

  void Claim() {
    RouteConjuncts();
    ValidateResiduals();
    FuseHaving();
    ReclaimIntoWhere();
    InstallWhere();
    RewritePostFilters();
    DropConsumed();
  }

 private:
  size_t EnumeratedFieldCount() const { return _ss.ts_dicts.size(); }

  bool Enumerated(irs::field_id field) const {
    return absl::c_any_of(
      _ss.ts_dicts, [&](const auto& req) { return req.field_id == field; });
  }

  void Optimize(irs::Filter::ptr& f, bool fuse_intersections = false) const {
    irs::Optimize(f, {.scored = false,
                      .fuse_seekable_acceptors = true,
                      .fuse_acceptor_intersections = fuse_intersections,
                      .analyzed_fields = _getters.analyzed_fields,
                      .null_markers = &_getters.null_markers});
  }

  // Comparisons over the enumerated column are the explicit ts_dict term
  // surface: they match TERMS (raw tokens) and bypass the doc-level
  // analyzer guards. Everything else on a tokenized field (@@ ts_*,
  // match sugar) stays document-level. Term-virtual refs (outer filters
  // pushed onto the term column) are term-level by construction.
  bool RefsTermLevel(const duckdb::Expression& expr, bool& source_analyzed,
                     bool& multi_tok, bool& source_ref) const {
    if (expr.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
      const auto col_id = ResolveColumnId(ref.Binding(), _bind_data, _get);
      if (col_id == catalog::Column::kInvertedIndexTermId) {
        if (const auto term_ref = ClassifyTsDictGetCol(
              _ss, ref.Binding().column_index.GetIndex())) {
          multi_tok |= !_index.IsKeywordField(
            *_snapshot, _ss.ts_dicts[term_ref->req_index].field_id);
        }
        return true;
      }
      const auto field = static_cast<irs::field_id>(col_id);
      if (col_id == catalog::Column::kInvalidId || !Enumerated(field)) {
        return false;
      }
      const auto type = _bind_data.ColumnTypeById(col_id).id();
      const bool analyzed = !_index.IsKeywordField(*_snapshot, field) ||
                            type == duckdb::LogicalTypeId::LIST ||
                            type == duckdb::LogicalTypeId::ARRAY;
      source_analyzed |= analyzed;
      multi_tok |= analyzed;
      source_ref = true;
      return true;
    }
    bool term_level = true;
    duckdb::ExpressionIterator::EnumerateChildren(
      expr, [&](const duckdb::Expression& child) {
        term_level = term_level && RefsTermLevel(child, source_analyzed,
                                                 multi_tok, source_ref);
      });
    return term_level;
  }

  std::optional<connector::SearchColumnInfo> RawTerms(
    std::optional<connector::SearchColumnInfo> info) const {
    if (info && Enumerated(info->field_id) && info->tokenizer.analyzer &&
        info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      info->tokenizer.analyzer =
        catalog::Tokenizer::TokenizerWrapper{new irs::StringTokenizer(), {}};
    }
    return info;
  }

  std::optional<connector::SearchColumnInfo> TermVirtualInfo(
    const duckdb::BoundColumnRefExpression& ref) const {
    if (ref.Binding().table_index != _get.table_index ||
        ResolveColumnId(ref.Binding(), _bind_data, _get) !=
          catalog::Column::kInvertedIndexTermId) {
      return std::nullopt;
    }
    const auto term_ref =
      ClassifyTsDictGetCol(_ss, ref.Binding().column_index.GetIndex());
    if (!term_ref) {
      return std::nullopt;
    }
    const auto field = _ss.ts_dicts[term_ref->req_index].field_id;
    auto info = MakeSearchColumnInfo(
      field, _index.FindEntry(field), duckdb::LogicalType::VARCHAR,
      {.analyzer =
         catalog::Tokenizer::TokenizerWrapper{new irs::StringTokenizer(), {}}});
    info.null_field_id = irs::field_limits::invalid();
    return info;
  }

  irs::Filter::ptr ClaimConjunct(
    duckdb::unique_ptr<duckdb::Expression>& conjunct,
    const connector::ColumnGetter& col_getter,
    const connector::ExpressionGetter& e_getter) const {
    return ClaimOptimizedConjunct(conjunct, col_getter, e_getter,
                                  _getters.analyzed_fields,
                                  _getters.null_markers, _context);
  }

  void RouteConjuncts() {
    for (size_t i = 0; i < _filters.size(); ++i) {
      if (_filters[i]->HasParameter()) {
        continue;
      }
      bool source_analyzed = false;
      bool multi_tok = false;
      bool source_ref = false;
      const bool term_level =
        RefsTermLevel(*_filters[i], source_analyzed, multi_tok, source_ref) &&
        (!source_analyzed || connector::IsStrictComparisonShape(*_filters[i]));
      _multi_term[i] = multi_tok;
      _row_origin[i] = source_ref;
      if (!term_level) {
        if (auto one = ClaimConjunct(_filters[i], _getters.getter,
                                     _getters.expr_getter)) {
          _where_and->add(std::move(one));
          _routes[i] = TsDictRoute::Where;
        }
        continue;
      }
      auto one = ClaimConjunct(_filters[i], _term_getter, _term_expr_getter);
      if (!one) {
        continue;
      }
      for (size_t f = 0; f < EnumeratedFieldCount(); ++f) {
        if (IsCompilableAcceptorOn(*one, _ss.ts_dicts[f].field_id)) {
          if (!_having_and[f]) {
            _having_and[f] = std::make_unique<irs::And>();
          }
          _having_and[f]->add(std::move(one));
          _routes[i] = TsDictRoute::HavingFast;
          break;
        }
      }
    }
  }

  void ValidateResiduals() {
    containers::FlatHashMap<irs::field_id, size_t> key_by_field;
    if (EnumeratedFieldCount() == 1) {
      key_by_field.emplace(_ss.ts_dicts.front().field_id, 0);
    }
    for (size_t i = 0; i < _filters.size(); ++i) {
      if (_routes[i] != TsDictRoute::Rejected || _filters[i]->HasParameter()) {
        continue;
      }
      if (IsCoveredColumnResidual(*_filters[i], _bind_data, _get, _index)) {
        continue;
      }
      EnumFieldRefs refs;
      if (EnumeratedFieldCount() == 1) {
        CollectEnumFieldRefs(*_filters[i], key_by_field, _bind_data, _get,
                             refs);
      }
      if (!refs.OnlyField() || ContainsTSQueryExpr(*_filters[i]) ||
          connector::ContainsIndexOnlyPredicate(*_filters[i])) {
        ThrowUnclaimableTsDictConjunct(EnumeratedFieldCount());
      }
      _routes[i] = TsDictRoute::PostFilter;
    }
  }

  void FuseHaving() {
    for (size_t f = 0; f < EnumeratedFieldCount(); ++f) {
      if (!_having_and[f] || _having_and[f]->empty()) {
        continue;
      }
      irs::Filter::ptr fused = std::move(_having_and[f]);
      Optimize(fused, true);
      if (fused->type() == irs::Type<irs::And>::id()) {
        auto& children = basics::downCast<irs::And>(*fused).mutable_filters();
        std::stable_sort(children.begin(), children.end(),
                         [](const auto& lhs, const auto& rhs) {
                           return irs::optimizer::AcceptorRank(*lhs) <
                                  irs::optimizer::AcceptorRank(*rhs);
                         });
      }
      _ss.ts_dicts[f].having_filter = std::move(fused);
    }
  }

  // A keyword-source conjunct means the row's VALUE (term == value), so
  // with several enumerated fields it MUST also restrict the other
  // fields' documents: its having claim is re-claimed into the shared
  // where filter unconditionally (unfusable -> unservable). Analyzed
  // term-surface conjuncts and term-virtual conjuncts are per-field
  // conditions and never cross fields. With one field the re-claim is a
  // pure optimization (counts per accepted term are unchanged by
  // construction) and only pays off when a where filter already exists.
  // Negations on multi-token fields (analyzed or LIST/ARRAY) never fuse:
  // their doc-level claim means "no token matches", which drops
  // documents holding an accepted token next to a rejected one.
  void ReclaimIntoWhere() {
    const bool had_where = !_where_and->empty();
    for (size_t i = 0; i < _filters.size(); ++i) {
      if (_routes[i] != TsDictRoute::HavingFast) {
        continue;
      }
      const bool mandatory =
        EnumeratedFieldCount() > 1 && _row_origin[i] && !_multi_term[i];
      if (EnumeratedFieldCount() > 1 && !mandatory) {
        continue;
      }
      if (!mandatory && !had_where) {
        continue;
      }
      auto one = ClaimConjunct(_filters[i], _term_getter, _term_expr_getter);
      if (!one || (_multi_term[i] && ContainsNegation(*one))) {
        if (mandatory) {
          ThrowUnclaimableTsDictConjunct(EnumeratedFieldCount());
        }
        continue;
      }
      _where_and->add(std::move(one));
    }
  }

  void InstallWhere() {
    if (_where_and->empty()) {
      return;
    }
    irs::Filter::ptr doc = std::move(_where_and);
    Optimize(doc);
    auto proxy = std::make_shared<irs::ProxyFilter>();
    proxy->set_filter(irs::IResourceManager::gNoop, std::move(doc));
    _ss.stored_filter = std::move(proxy);
  }

  void RewritePostFilters() {
    if (!absl::c_linear_search(_routes, TsDictRoute::PostFilter)) {
      return;
    }
    auto& req = _ss.ts_dicts.front();
    EnsureTsDictCol(_bind_data, _get, req, TsDictColKind::Term);
    req.term_uses |= connector::TsDictTermUses::kFull;
    for (size_t i = 0; i < _filters.size(); ++i) {
      if (_routes[i] == TsDictRoute::PostFilter) {
        RewriteFieldRefsToTerm(_filters[i], req.field_id, _bind_data, _get,
                               req.term_col_idx);
      }
    }
  }

  void DropConsumed() {
    size_t out = 0;
    for (size_t i = 0; i < _filters.size(); ++i) {
      const bool drop = _routes[i] == TsDictRoute::HavingFast ||
                        _routes[i] == TsDictRoute::Where;
      if (!drop) {
        if (out != i) {
          _filters[out] = std::move(_filters[i]);
        }
        ++out;
      }
    }
    _filters.erase(_filters.begin() + out, _filters.end());
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& _filters;
  duckdb::LogicalGet& _get;
  connector::SereneDBScanBindData& _bind_data;
  connector::SereneDBScanBindData& _ss;
  const catalog::InvertedIndex& _index;
  const std::shared_ptr<const catalog::Snapshot>& _snapshot;
  duckdb::ClientContext& _context;
  const SearchGetters& _getters;
  connector::ColumnGetter _term_getter;
  connector::ExpressionGetter _term_expr_getter;
  std::vector<TsDictRoute> _routes;
  std::vector<bool> _multi_term;
  std::vector<bool> _row_origin;
  std::vector<std::unique_ptr<irs::And>> _having_and;
  std::unique_ptr<irs::And> _where_and;
};

}  // namespace

void ClaimTsDictFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  connector::SereneDBScanBindData& ss, const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  duckdb::ClientContext& context) {
  WithSearchGetters(get, bind_data, index, snapshot, context,
                    [&](const SearchGetters& getters) {
                      TsDictFilterClaim{filters, get,      bind_data, ss,
                                        index,   snapshot, context,   getters}
                        .Claim();
                      return true;
                    });
}

}  // namespace sdb::optimizer
