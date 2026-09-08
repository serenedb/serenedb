////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/status/status.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <optional>
#include <span>

#include "basics/containers/flat_hash_map.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"

namespace sdb::connector {

// `field_id` is the unified iresearch field id: both a plain indexed column's
// id (`catalog::ColumnId`) and an indexed expression's id come from
// `catalog::NextId()` / `NextNIds()` (single global tick allocator), so a
// single uint64 fits both. Disambiguate via catalog lookup when the kind
// matters; the writer/printer paths don't need to.
struct SearchColumnInfo {
  irs::field_id field_id = irs::field_limits::invalid();
  // Valid iff NULL rows can exist here: producers invalidate it when a table
  // constraint proves the column NOT NULL (or the surface has no NULLs, e.g.
  // term columns). Negation claims exclude it so SQL three-valued logic
  // holds; IS NULL claims match it; invalid keeps negations plain acceptor
  // shapes and declines IS NULL claims.
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();
  duckdb::LogicalType logical_type;
  catalog::ColumnTokenizer tokenizer;
  std::optional<uint32_t> levenshtein_max_terms;
};

// Resolves a DuckDB bound column reference (by table_index + column_index,
// the same information the filter combiner will pass through) to a
// SearchColumnInfo. Returns nullopt if the reference does not belong to
// the inverted-index-backed scan the caller is building a filter for, or
// the column is not part of the index. Caller owns the concrete
// implementation (typically captures bind data + InvertedIndex).
using ColumnGetter = absl::AnyInvocable<std::optional<SearchColumnInfo>(
  const duckdb::BoundColumnRefExpression&) const>;

using ExpressionGetter = absl::AnyInvocable<std::optional<SearchColumnInfo>(
  const duckdb::Expression&) const>;

// Builds iresearch filters into `root`'s `Must` bucket from an implicit-AND
// list of DuckDB bound filter expressions (as found in a LogicalFilter). Each
// expression either becomes a clause of `root` (on success) or causes
// MakeSearchFilter to throw (leaving `root` in an unspecified but still
// safely-destructible state -- caller should discard it on failure).
//
// The ClientContext is required (reference, not pointer): the filter
// builder needs it to resolve named catalog analyzers at filter-build
// time (`TOKENIZE(text, 'english')` whose stub never runs) and to read
// session settings.
// A non-ok status means "the index cannot claim this predicate" and carries
// the reason; the optimizer treats it as "decline, fall back" (`root` may
// hold partially-added children the caller must roll back), ts_offsets
// surfaces it as a SQL error. Genuine user errors under index-only syntax
// (`@@`, ts_*, geo, ::boost) throw SqlException at origin instead.
using FilterScorers = std::vector<std::shared_ptr<irs::Scorer>>;

// Where a produced clause goes: the node that will hold it, and which of its
// three buckets. A conjunction is a node filled through `Must`, a disjunction
// one filled through `Should`; nothing else distinguishes them, so this pair
// is what every producer takes in place of a boolean node alone.
struct BoolTarget {
  irs::BooleanFilter* node = nullptr;
  irs::Occur occur = irs::Occur::Must;

  void Add(irs::Filter::ptr filter) const {
    node->Add(std::move(filter), occur);
  }
  void Add(irs::TermClause clause) const {
    node->Add(std::move(clause), occur);
  }
};

// A `ByTerm` is absorbed into the term bucket on insertion, so the node keeps
// no filter to hand back and a reference to one would dangle. Such a leaf is
// built whole and handed over through `BoolTarget::Add` instead.
template<typename Filter, typename... Args>
Filter& AddFilter(BoolTarget parent, Args&&... args) {
  static_assert(!std::is_same_v<Filter, irs::ByTerm>);
  auto filter = std::make_unique<Filter>(std::forward<Args>(args)...);
  auto& ref = *filter;
  parent.Add(std::move(filter));
  return ref;
}

// A term clause, which the node stores rather than the `ByTerm` it came as.
inline void AddTerm(BoolTarget parent, irs::field_id field,
                    irs::bytes_view term, irs::score_t boost = irs::kNoBoost,
                    const irs::Scorer* scorer = nullptr) {
  parent.Add(irs::TermClause{
    .field = field,
    .scorer = scorer,
    .term = irs::bstring{term},
    .boost = boost,
  });
}

// How many of the `Should` bucket a document needs. Set once the bucket is
// full, because the count is a count of clauses; a threshold larger than the
// bucket can never be reached, which the model spells as an unsatisfiable
// required clause rather than as a smaller threshold.
inline void SetMinMatch(irs::BooleanFilter& node, size_t min_match) {
  const auto size = node.Size(irs::Occur::Should);
  if (min_match > size) {
    node.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
    min_match = size;
  }
  node.SetMinShouldMatch(static_cast<uint32_t>(min_match));
}

// A nested boolean whose clauses go into `occur`. `Should` groups are closed
// with `SetMinMatch` once filled.
inline BoolTarget AddGroup(BoolTarget parent, irs::Occur occur) {
  return {&AddFilter<irs::BooleanFilter>(parent), occur};
}

void AddNullMarkerTerm(BoolTarget parent, irs::field_id null_field_id);

// `ByTerms{field, terms, mm=k}`: one node holding the terms of a single
// field. Every term required is the `Must` bucket and no threshold at all;
// anything less is `Should` counted to `k`.
irs::BooleanFilter& AddTermSet(BoolTarget parent, irs::field_id field,
                               std::span<irs::bstring> terms, size_t min_match);

// What a negated clause is added to. Under a conjunction the node's own
// `MustNot` bucket is the negation, and nothing has to be allocated for it;
// anywhere else the negation applies to a whole node, so it gets one.
inline BoolTarget Negate(BoolTarget parent) {
  if (parent.occur == irs::Occur::Must) {
    return {parent.node, irs::Occur::MustNot};
  }
  return AddGroup(parent, irs::Occur::MustNot);
}

// SQL three-valued logic: a NULL row satisfies no comparison, but a bare
// negation runs against ALL live docs and would readmit rows without a token
// in the negated column. Scoped negation excludes the column's null-marker
// docs alongside the negated set -- `must_not` is a union, so they share one
// bucket; the and_null_exclusion optimizer rule prunes the branch wherever a
// positive same-column conjunct already rejects those rows.
// A node is what it includes, less what it excludes, so nothing included is
// nothing to exclude from. SQL negation excludes from every row, which is an
// include side of `All` -- said explicitly here rather than left implicit in a
// bare `MustNot` bucket, which the model reads as matching nothing.
//
// Post-order, and only where the include side is genuinely empty: a node that
// requires something, or whose optional side its threshold reaches, already
// has one and must not gain a clause that would score.
inline void EnsureIncludeSides(irs::Filter& filter) {
  if (filter.type() != irs::Type<irs::BooleanFilter>::id()) {
    return;
  }
  auto& node = sdb::basics::downCast<irs::BooleanFilter>(filter);
  node.VisitChildren([](irs::Filter::ptr& child) {
    if (child) {
      EnsureIncludeSides(*child);
    }
  });
  if (node.Size(irs::Occur::MustNot) != 0 && node.Size(irs::Occur::Must) == 0 &&
      (node.MinShouldMatch() == 0 || node.Size(irs::Occur::Should) == 0)) {
    node.Add(std::make_unique<irs::All>(), irs::Occur::Must);
  }
}

inline BoolTarget NegateScoped(BoolTarget parent,
                               const SearchColumnInfo& info) {
  const auto target = Negate(parent);
  if (irs::field_limits::valid(info.null_field_id)) {
    AddNullMarkerTerm(target, info.null_field_id);
  }
  return target;
}

void AddNegated(BoolTarget parent, const SearchColumnInfo& info,
                irs::Filter::ptr target);

absl::Status MakeSearchFilter(
  irs::BooleanFilter& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter, duckdb::ClientContext& context,
  const ExpressionGetter& expr_getter, FilterScorers* scorers);

inline irs::field_id PickPerKindFieldId(const SearchColumnInfo& column_info,
                                        duckdb::LogicalTypeId type_id) {
  const auto pick = [&](irs::field_id per_kind) {
    return irs::field_limits::valid(per_kind) ? per_kind : column_info.field_id;
  };
  const auto kind = catalog::term_dict::Classify(type_id);
  if (kind == catalog::term_dict::Kind::Bool) {
    return pick(column_info.bool_field_id);
  }
  if (catalog::term_dict::IsNumeric(kind)) {
    return pick(column_info.numeric_field_id);
  }
  return column_info.field_id;
}

// True when the expression tree contains an optimizer-claimed index-only
// predicate (`@@` or the match sugar) that cannot run as a scalar.
bool ContainsIndexOnlyPredicate(const duckdb::Expression& expr);

// Term-surface comparison shapes: comparisons plus the scalar pattern
// functions, excluding the index-only match sugar.
bool IsStrictComparisonShape(const duckdb::Expression& expr);

}  // namespace sdb::connector
