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

#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/search_field_name.hpp"

namespace sdb::connector {

class BooleanFilterBuilder {
 public:
  enum class Kind { And, Or };

  explicit BooleanFilterBuilder(Kind kind) noexcept : _kind{kind} {}

  irs::TypeInfo::type_id type() const noexcept {
    return _kind == Kind::And ? irs::Type<irs::And>::id()
                              : irs::Type<irs::Or>::id();
  }
  void boost(irs::score_t boost) noexcept { _boost = boost; }
  irs::score_t Boost() const noexcept { return _boost.value_or(irs::kNoBoost); }
  void min_match_count(size_t count) { _min_match = count; }

  template<typename Filter>
  Filter& AddInclude() {
    auto filter = std::make_unique<Filter>();
    auto& ref = *filter;
    _entries.push_back(Entry{.leaf = std::move(filter)});
    return ref;
  }

  template<typename Filter>
  Filter& AddExclude() {
    auto filter = std::make_unique<Filter>();
    auto& ref = *filter;
    _entries.push_back(Entry{.leaf = std::move(filter), .exclude = true});
    return ref;
  }

  irs::Filter& AddInclude(irs::Filter::ptr filter) {
    auto& ref = *filter;
    _entries.push_back(Entry{.leaf = std::move(filter)});
    return ref;
  }

  template<typename Group>
  BooleanFilterBuilder& AddIncludeGroup() {
    auto& entry = _entries.emplace_back(Entry{
      .group = std::make_unique<BooleanFilterBuilder>(GroupKind<Group>())});
    return *entry.group;
  }

  template<typename Group>
  BooleanFilterBuilder& AddExcludeGroup() {
    auto& entry = _entries.emplace_back(
      Entry{.group = std::make_unique<BooleanFilterBuilder>(GroupKind<Group>()),
            .exclude = true});
    return *entry.group;
  }

  size_t size() const noexcept { return _entries.size(); }
  void Resize(size_t count) { _entries.resize(count); }

  irs::Filter::ptr Build() {
    std::vector<irs::Filter::ptr> children;
    children.reserve(_entries.size());
    for (auto& entry : _entries) {
      irs::Filter::ptr child =
        entry.group ? entry.group->Build() : std::move(entry.leaf);
      if (entry.exclude) {
        child = std::make_unique<irs::Not>(std::move(child));
      }
      children.emplace_back(std::move(child));
    }
    if (_kind == Kind::And) {
      auto and_node = std::make_unique<irs::And>(std::move(children));
      if (_boost) {
        and_node->boost(*_boost);
      }
      return and_node;
    }
    auto or_node = std::make_unique<irs::Or>(
      std::move(children), irs::ScoreMergeType::Sum, _min_match.value_or(1));
    if (_boost) {
      or_node->boost(*_boost);
    }
    return or_node;
  }

 private:
  struct Entry {
    irs::Filter::ptr leaf;
    std::unique_ptr<BooleanFilterBuilder> group;
    bool exclude = false;
  };

  template<typename Group>
  static Kind GroupKind() noexcept {
    return std::is_same_v<Group, irs::And> ? Kind::And : Kind::Or;
  }

  Kind _kind;
  std::vector<Entry> _entries;
  std::optional<size_t> _min_match;
  std::optional<irs::score_t> _boost;
};

// `field_id` is the unified iresearch field id: both a plain indexed column's
// id (`catalog::Column::Id`) and an indexed expression's id come from
// `catalog::NextId()` / `NextNIds()` (single global tick allocator), so a
// single uint64 fits both. Disambiguate via catalog lookup when the kind
// matters; the writer/printer paths don't need to.
struct SearchColumnInfo {
  irs::field_id field_id = 0;
  duckdb::LogicalType logical_type;
  catalog::ColumnTokenizer tokenizer;
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

// Builds iresearch filters into `root` from an implicit-AND list of
// DuckDB bound filter expressions (as found in a LogicalFilter). Each
// expression either becomes a child of `root` (on success) or causes
// MakeSearchFilter to return a failure Result (leaving `root` in an
// unspecified but still safely-destructible state -- caller should discard
// it on failure).
//
// Per-query session options threaded into the filter builder. The
// optimizer pipeline reads them once from the ClientContext settings
// and forwards the same struct through its passes; tests construct
// it directly with `_conn.context` (or any owned ClientContext).
//
// The ClientContext is required (reference, not pointer): the filter
// builder needs it to resolve named catalog analyzers at filter-build
// time (`TOKENIZE(text, 'english')` whose stub never runs).
struct SearchFilterOptions {
  duckdb::ClientContext& client_context;
  // Caps the number of terms a multi-term filter (PREFIX / LIKE /
  // RANGE / REGEXP / LEVENSHTEIN) collects for scoring. Comes from
  // the `sdb_scored_terms_limit` session setting; the iresearch
  // default is 1024.
  size_t scored_terms_limit = 1024;
  // When true, the optimizer skips pulling `ORDER BY <scorer> DESC LIMIT k`
  // into the SearchScan, so WAND never engages even on indexes that have
  // wand metadata. Driven by the `sdb_disable_top_k_optimization` session
  // setting; default false (optimization on).
  bool disable_top_k_optimization = false;
};

Result MakeSearchFilter(
  BooleanFilterBuilder& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter, const SearchFilterOptions& options,
  const ExpressionGetter& expr_getter = {});

}  // namespace sdb::connector
