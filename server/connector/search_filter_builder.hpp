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

#include <absl/functional/function_ref.h>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <optional>
#include <span>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"

namespace sdb::connector {

// `field_id` is the unified iresearch field id: both a plain indexed column's
// id (`catalog::Column::Id`) and an indexed expression's id come from
// `catalog::NextId()` / `NextNIds()` (single global tick allocator), so a
// single uint64 fits both. Disambiguate via catalog lookup when the kind
// matters; the writer/printer paths don't need to.
struct SearchColumnInfo {
  irs::field_id field_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();
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
// The ClientContext is required (reference, not pointer): the filter
// builder needs it to resolve named catalog analyzers at filter-build
// time (`TOKENIZE(text, 'english')` whose stub never runs) and to read
// the `sdb_scored_terms_limit` session setting.
Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter, duckdb::ClientContext& context,
  const ExpressionGetter& expr_getter = {});

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

}  // namespace sdb::connector
