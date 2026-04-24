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

// Info describing how to build iresearch terms for a referenced column.
// `logical_type` is the DuckDB column type (what the filter value will
// coerce to); `analyzer` is the catalog-supplied column analyzer
// (op-class determines tokenizer choice for text columns -- non-text
// columns get a null analyzer here).
struct SearchColumnInfo {
  catalog::Column::Id column_id{};
  duckdb::LogicalType logical_type;
  catalog::ColumnAnalyzer analyzer;
};

// Resolves a DuckDB bound column reference (by table_index + column_index,
// the same information the filter combiner will pass through) to a
// SearchColumnInfo. Returns nullopt if the reference does not belong to
// the inverted-index-backed scan the caller is building a filter for, or
// the column is not part of the index. Caller owns the concrete
// implementation (typically captures bind data + InvertedIndex).
using ColumnGetter = absl::AnyInvocable<std::optional<SearchColumnInfo>(
  const duckdb::BoundColumnRefExpression&) const>;

// Encodes column_id as an 8-byte big-endian binary string into
// field_name. Before being used as an iresearch field name, the result
// still needs mangling (MangleString / MangleBool / MangleNumeric /
// MangleNull) based on what the caller is querying.
void MakeFieldName(catalog::Column::Id column_id, std::string& field_name);

// Builds iresearch filters into `root` from an implicit-AND list of
// DuckDB bound filter expressions (as found in a LogicalFilter). Each
// expression either becomes a child of `root` (on success) or causes
// MakeSearchFilter to return a failure Result (leaving `root` in an
// unspecified but still safely-destructible state -- caller should discard
// it on failure).
Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter);

}  // namespace sdb::connector
