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

#pragma once

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/planner/expression.hpp>
#include <memory>
#include <span>
#include <string>

#include "catalog/table_options.h"

namespace sdb::connector {

// Per-row writer's view of one configured JSON-extract expression.
// Owned elsewhere; this is a non-owning bundle the caller pulls per chunk.
struct JsonExpressionEval {
  const duckdb::Expression* bound_expr;
  std::string_view serialized;
  catalog::Column::Id column_id;
};

// Walks a JSON-extract chain and validates every step (JSON-extract function
// with a non-null constant key bottoming out at a BoundColumnRef). Returns
// the leaf column ref on success, nullptr if the expression isn't a valid
// JSON path.
const duckdb::BoundColumnRefExpression* TryGetJsonLeafColumnRef(
  const duckdb::Expression& expr);

// Serialise a bound expression to an opaque byte buffer suitable for
// catalog persistence and cross-context byte-comparison. The caller is
// responsible for passing a `NormalizeBoundExpression`-normalised tree
// when the bytes will be compared across binding contexts.
std::string SerializeBoundExpression(const duckdb::Expression& expr);

std::string SerializeParsedExpression(const duckdb::ParsedExpression& expr);

// Reverse of `SerializeBoundExpression`. Requires a ClientContext because
// DuckDB's Expression::Deserialize resolves catalog references (e.g. scalar
// functions) against the live database state.
duckdb::unique_ptr<duckdb::Expression> DeserializeBoundExpression(
  std::string_view bytes, duckdb::ClientContext& context);

// Pins every BoundColumnRefExpression in `expr` to TableIndex(0) (column_index
// untouched). Produces a canonical, chunk-layout-independent form that any
// consumer can resolve via `ResolveBoundColumnRefsForChunk` against its own
// DataChunk. Called once at CREATE INDEX time before serialising for catalog
// persistence; the deserialised form on the read side is already normalised.
void NormalizeBoundColumnRefs(duckdb::Expression& expr);

// Returns a copy of `expr` whose binder-state noise has been replaced with
// stable catalog identities so `SerializeBoundExpression` of the result
// produces byte-identical output across binding contexts (CREATE INDEX vs
// SELECT vs INSERT etc.). Specifically:
//   * Every node has its `alias` cleared and `query_location` reset.
//   * BoundFunctionExpression::is_operator is forced to `false`
//     (`->>` parses as operator on one side, function on the other).
//   * Every BoundColumnRefExpression's `binding` is rewritten to encode
//     stable catalog ids: `table_index` <- `table_id.id()`,
//     `column_index` <- `col_index_to_id[old_column_index]` so the same
//     base-table column produces the same bytes regardless of which
//     binder allocated the original `(table_index, column_index)` pair.
//
// `col_index_to_id` MUST cover every `column_index` referenced by `expr`'s
// leaves; the function asserts in debug if a leaf points past the end.
duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, ObjectId table_id,
  std::span<const catalog::Column::Id> col_index_to_id);

// Returns a copy of `expr` whose BoundColumnRefExpression leaves have been
// rewritten to BoundReferenceExpression(slot, type) so the expression can
// be fed directly to ExpressionExecutor against `chunk`.
//
// The caller passes:
//   * `table_id`         -- base relation's ObjectId. Must match the
//                           `TableIndex` baked into the normalised leaves.
//   * `slot_to_col_id`   -- chunk-slot -> Column::Id map. The resolver
//                           builds reverse bindings so a normalised leaf
//                           `BoundColumnRef(TableIndex(table_id),
//                            ProjectionIndex(col_id))` is replaced with
//                           `BoundReferenceExpression(slot,
//                           chunk_types[slot])`.
duckdb::unique_ptr<duckdb::Expression> ResolveBoundColumnRefsForChunk(
  const duckdb::Expression& expr, const duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id);

// Rejects an INSERT/UPDATE/backfill row when its JSON-extract result is the
// stringified form of an object or array (`{...}` / `[...]`). DuckDB's
// json_extract_string emits objects and arrays verbatim as JSON text; the
// inverted index requires primitive leaves, so we surface a hard error
// instead of silently tokenising the JSON braces as text. Expects `result`
// to be a VARCHAR vector with cardinality `num_rows`.
void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows);

// One-shot helper used by INSERT, UPDATE, CREATE INDEX backfill, and WAL
// recovery: resolves the catalog-keyed `bound_expr` to chunk-relative slots
// (via ResolveBoundColumnRefsForChunk), evaluates it with ExpressionExecutor
// against `chunk`, and rejects object/array leaves. The returned Vector has
// the resolved expression's return type and cardinality `chunk.size()`.
duckdb::Vector EvaluateJsonPathOverChunk(
  const duckdb::Expression& bound_expr, duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& context);

// Walks a bound JSON-extract chain (post-`ResolveBoundColumnRefsForChunk`
// or pre-resolution) and returns the chunk slot of the source JSON column
// it ultimately references. Returns `idx_t(-1)` if the leaf isn't a
// reference / column-ref expression.
duckdb::idx_t ExtractJsonSourceColId(const duckdb::Expression& expr);

// Per-row simdjson navigation of `source_json` along `path_keys`. Sets
// `out_mask[i] = true` for any row whose path traversal stops short of the
// leaf (key absent / array index out of range / wrong shape mid-chain).
// Rows whose path resolves successfully -- including to an explicit JSON
// null -- get `out_mask[i] = false`. Caller uses this to skip "missing"
// rows entirely so IS NULL doesn't false-match them.
void ComputeJsonMissingMask(const duckdb::Vector& source_json,
                            duckdb::idx_t num_rows,
                            std::span<const std::string> path_keys,
                            std::vector<bool>& out_mask);

}  // namespace sdb::connector
