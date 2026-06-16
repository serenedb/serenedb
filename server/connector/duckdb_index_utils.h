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

#include <duckdb.hpp>
#include <memory>
#include <span>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/index_expression.hpp"
namespace sdb {

class ConnectionContext;
}

namespace sdb::connector {

enum class DuckDBWriteKind { Insert, Delete, Update };

// Writer for ONE inverted index, identified by id -- the store-side
// BoundIndex feeds exactly its own index. nullptr when the index is not an
// inverted index of `table_id` (e.g. concurrently dropped).
template<DuckDBWriteKind Kind>
std::unique_ptr<DuckDBSinkIndexWriter> CreateInvertedIndexWriter(
  ObjectId table_id, ObjectId index_id, ConnectionContext& conn_ctx,
  duckdb::optional_ptr<duckdb::ClientContext> expr_context = nullptr);

// Catalog column positions to project for a CREATE INDEX backfill scan:
// union of index-key columns and PK columns, sorted+deduped (== catalog
// column order). For tables with a generated PK the caller appends ROW_ID
// at chunk position equal to projection.size().
//
// `index_column_positions` are positions into `columns` (positional, not
// Column::Id). `pk_column_ids` are Column::Id values, resolved internally.
std::vector<size_t> BuildCreateIndexProjection(
  std::span<const catalog::Column> columns,
  std::span<const catalog::Column::Id> pk_column_ids,
  std::span<const duckdb::idx_t> index_column_positions);

// Evaluates each indexed expression against `chunk` and writes its result as
// a virtual column into `sink` under the original `row_keys`. Stamps each
// expression's column id into the keys, switches the writer to it, and
// emits the value via the serializer. Shared by INSERT, UPDATE, CREATE
// INDEX backfill, and WAL recovery.
void EvaluateAndWriteIndexedExpressions(
  DuckDBSinkIndexWriter& sink, std::span<const IndexedExpression> indexed_exprs,
  duckdb::DataChunk& chunk, ObjectId table_id,
  std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& client_context, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys);

}  // namespace sdb::connector
