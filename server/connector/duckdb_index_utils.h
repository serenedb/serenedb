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

enum class DuckDBWriteKind { Insert, Delete };

template<DuckDBWriteKind Kind>
std::unique_ptr<DuckDBSinkIndexWriter> CreateInvertedIndexWriter(
  ObjectId table_id, ObjectId index_id, ConnectionContext& conn_ctx,
  duckdb::optional_ptr<duckdb::ClientContext> expr_context = nullptr);

std::vector<size_t> BuildCreateIndexProjection(
  std::span<const catalog::Column> columns,
  std::span<const catalog::Column::Id> pk_column_ids,
  std::span<const duckdb::idx_t> index_column_positions);

void EvaluateAndWriteIndexedExpressions(
  DuckDBSinkIndexWriter& sink, std::span<const IndexedExpression> indexed_exprs,
  duckdb::DataChunk& chunk, ObjectId table_id,
  std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& client_context, duckdb::idx_t num_rows);

// One indexed base column of a feed chunk: its position in the chunk and the
// (column id, type) the sink needs to tokenize it.
struct FeedColumn {
  duckdb::idx_t slot;
  ColumnDescriptor desc;
};


// The single feed path shared by every caller that tokenizes a chunk into an
// inverted index (CREATE INDEX backfill, live insert, recovery replay): open
// the batch, hand each indexed column to the sink, feed the indexed
// expressions, close. `writer` already holds the target iresearch transaction.
// Indexed expressions are evaluated over the chunk and tokenized. By default
// the writer's own expressions are used; `inline_exprs`, when non-empty,
// overrides them with a caller-owned set (the live parallel feed passes the
// session's shared expressions so its workers evaluate in parallel over one
// shared context instead of per-worker connections). `expr_context` is the
// evaluation context (may be null when nothing needs evaluation).
// `commit_on_flush` reaches the online build's delete-log tracking.
void FeedChunk(DuckDBSinkIndexWriter& writer, duckdb::idx_t count,
               const PkChunk& pk, duckdb::DataChunk& chunk,
               std::span<const FeedColumn> columns, ObjectId table_id,
               std::span<const catalog::Column::Id> slot_to_col_id,
               duckdb::optional_ptr<duckdb::ClientContext> expr_context,
               std::span<const IndexedExpression> inline_exprs = {},
               uint64_t* commit_on_flush = nullptr);

}  // namespace sdb::connector
