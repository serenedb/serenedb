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
#include "rocksdb/utilities/transaction.h"

namespace rocksdb {

class ColumnFamilyHandle;
}

namespace sdb {

class ConnectionContext;
}

namespace sdb::connector {

// Factory: create DuckDB index writers for all indexes on a table.
//
// Writers are created once (e.g. in GetGlobalSinkState) and reused for each
// Sink() call. The WriteKind template selects Insert/Delete/Update writers.
//
// col_id_to_chunk_pos: optional override mapping Column::Id -> position in
// the input DataChunk. If empty, table column order is assumed (for INSERT).
// For DELETE/UPDATE, pass the actual positions of columns in the scan output.
//
// updated_col_ids: optional filter -- only create writers for indexes whose
// columns overlap with this set. If empty, create writers for ALL indexes.
// Used by UPDATE to skip indexes on non-updated columns.
enum class DuckDBWriteKind { Insert, Delete, Update };

using ColumnChunkMapping = containers::FlatHashMap<catalog::Column::Id, size_t>;

template<DuckDBWriteKind Kind>
std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> CreateDuckDBIndexWriters(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos = {},
  std::span<const catalog::Column::Id> updated_col_ids = {},
  const ColumnChunkMapping& old_col_id_to_chunk_pos = {});

// Explicit instantiation declarations
extern template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

extern template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Delete>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

extern template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Update>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

// Returns the chunk-order list of catalog column positions to project for a
// CREATE INDEX backfill scan over a base table:
//   1. columns the index keys on (in `index_column_positions` order, deduped),
//   2. PK columns not already included (in `pk_column_ids` order).
//
// `index_column_positions` are positions into `columns` (matching how
// CreateIndexInfo::column_ids is populated -- positional, not Column::Id).
// `pk_column_ids` are Column::Id values; the helper resolves them to
// positions internally.
//
// For tables with a generated PK the caller separately appends ROW_ID at
// chunk position equal to projection.size(). Only the base-table CREATE INDEX
// path uses this helper -- view-backed indexes get their projection from the
// view body, not from us.
std::vector<size_t> BuildCreateIndexProjection(
  std::span<const catalog::Column> columns,
  std::span<const catalog::Column::Id> pk_column_ids,
  std::span<const duckdb::idx_t> index_column_positions);

}  // namespace sdb::connector
