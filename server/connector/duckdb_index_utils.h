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

#include <memory>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
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

}  // namespace sdb::connector
