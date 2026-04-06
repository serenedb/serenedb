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

#include "connector/duckdb_index_utils.h"

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/duckdb_secondary_sink_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/utilities/transaction.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {

namespace {

// Build SK column mappings: for each index column, find the corresponding
// position in the input DataChunk and its DuckDB type.
// If col_id_to_chunk_pos is empty, use table column order (for INSERT).
// Otherwise, use the provided mapping (for DELETE/UPDATE scan output).
std::vector<duckdb_secondary_key::SKColumn> BuildSKColumns(
  const catalog::Index& index, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos) {
  const auto& columns = table.Columns();
  std::vector<duckdb_secondary_key::SKColumn> result;
  result.reserve(index.GetColumnIds().size());

  for (auto col_id : index.GetColumnIds()) {
    duckdb::LogicalType type;
    size_t input_idx = 0;
    bool found = false;

    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == col_id) {
        type = VeloxTypeToDuckDB(columns[i].type);
        if (!col_id_to_chunk_pos.empty()) {
          auto it = col_id_to_chunk_pos.find(col_id);
          SDB_ASSERT(it != col_id_to_chunk_pos.end(),
                     "Index column not found in chunk mapping");
          input_idx = it->second;
        } else {
          input_idx = i;  // table column order
        }
        found = true;
        break;
      }
    }
    SDB_ASSERT(found, "Index column not found in table");
    result.push_back(duckdb_secondary_key::SKColumn{
      .input_col_idx = input_idx,
      .type = type,
    });
  }
  return result;
}

// --- Factory helpers for each WriteKind ---

template<bool Unique>
std::unique_ptr<DuckDBSinkIndexWriter> MakeDuckDBSecondaryWriter(
  DuckDBWriteKind kind, rocksdb::Transaction& trx, ObjectId shard_id,
  std::span<const catalog::Column::Id> columns,
  std::vector<duckdb_secondary_key::SKColumn> sk_columns) {
  switch (kind) {
    case DuckDBWriteKind::Insert:
      return std::make_unique<DuckDBSecondarySinkInsertWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns));
    case DuckDBWriteKind::Delete:
      return std::make_unique<DuckDBSecondarySinkDeleteWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns));
    case DuckDBWriteKind::Update:
      // For update, old_sk_columns = sk_columns (same mapping for now)
      auto old_sk = sk_columns;
      return std::make_unique<DuckDBSecondarySinkUpdateWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns), std::move(old_sk));
  }
  SDB_ASSERT(false, "Unknown DuckDBWriteKind");
  return nullptr;
}

std::unique_ptr<DuckDBSinkIndexWriter> MakeDuckDBSearchWriter(
  DuckDBWriteKind kind, irs::IndexWriter::Transaction& trx,
  AnalyzerProvider&& analyzer_provider,
  std::span<const catalog::Column::Id> columns) {
  switch (kind) {
    case DuckDBWriteKind::Insert:
      return std::make_unique<DuckDBSearchSinkInsertWriter>(
        trx, std::move(analyzer_provider), columns);
    case DuckDBWriteKind::Delete:
      return std::make_unique<DuckDBSearchSinkDeleteWriter>(trx);
    case DuckDBWriteKind::Update:
      return std::make_unique<DuckDBSearchSinkUpdateWriter>(
        trx, std::move(analyzer_provider), columns);
  }
  SDB_ASSERT(false, "Unknown DuckDBWriteKind");
  return nullptr;
}

}  // namespace

template<DuckDBWriteKind Kind>
std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> CreateDuckDBIndexWriters(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids) {
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> writers;
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  // Build filter: only include indexes whose columns overlap with updated set.
  // If updated_col_ids is empty, include all indexes (INSERT/DELETE).
  auto visitor = [&](auto& index_txn, const catalog::Index& index) {
    using TxnType = std::decay_t<decltype(index_txn)>;

    if constexpr (std::is_same_v<TxnType, rocksdb::Transaction>) {
      // Secondary index
      auto& sec_index = basics::downCast<const catalog::SecondaryIndex>(index);

      // Find shard ID
      ObjectId shard_id;
      for (auto& shard : snapshot->GetIndexShardsByTable(table_id)) {
        if (shard->GetIndexId() == index.GetId()) {
          shard_id = shard->GetId();
          break;
        }
      }

      auto sk_columns = BuildSKColumns(index, table, col_id_to_chunk_pos);

      if (sec_index.IsUnique()) {
        writers.push_back(MakeDuckDBSecondaryWriter<true>(
          Kind, index_txn, shard_id, index.GetColumnIds(),
          std::move(sk_columns)));
      } else {
        writers.push_back(MakeDuckDBSecondaryWriter<false>(
          Kind, index_txn, shard_id, index.GetColumnIds(),
          std::move(sk_columns)));
      }
    } else {
      // Inverted/search index
      auto& inverted_index =
        basics::downCast<const catalog::InvertedIndex>(index);
      auto analyzer_provider = MakeAnalyzerProvider(snapshot, inverted_index);

      writers.push_back(MakeDuckDBSearchWriter(
        Kind, index_txn, std::move(analyzer_provider), index.GetColumnIds()));
    }
  };

  if (updated_col_ids.empty()) {
    // No filter — create writers for all indexes (INSERT/DELETE)
    conn_ctx.EnsureIndexesTransactions(table_id, visitor);
  } else {
    // Filter: only indexes whose columns overlap with updated set (UPDATE)
    containers::FlatHashSet<catalog::Column::Id> updated_set(
      updated_col_ids.begin(), updated_col_ids.end());
    conn_ctx.EnsureIndexesTransactions(
      table_id, visitor, [&](std::span<const catalog::Column::Id> index_cols) {
        for (auto col_id : index_cols) {
          if (updated_set.contains(col_id)) {
            return true;
          }
        }
        return false;
      });
  }

  return writers;
}

// Explicit instantiations
template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids);

template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Delete>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids);

template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Update>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids);

}  // namespace sdb::connector
