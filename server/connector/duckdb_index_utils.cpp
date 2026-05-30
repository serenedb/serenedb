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

#include <absl/algorithm/container.h>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_rocksdb_writer.h"
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
      if (columns[i].GetId() == col_id) {
        type = columns[i].type;
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
  std::vector<duckdb_secondary_key::SKColumn> sk_columns,
  std::vector<duckdb_secondary_key::SKColumn> old_sk_columns) {
  switch (kind) {
    case DuckDBWriteKind::Insert:
      return std::make_unique<DuckDBSecondarySinkInsertWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns));
    case DuckDBWriteKind::Delete:
      return std::make_unique<DuckDBSecondarySinkDeleteWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns));
    case DuckDBWriteKind::Update:
      return std::make_unique<DuckDBSecondarySinkUpdateWriter<Unique>>(
        trx, shard_id, columns, std::move(sk_columns),
        std::move(old_sk_columns));
  }
  SDB_ASSERT(false, "Unknown DuckDBWriteKind");
  return nullptr;
}

std::unique_ptr<DuckDBSinkIndexWriter> MakeDuckDBSearchWriter(
  DuckDBWriteKind kind, irs::IndexWriter::Transaction& trx,
  TokenizerProvider&& tokenizer_provider,
  StoreValuesProvider&& store_values_provider,
  IsTextIndexedProvider&& is_text_indexed_provider,
  HNSWInfoProvider&& hnsw_info_provider,
  std::span<const catalog::Column::Id> columns,
  ExpressionTokenizerProvider&& expr_tokenizer_provider,
  std::vector<IndexedExpression>&& indexed_exprs) {
  switch (kind) {
    case DuckDBWriteKind::Insert:
      return std::make_unique<DuckDBSearchSinkInsertWriter>(
        trx, std::move(tokenizer_provider), columns,
        std::move(store_values_provider), std::move(is_text_indexed_provider),
        std::move(hnsw_info_provider), std::move(expr_tokenizer_provider),
        std::move(indexed_exprs));
    case DuckDBWriteKind::Delete:
      return std::make_unique<DuckDBSearchSinkDeleteWriter>(trx);
    case DuckDBWriteKind::Update:
      return std::make_unique<DuckDBSearchSinkUpdateWriter>(
        trx, std::move(tokenizer_provider), columns,
        std::move(store_values_provider), std::move(is_text_indexed_provider),
        std::move(hnsw_info_provider), std::move(expr_tokenizer_provider),
        std::move(indexed_exprs));
  }
  SDB_ASSERT(false, "Unknown DuckDBWriteKind");
  return nullptr;
}

}  // namespace

template<DuckDBWriteKind Kind>
std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> CreateDuckDBIndexWriters(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos) {
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> writers;
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  // Build filter: only include indexes whose columns overlap with updated set.
  // If updated_col_ids is empty, include all indexes (INSERT/DELETE).
  auto visitor = [&](auto& index_txn, const catalog::Index& index) {
    using TxnType = std::decay_t<decltype(index_txn)>;

    if constexpr (std::is_same_v<TxnType, rocksdb::Transaction>) {
      // Secondary index
      auto& sec_index = basics::downCast<const catalog::SecondaryIndex>(index);

      ObjectId shard_id;
      if (auto shard = snapshot->GetIndexShard(index.GetId())) {
        shard_id = shard->GetId();
      }

      auto sk_columns = BuildSKColumns(index, table, col_id_to_chunk_pos);
      // For Update writers: old_sk_columns use old mapping if provided,
      // otherwise fall back to the (new) col_id_to_chunk_pos.
      auto old_sk_columns =
        (Kind == DuckDBWriteKind::Update && !old_col_id_to_chunk_pos.empty())
          ? BuildSKColumns(index, table, old_col_id_to_chunk_pos)
          : sk_columns;

      if (sec_index.IsUnique()) {
        writers.push_back(MakeDuckDBSecondaryWriter<true>(
          Kind, index_txn, shard_id, index.GetColumnIds(),
          std::move(sk_columns), std::move(old_sk_columns)));
      } else {
        writers.push_back(MakeDuckDBSecondaryWriter<false>(
          Kind, index_txn, shard_id, index.GetColumnIds(),
          std::move(sk_columns), std::move(old_sk_columns)));
      }
    } else {
      // Inverted/search index
      auto& inverted_index =
        basics::downCast<const catalog::InvertedIndex>(index);
      auto tokenizer_provider = MakeTokenizerProvider(snapshot, inverted_index);
      auto store_values_provider = MakeStoreValuesProvider(inverted_index);
      auto is_text_indexed_provider = MakeIsTextIndexedProvider(inverted_index);
      auto hnsw_info_provider = MakeHNSWInfoProvider(inverted_index);
      auto expr_tokenizer_provider =
        MakeExpressionTokenizerProvider(snapshot, inverted_index);
      auto indexed_exprs =
        MakeIndexedExpressions(inverted_index, conn_ctx.GetClientContext());

      writers.push_back(MakeDuckDBSearchWriter(
        Kind, index_txn, std::move(tokenizer_provider),
        std::move(store_values_provider), std::move(is_text_indexed_provider),
        std::move(hnsw_info_provider), index.GetColumnIds(),
        std::move(expr_tokenizer_provider), std::move(indexed_exprs)));
    }
  };

  if (updated_col_ids.empty()) {
    // No filter -- create writers for all indexes (INSERT/DELETE)
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
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Delete>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

template std::vector<std::unique_ptr<DuckDBSinkIndexWriter>>
CreateDuckDBIndexWriters<DuckDBWriteKind::Update>(
  ObjectId table_id, ConnectionContext& conn_ctx, const catalog::Table& table,
  const ColumnChunkMapping& col_id_to_chunk_pos,
  std::span<const catalog::Column::Id> updated_col_ids,
  const ColumnChunkMapping& old_col_id_to_chunk_pos);

bool NeedsRowDeleteMarkers(
  std::span<const std::shared_ptr<catalog::Index>> indexes,
  std::span<const catalog::Column> columns) {
  for (const auto& index : indexes) {
    if (index->GetType() != catalog::ObjectType::InvertedIndex) {
      continue;
    }
    bool all_indexonly = true;
    for (auto col_id : index->GetColumnIds()) {
      auto it = absl::c_find_if(
        columns, [&](const auto& c) { return c.GetId() == col_id; });
      SDB_ASSERT(it != columns.end(),
                 "inverted index references unknown column id ", col_id);
      if (it->store_mode != catalog::ColumnStoreMode::kIndexOnly) {
        all_indexonly = false;
        break;
      }
    }
    if (all_indexonly) {
      return true;
    }
  }
  return false;
}

std::vector<size_t> BuildCreateIndexProjection(
  std::span<const catalog::Column> columns,
  std::span<const catalog::Column::Id> pk_column_ids,
  std::span<const duckdb::idx_t> index_column_positions) {
  // Sort + unique on a small vector is faster than a hash set and avoids
  // an allocation. Sorted order == catalog order, which keeps the
  // projection stable across call sites.
  std::vector<size_t> projection;
  projection.reserve(index_column_positions.size() + pk_column_ids.size());

  for (auto pos : index_column_positions) {
    SDB_ASSERT(pos < columns.size());
    projection.push_back(static_cast<size_t>(pos));
  }
  for (auto pk_id : pk_column_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == pk_id) {
        projection.push_back(i);
        break;
      }
    }
  }
  absl::c_sort(projection);
  projection.erase(std::unique(projection.begin(), projection.end()),
                   projection.end());
  return projection;
}

void EvaluateAndWriteIndexedExpressions(
  DuckDBSinkIndexWriter& sink, std::span<const IndexedExpression> indexed_exprs,
  duckdb::DataChunk& chunk, ObjectId table_id,
  std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& client_context, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys, DuckDBColumnSerializer& serializer) {
  for (const auto& indexed_expr : indexed_exprs) {
    SDB_ASSERT(indexed_expr.normalized_expr);
    auto result = EvaluateExprOverChunk(
      *indexed_expr.normalized_expr, chunk, table_id, slot_to_col_id,
      client_context, indexed_expr.is_geojson);

    const ExpressionDescriptor expr_desc{result.GetType(), /*have_nulls=*/true,
                                         indexed_expr.field_id};
    const bool switched = sink.SwitchExpression(expr_desc, result, num_rows);
    if (!switched) {
      continue;
    }

    DuckDBSinkIndexWriter* writer_ptr = &sink;
    ColumnDescriptor desc{catalog::Column::kInvalidId,
                          catalog::ColumnStoreMode::kNormal, result.GetType(),
                          /*have_nulls=*/true};
    DuckDBColumnSerializer::NoopWriter noop_writer{};
    serializer.WriteColumn(noop_writer, result, num_rows, row_keys,
                           {&writer_ptr, 1}, desc);
  }
}

}  // namespace sdb::connector
