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
#include "basics/log.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_table_entry.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

std::unique_ptr<DuckDBSinkIndexWriter> MakeDuckDBSearchWriter(
  DuckDBWriteKind kind, irs::IndexWriter::Transaction& trx,
  TokenizerProvider&& tokenizer_provider,
  EntryInfoProvider&& entry_info_provider,
  std::span<const catalog::Column::Id> indexed_columns,
  std::vector<IndexedExpression>&& indexed_exprs, PkPolicy pk_policy) {
  switch (kind) {
    case DuckDBWriteKind::Insert:
      return std::make_unique<DuckDBSearchSinkInsertWriter>(
        trx, std::move(tokenizer_provider), indexed_columns,
        std::move(entry_info_provider), std::move(indexed_exprs), pk_policy);
    case DuckDBWriteKind::Delete:
      return std::make_unique<DuckDBSearchSinkDeleteWriter>(trx);
  }
  SDB_ASSERT(false, "Unknown DuckDBWriteKind");
  return nullptr;
}

}  // namespace

template<DuckDBWriteKind Kind>
std::unique_ptr<DuckDBSinkIndexWriter> CreateInvertedIndexWriter(
  ObjectId table_id, ObjectId index_id, ConnectionContext& conn_ctx,
  duckdb::optional_ptr<duckdb::ClientContext> expr_context) {
  std::unique_ptr<DuckDBSinkIndexWriter> writer;
  auto snapshot = conn_ctx.CatalogSnapshot();
  auto make_writer = [&](auto& index_txn, const catalog::Index& index) {
    if (index.GetId() != index_id) {
      return;
    }
    auto& inverted_index = basics::downCast<const catalog::InvertedIndex>(index);
    if constexpr (Kind == DuckDBWriteKind::Delete) {
      if (!inverted_index.GetOptions().pk_term) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("inverted index \"", inverted_index.GetName(),
                  "\" was created WITH (store_pk = 'none') and does not "
                  "index row PKs: DELETE/UPDATE cannot maintain it; drop "
                  "the index first or recreate it without store_pk = "
                  "'none'"));
      }
    }
    auto tokenizer_provider = MakeTokenizerProvider(snapshot, inverted_index);
    auto entry_info_provider = MakeEntryInfoProvider(inverted_index);
    std::vector<IndexedExpression> indexed_exprs;
    if constexpr (Kind == DuckDBWriteKind::Insert) {
      indexed_exprs = MakeIndexedExpressions(
        inverted_index,
        expr_context ? *expr_context : conn_ctx.GetClientContext());
    }
    const auto& index_options = inverted_index.GetOptions();
    writer =
      MakeDuckDBSearchWriter(Kind, index_txn, std::move(tokenizer_provider),
                             std::move(entry_info_provider),
                             index.GetColumns(), std::move(indexed_exprs),
                             PkPolicy{.index_term = index_options.pk_term,
                                      .column = index_options.pk_column});
  };
  conn_ctx.EnsureIndexesTransactions(table_id, make_writer);
  if (!writer) {
    // The caller is the duckdb index-list walk -- the publication authority.
    // An index published after this transaction's snapshot was pinned (its
    // creation held the table's checkpoint lock while this commit waited) is
    // missing from the session snapshot but must still be fed; resolve it,
    // and the tokenizers it references, from the latest catalog snapshot.
    auto latest = catalog::GetCatalog().GetCatalogSnapshot();
    auto inverted =
      latest ? latest->GetObject<catalog::InvertedIndex>(index_id) : nullptr;
    // No tombstone check: an index under construction stays tombstoned until
    // its CREATE finalizes, and feeding it is the point. Membership in the
    // live duckdb index list is the drop authority here.
    if (inverted && inverted->GetRelationId() == table_id) {
      snapshot = latest;
      conn_ctx.EnsureIndexTransaction(std::move(inverted), make_writer);
    }
  }
  return writer;
}

template std::unique_ptr<DuckDBSinkIndexWriter>
CreateInvertedIndexWriter<DuckDBWriteKind::Insert>(
  ObjectId table_id, ObjectId index_id, ConnectionContext& conn_ctx,
  duckdb::optional_ptr<duckdb::ClientContext> expr_context);
template std::unique_ptr<DuckDBSinkIndexWriter>
CreateInvertedIndexWriter<DuckDBWriteKind::Delete>(
  ObjectId table_id, ObjectId index_id, ConnectionContext& conn_ctx,
  duckdb::optional_ptr<duckdb::ClientContext> expr_context);

std::vector<size_t> BuildCreateIndexProjection(
  std::span<const catalog::Column> columns,
  std::span<const catalog::Column::Id> pk_column_ids,
  std::span<const duckdb::idx_t> index_column_positions) {
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
  duckdb::ClientContext& client_context, duckdb::idx_t num_rows) {
  for (const auto& indexed_expr : indexed_exprs) {
    SDB_ASSERT(indexed_expr.normalized_expr);
    auto result = EvaluateExprOverChunk(
      *indexed_expr.normalized_expr, chunk, table_id, slot_to_col_id,
      client_context, indexed_expr.is_geojson);

    const ExpressionDescriptor expr_desc{result.GetType(),
                                         indexed_expr.field_id};
    sink.SwitchExpression(expr_desc, result, num_rows);
  }
}

}  // namespace sdb::connector
