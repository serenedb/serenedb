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

#include "connector/duckdb_physical_search_insert.h"

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/key_utils.hpp"
#include "connector/search_table_marker.h"
#include "connector/search_table_sink_writer.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/local_table_changes.h"
#include "query/transaction.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

// All operator state for the parallel INSERT pipeline. One instance per
// query::Transaction-bound INSERT; rebuilt per query, not reused across
// transactions.
struct SearchInsertGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<TableShard> table_shard;
  // Borrowed -- both live on the connection's query::Transaction, which
  // outlives this state (the sdb-side commit happens after Finalize and
  // owns the iresearch trx via _search_transactions).
  query::Transaction* sdb_txn = nullptr;
  irs::IndexWriter::Transaction* search_trx = nullptr;

  // Catalog column ids in input-chunk order (skips the synthetic
  // generated-PK column). Used both as the SearchTableSinkWriter
  // SwitchColumn key and as the WAL marker's column_ids payload.
  std::vector<catalog::Column::Id> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;

  // PK encoding scaffolding (matches duckdb_physical_insert's setup,
  // but without conflict-resolver / index-writer fanout).
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::string table_key;

  // Set for tables without an explicit PRIMARY KEY -- Sink reserves a
  // contiguous range from this sequence per chunk; Finalize walks the
  // reservations via a cursor when assigning per-row PKs. Null for
  // explicit-PK tables.
  std::shared_ptr<catalog::Sequence> generated_pk_seq;

  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t insert_count = 0;
};

struct SearchInsertSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(std::move(table)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertGlobalState>();
  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  state->table_shard = snapshot->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  SDB_ASSERT(state->table_shard->GetStorage() == catalog::StorageKind::kSearch,
             "SereneDBSearchInsert dispatched against a non-search shard");
  state->table_lock = std::shared_lock{state->table_shard->GetTableLock()};

  // No explicit PK -> resolve the generated-PK sequence object once here;
  // Sink reserves per-chunk ranges from it. Mirrors duckdb_physical_insert.
  if (_table->PKColumns().empty()) {
    state->generated_pk_seq =
      snapshot->GetObject<catalog::Sequence>(_table->GetGeneratedPkSeqId());
    SDB_ASSERT(state->generated_pk_seq);
  }

  // Build column metadata in chunk order. The DuckDB plan binds the
  // chunk to non-generated columns of the table in declaration order.
  state->column_ids.reserve(_table->Columns().size());
  state->chunk_types.reserve(_table->Columns().size());
  for (const auto& col : _table->Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->column_ids.push_back(col.GetId());
    state->chunk_types.push_back(col.type);
  }

  state->pk_columns = duckdb_primary_key::BuildPKColumns(*_table);

  state->sdb_txn = &conn_ctx;
  auto& search_shard =
    basics::downCast<search::SearchTableShard>(*state->table_shard);
  state->search_trx = &state->sdb_txn->EnsureSearchTransaction(
    state->table_shard->GetId(), [&] { return search_shard.GetTransaction(); });

  return state;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // Append the chunk to the per-(txn, table) buffer. No iresearch write
  // here: the buffer is drained in Finalize so the per-batch SwitchColumn
  // / per-row Write loop sees all chunks in one pass against one iresearch
  // Document.
  auto& entry = gstate.sdb_txn->GetLocalTableChanges()[gstate.table_id];
  if (!entry.inserts) {
    entry.inserts = std::make_unique<duckdb::ColumnDataCollection>(
      duckdb::BufferManager::GetBufferManager(context.client),
      gstate.chunk_types);
  }

  // For generated-PK tables, reserve a contiguous PK range now (in Sink
  // arrival order) so the per-chunk assignment order matches
  // duckdb_physical_insert's behaviour even though our row writes don't
  // happen until Finalize. CDC may consolidate Sink chunks during
  // iteration, so the ranges are aligned to *Sink arrivals*, not to
  // Chunks() iteration boundaries; Finalize walks them via a cursor.
  if (gstate.generated_pk_seq) {
    auto base = gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows);
    entry.reserved_pk_ranges.push_back(
      {base, static_cast<duckdb::idx_t>(num_rows)});
  }
  entry.inserts->Append(chunk);

  gstate.insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBSearchInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  if (gstate.insert_count == 0) {
    return duckdb::SinkFinalizeType::READY;
  }

  auto it = gstate.sdb_txn->GetLocalTableChanges().find(gstate.table_id);
  SDB_ASSERT(it != gstate.sdb_txn->GetLocalTableChanges().end() &&
             it->second.inserts);
  auto& buffer = *it->second.inserts;
  auto& reserved_ranges = it->second.reserved_pk_ranges;

  // Generated-PK cursor: walks the per-Sink-call reservations as we
  // iterate buffer rows. The reservations were pushed in Sink-arrival
  // order; CDC may consolidate Sink chunks into different sizes during
  // Chunks() iteration, so we can't 1:1-align chunks and ranges --
  // instead advance the cursor row-by-row through the flat range list.
  // Unused for explicit-PK tables (reserved_ranges is empty).
  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  SDB_ASSERT(!uses_generated_pk ||
             !reserved_ranges.empty() == (gstate.insert_count > 0));
  size_t range_idx = 0;
  duckdb::idx_t row_in_range = 0;

  // Drain the buffer through the sink: per-chunk Init/Finish cycle, with
  // one SwitchColumn per column + one per-row PK Write between them.
  //
  // Init's batch_size is the *exact* row count for the upcoming chunk:
  // iresearch's Transaction::Insert(false, batch_size) pre-allocates that
  // many DocContext slots in the segment, and the segment's docs_count
  // reflects the allocation, not the number of NextDocument calls
  // (over-allocation by N leaves the segment claiming N extra docs with
  // no columnstore data behind them). Using STANDARD_VECTOR_SIZE once
  // across all chunks produced docs_count=2048 with 3 row of cs data
  // before this was fixed.
  SearchTableSinkWriter sink{*gstate.search_trx};

  // Reusable per-row PK buffer. Per-chunk PK formats prepared each
  // iteration (Vector::Reference inside the chunk iterator -- the formats
  // are bound to that vector's underlying buffer).
  std::string pk_buffer;

  for (auto& chunk : buffer.Chunks()) {
    const auto num_rows = chunk.size();
    if (num_rows == 0) {
      continue;
    }

    sink.Init(num_rows);

    // SwitchColumn per input column: opens (or reuses) the columnstore
    // writer for the column and batch-appends the Vector.
    for (duckdb::idx_t col_idx = 0; col_idx < gstate.column_ids.size();
         ++col_idx) {
      sink.SwitchColumn(gstate.column_ids[col_idx], gstate.chunk_types[col_idx],
                        chunk.data[col_idx], num_rows);
    }

    // Per-row PK injection. For explicit-PK tables, MakeColumnKey reads
    // the PK from chunk columns via pk_columns/pk_formats; generated_pk
    // arg is ignored. For generated-PK tables, pk_columns is empty and
    // generated_pk supplies the row's PK -- pulled from the cursor walk
    // over reserved_ranges (advanced one slot per row).
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      uint64_t generated_pk = 0;
      if (uses_generated_pk) {
        while (range_idx < reserved_ranges.size() &&
               row_in_range >= reserved_ranges[range_idx].count) {
          ++range_idx;
          row_in_range = 0;
        }
        SDB_ASSERT(range_idx < reserved_ranges.size(),
                   "reserved_pk_ranges exhausted before buffer rows did "
                   "-- Sink/Finalize accounting bug");
        generated_pk = reserved_ranges[range_idx].base + row_in_range;
        ++row_in_range;
      }
      pk_buffer.clear();
      duckdb_primary_key::MakeColumnKey(
        pk_formats, gstate.pk_columns, row, generated_pk, gstate.table_key,
        [](std::string_view) {}, pk_buffer);
      // The sink's PK field indexes only the row-key portion (the bytes
      // after the table_id + column_id prefix); pass it directly.
      sink.Write(key_utils::ExtractRowKey(pk_buffer));
    }

    sink.Finish();
  }

  // Emit the buffer as WAL marker(s) -- one CDC marker below the split
  // threshold, per-chunk markers above. EmitInsertsForBuffer rides the
  // markers into the sdb_txn's rocksdb WriteBatch via PutLogData; the
  // iresearch trx is committed later by query::Transaction::Commit (same
  // path InvertedIndexShard's trxs go through).
  search_table_marker::EmitInsertsForBuffer(*gstate.sdb_txn, gstate.table_id,
                                            gstate.column_ids, buffer);

  // Buffer is consumed; drop it so concurrent / subsequent statements in
  // the same txn see a fresh entry.
  gstate.sdb_txn->GetLocalTableChanges().erase(it);

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.UpdateNumRows(gstate.table_id, gstate.insert_count);
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SearchInsertSourceState>();
}

duckdb::SourceResultType SereneDBSearchInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchInsertSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
