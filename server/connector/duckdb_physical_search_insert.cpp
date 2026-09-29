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

#include <atomic>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"
#include "search/search_table_changes.h"
#include "server/utils/app_server.h"

namespace sdb::connector {
namespace {

struct SearchInsertGlobalState final : duckdb::GlobalSinkState {
  std::shared_ptr<search::SearchTable> search_table;
  duckdb::Catalog* catalog = nullptr;
  duckdb::idx_t table_id = 0;
  query::Transaction* sdb_txn = nullptr;
  std::vector<ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  duckdb::optional_ptr<duckdb::SequenceCatalogEntry> generated_pk_seq;
  std::shared_lock<std::shared_mutex> table_lock;

  std::atomic<bool> has_local_state = false;

  std::mutex combine_mu;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: the inserted rows, merged out of the sink threads.
  std::optional<duckdb::ColumnDataCollection> returned;

  // Segments the bulk workers flushed + fsynced, for the WAL to reference.
  std::vector<search::SearchDbWal::SegmentRef> bulk_segments;
};

struct SearchInsertSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

struct SearchInsertLocalState final : duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  bool bulk = false;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: collected per sink thread so a parallel insert does not
  // serialise on one collection, and merged on Combine.
  std::optional<duckdb::ColumnDataCollection> returned;
};

}  // namespace

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _return_chunk(return_chunk) {}

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _ctas_info(std::move(info)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  auto table = _table;
  if (_ctas_info) {
    auto& catalog = _ctas_info->schema.ParentCatalog();
    auto entry = catalog.CreateTable(catalog.GetCatalogTransaction(context),
                                     _ctas_info->schema, *_ctas_info);
    table = &entry->Cast<catalog::SearchTableEntry>();
  }

  state->search_table = table->Storage();
  state->catalog = &table->catalog;
  state->table_id = table->oid;
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};
  // Before any sink reads the shard's index config, so a rebuild can tell
  // that this transaction predates a config it publishes.
  conn_ctx.SearchTxn().RegisterWriter(state->search_table);

  const auto& columns = table->GetColumns();
  state->column_ids.reserve(columns.LogicalColumnCount());
  for (const auto& column : columns.Logical()) {
    state->column_ids.emplace_back(column.Oid());
  }
  state->chunk_types = columns.GetColumnTypes();
  state->generated_pk_seq = table->GeneratedPkSequence(context);
  SDB_ASSERT(state->generated_pk_seq);

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }

  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchInsert::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
  auto lstate = duckdb::make_uniq<SearchInsertLocalState>();

  lstate->bulk =
    gstate.has_local_state.exchange(true, std::memory_order_relaxed) ||
    (context.pipeline && context.pipeline->GetMaxThreads() > 1);
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }

  if (lstate->bulk) {
    // Exclusive: Combine reports this thread's segments to the WAL, so they
    // must not also carry a previous transaction's documents.
    lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
      gstate.search_table->GetTransaction(/*exclusive_segment=*/true));
    lstate->sink =
      MakeSearchTableInsertSink(*lstate->search_trx, *gstate.search_table,
                                *gstate.catalog, context.client);
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate =
    irs::utils::downCast<SearchInsertLocalState>(&input.local_state);

  const auto num_rows = chunk.size();
  if (!lstate->sink) {
    SDB_ASSERT(!lstate->bulk);
    auto& trx = gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
      gstate.search_table,
      [&] { return gstate.search_table->GetTransaction(); });
    lstate->sink = MakeSearchTableInsertSink(trx, *gstate.search_table,
                                             *gstate.catalog, context.client);
  }

  const uint64_t pk_base = gstate.generated_pk_seq->NextValues(
    duckdb::DuckTransaction::Get(context.client,
                                 gstate.generated_pk_seq->catalog),
    num_rows);
  WriteChunkToSearchSink(*lstate->sink, chunk, gstate.column_ids, pk_base,
                         gstate.table_id, context.client);
  if (lstate->returned) {
    // The chunk is the whole row in table-column order -- the defaults and the
    // STORED generated columns were resolved into the plan below this sink --
    // which is exactly what RETURNING projects over.
    lstate->returned->Append(chunk);
  }

  // The bulk path records nothing here: these rows and their PKs are already in
  // this thread's segment, which Combine hands to the WAL by reference.
  if (!lstate->bulk) {
    gstate.sdb_txn->SearchTxn().AddInlineInsertChunk(
      gstate.search_table,
      duckdb::BufferManager::GetBufferManager(context.client),
      gstate.chunk_types, chunk, pk_base);
  }
  lstate->insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchInsert::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate =
    irs::utils::downCast<SearchInsertLocalState>(&input.local_state);
  lstate->sink.reset();

  if (lstate->returned && lstate->returned->Count() != 0) {
    std::lock_guard<std::mutex> lock(gstate.combine_mu);
    gstate.returned->Combine(*lstate->returned);
  }

  if (lstate->insert_count == 0) {
    lstate->search_trx.reset();
    return duckdb::SinkCombineResultType::FINISHED;
  }

  // On the worker, in parallel with the others, rather than deferring the tail
  // to the single-threaded refresh commit; the fsync is what lets the WAL
  // reference these by name instead of copying the rows. The tick is still
  // assigned serially in SearchTableTransaction::Commit -- so never
  // FlushAndCommit -- and the returned span points into the segment context.
  std::vector<search::SearchDbWal::SegmentRef> segments;
  if (lstate->bulk) {
    const auto flushed = lstate->search_trx->FlushAndFsync();
    SDB_ASSERT(!flushed.empty(),
               "bulk sink thread with rows but no flushed segment");
    segments.reserve(flushed.size());
    for (const auto& segment : flushed) {
      segments.push_back(search::SearchDbWal::SegmentRef{
        .meta_file = segment.filename,
        .codec = std::string{segment.meta.codec->type()().name()}});
    }
  }

  std::lock_guard<std::mutex> lock(gstate.combine_mu);
  gstate.insert_count += lstate->insert_count;
  if (lstate->bulk) {
    gstate.bulk_segments.insert(gstate.bulk_segments.end(),
                                std::make_move_iterator(segments.begin()),
                                std::make_move_iterator(segments.end()));
    gstate.sdb_txn->SearchTxn().AddParallelSearchTransaction(
      gstate.search_table, std::move(lstate->search_trx));
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  if (!gstate.bulk_segments.empty()) {
    gstate.sdb_txn->SearchTxn().AddSegments(gstate.search_table,
                                            std::move(gstate.bulk_segments));
  }

  if (_ctas_info) {
    SDB_IF_FAILURE("crash_before_commit") { SDB_IMMEDIATE_ABORT(); }
  }
  if (gstate.table_lock.owns_lock()) {
    gstate.table_lock.unlock();
  }
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertSourceState>();
  if (_return_chunk) {
    sink_state->Cast<SearchInsertGlobalState>().returned->InitializeScan(
      state->scan);
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchInsertSourceState>();
  auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
  if (gstate.returned) {
    gstate.returned->Scan(source.scan, chunk);
    return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
