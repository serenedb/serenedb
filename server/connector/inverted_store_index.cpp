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

#include "connector/inverted_store_index.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <atomic>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/storage/table/row_group_segment_tree.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/scorer_options.h"
#include "search/store.h"

namespace sdb::connector {
namespace {

// The index entry one id names in the database holding it, or null when no
// entry there carries it -- an online CREATE INDEX feeds a concurrent writer
// before its own transaction has committed, so a miss is ordinary.
duckdb::optional_ptr<const duckdb::IndexCatalogEntry> FindIndexEntry(
  duckdb::ClientContext* context, duckdb::AttachedDatabase& db,
  duckdb::idx_t id) {
  const auto found = db.GetCatalog()
                       .Cast<catalog::SereneDBCatalog>()
                       .FindIn<duckdb::DuckIndexEntry>(context, id);
  return found ? &found->Cast<duckdb::IndexCatalogEntry>() : nullptr;
}

constexpr const char* kIndexIdOption = "catalog_oid";

duckdb::idx_t IdOption(const duckdb::case_insensitive_map_t<duckdb::Value>& o,
                       const char* key) {
  const auto* value = catalog::FindOption(o, key);
  if (!value || value->IsNull()) {
    return 0;
  }
  return value->GetValue<uint64_t>();
}

duckdb::IndexStorageInfo StorageRecord(const InvertedStoreIndex& index) {
  duckdb::IndexStorageInfo info{index.name};
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(index.IndexId());
  return info;
}

duckdb::idx_t SelectRows(duckdb::Vector& predicate, duckdb::idx_t total,
                         duckdb::SelectionVector& sel) {
  duckdb::UnifiedVectorFormat fmt;
  predicate.ToUnifiedFormat(total, fmt);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < total; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (fmt.validity.RowIsValid(idx) && values[idx]) {
      sel.set_index(kept++, i);
    }
  }
  return kept;
}

std::unique_ptr<DuckDBSearchSinkInsertWriter> MakeInsertWriter(
  irs::IndexWriter::Transaction& trx, const InvertedIndexConfig& config,
  TokenizerProvider tokenizers) {
  return std::make_unique<DuckDBSearchSinkInsertWriter>(
    trx, std::move(tokenizers), IndexedColumnIds(config),
    MakeEntryInfoProvider(config), config.pk);
}

duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> FieldExpressions(
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions,
  const InvertedIndexConfig& config) {
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> fields;
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (i >= config.keys.size() || config.WritesField(config.keys[i])) {
      fields.emplace_back(expressions[i]->Copy());
    }
  }
  return fields;
}

constexpr duckdb::idx_t kMinSlotRows = 8 * STANDARD_VECTOR_SIZE;
constexpr size_t kLiveFeedDepth = 2;

std::atomic<size_t> gBoundInvertedIndexes{0};

void FeedBatch(DuckDBSinkIndexWriter& writer, duckdb::DataChunk& batch,
               std::span<const FeedColumn> columns,
               std::span<const ExpressionSlot> expressions) {
  FeedChunk(writer, batch.size(), &batch.data.back(), batch, columns,
            expressions);
}

}  // namespace

struct InvertedStoreIndex::RangeTask final : duckdb::BaseExecutorTask {
  struct Target {
    InvertedStoreIndex* index;
    DuckDBSinkIndexWriter* writer;
    irs::IndexWriter::Transaction* transaction;
  };

  RangeTask(duckdb::TaskExecutor& executor,
            duckdb::DuckTransaction& transaction,
            duckdb::ClientContext& context, duckdb::RowGroupCollection& source,
            const duckdb::vector<duckdb::StorageIndex>& columns,
            std::vector<Target> targets, duckdb::idx_t begin, duckdb::idx_t end,
            duckdb::row_t row_start)
    : BaseExecutorTask{executor},
      transaction{transaction},
      context{context},
      columns{columns},
      targets{std::move(targets)},
      results(this->targets.size()),
      row{row_start + static_cast<duckdb::row_t>(begin)} {
    const auto& table_types = source.GetTypes();
    duckdb::vector<duckdb::LogicalType> scan_types;
    scan_types.reserve(columns.size());
    for (const auto& column : columns) {
      scan_types.push_back(table_types[column.GetPrimaryIndex()]);
    }
    const auto base = static_cast<duckdb::idx_t>(duckdb::MAX_ROW_ID);
    state.Initialize(columns);
    source.InitializeScanWithOffset(duckdb::QueryContext{}, state.local_state,
                                    columns, base + begin, base + end);
    scanned.Initialize(source.GetAllocator(), scan_types);
    view.InitializeEmpty(table_types);
    executors.reserve(this->targets.size());
    for (const auto& target : this->targets) {
      executors.push_back(std::make_unique<duckdb::ExpressionExecutor>(
        context, target.index->bound_expressions));
    }
  }

  duckdb::TaskExecutionResult ExecuteTaskStep() final {
    scanned.Reset();
    state.local_state.Scan(transaction, scanned);
    const auto size = scanned.size();
    if (size == 0) {
      return duckdb::TaskExecutionResult::TASK_FINISHED;
    }
    for (duckdb::idx_t i = 0; i < columns.size(); ++i) {
      view.data[columns[i].GetPrimaryIndex()].Reference(scanned.data[i]);
    }
    duckdb::VectorOperations::GenerateSequence(row_ids, size, row, 1);
    for (size_t t = 0; t < targets.size(); ++t) {
      duckdb::Vector rows{duckdb::LogicalType::ROW_TYPE, nullptr, 0};
      const auto fed = targets[t].index->Evaluate(
        context, view, row_ids, results[t], rows, executors[t].get());
      targets[t].index->Feed(*targets[t].writer, results[t], rows, fed);
    }
    row += static_cast<duckdb::row_t>(size);
    return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
  }

  std::string TaskType() const final { return "InvertedIndexRangeFeed"; }

  duckdb::DuckTransaction& transaction;
  duckdb::ClientContext& context;
  const duckdb::vector<duckdb::StorageIndex>& columns;
  std::vector<Target> targets;
  duckdb::TableScanState state;
  duckdb::DataChunk scanned;
  duckdb::DataChunk view;
  std::vector<std::unique_ptr<duckdb::ExpressionExecutor>> executors;
  std::vector<duckdb::DataChunk> results;
  duckdb::Vector row_ids{duckdb::LogicalType::ROW_TYPE};
  duckdb::row_t row;
};

InvertedStoreIndex::InvertedStoreIndex(
  duckdb::CreateIndexInput& input, duckdb::idx_t index_id,
  duckdb::idx_t table_oid, std::shared_ptr<search::Store> storage,
  std::shared_ptr<const InvertedIndexConfig> config,
  catalog::IndexTokenizers tokenizers, bool has_predicate)
  : BoundIndex(input.name, kTypeName, input.constraint_type, input.column_ids,
               input.table_io_manager,
               FieldExpressions(input.unbound_expressions, *config), input.db),
    _index_id{index_id},
    _table_oid{table_oid},
    _storage{std::move(storage)},
    _config{std::move(config)},
    _tokenizers{std::move(tokenizers)},
    _has_predicate{has_predicate} {
  SDB_ASSERT(_config);
  for (const auto& key : _config->keys) {
    if (!_config->WritesField(key)) {
      continue;
    }
    const duckdb::idx_t slot = _columns.size() + _expressions.size();
    if (bound_expressions[slot]->GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_REF) {
      _columns.emplace_back(
        slot, ColumnDescriptor{key.field_id, logical_types[slot]});
    } else {
      _expressions.emplace_back(key.field_id, slot);
    }
  }
  gBoundInvertedIndexes.fetch_add(1, std::memory_order_release);
}

InvertedStoreIndex::~InvertedStoreIndex() {
  gBoundInvertedIndexes.fetch_sub(1, std::memory_order_release);
}

bool InvertedStoreIndex::AnyBound() noexcept {
  return gBoundInvertedIndexes.load(std::memory_order_acquire) != 0;
}

duckdb::idx_t InvertedStoreIndex::Evaluate(
  duckdb::ClientContext& context, duckdb::DataChunk& chunk,
  duckdb::Vector& row_ids, duckdb::DataChunk& results, duckdb::Vector& rows,
  duckdb::ExpressionExecutor* executor) {
  const auto total = chunk.size();
  rows.Reference(row_ids);
  if (bound_expressions.empty()) {
    return total;
  }
  if (results.ColumnCount() == 0) {
    results.Initialize(duckdb::Allocator::DefaultAllocator(), logical_types);
  } else {
    results.Reset();
  }
  if (executor) {
    executor->Execute(chunk, results);
  } else {
    ExecuteExpressions(context, chunk, results);
  }
  if (!_has_predicate) {
    return total;
  }
  duckdb::SelectionVector sel{total};
  const auto count = SelectRows(results.data.back(), total, sel);
  if (count != total) {
    rows.Slice(row_ids, sel, count);
    results.Slice(sel, count);
  }
  return count;
}

void InvertedStoreIndex::Feed(DuckDBSinkIndexWriter& writer,
                              duckdb::DataChunk& results, duckdb::Vector& rows,
                              duckdb::idx_t count) {
  if (count != 0) {
    FeedChunk(writer, count, &rows, results, _columns, _expressions);
  }
}

std::unique_ptr<duckdb::DataChunk> InvertedStoreIndex::Batch(
  search::Writes* writes, duckdb::DataChunk& chunk, duckdb::DataChunk& results,
  duckdb::Vector& rows, duckdb::idx_t count) {
  auto types = results.GetTypes();
  types.push_back(duckdb::LogicalType::ROW_TYPE);
  auto batch = std::make_unique<duckdb::DataChunk>();
  batch->Initialize(duckdb::Allocator::DefaultAllocator(), types, count);
  auto& batch_rows = batch->data.back();
  duckdb::VectorOperations::Copy(rows, batch_rows, count, 0, 0);
  duckdb::FlatVector::SetSize(batch_rows, count);
  const auto first_row =
    duckdb::FlatVector::GetData<duckdb::row_t>(batch_rows)[0];
  for (duckdb::idx_t i = 0; i < results.ColumnCount(); ++i) {
    auto& target = batch->data[i];
    const auto& expression = *bound_expressions[i];
    if (writes && !_has_predicate &&
        expression.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
      const auto column =
        expression.Cast<duckdb::BoundReferenceExpression>().Index();
      target.Reference(writes->FeedColumn(&db, _table_oid, first_row, count,
                                          column, chunk.data[column]));
    } else {
      duckdb::VectorOperations::Copy(results.data[i], target, count, 0, 0);
      duckdb::FlatVector::SetSize(target, count);
    }
  }
  batch->SetCardinality(count);
  return batch;
}

search::Replay* InvertedStoreIndex::BeginReplay() {
  if (!_storage->Replays(db)) {
    return nullptr;
  }
  if (!_replay) {
    _replay = std::make_unique<search::Replay>(db, *_storage);
    _replay->FeedInParallel(
      [&](std::span<irs::IndexWriter::Transaction> slots) {
        std::vector<std::unique_ptr<DuckDBSearchSinkInsertWriter>> writers;
        writers.reserve(slots.size());
        for (auto& trx : slots) {
          trx.SetFieldOptions(_config);
          writers.push_back(MakeInsertWriter(
            trx, *_config, BoundTokenizers(_tokenizers.AcquireAll(*_context))));
        }
        return
          [writers = std::move(writers), columns = _columns,
           expressions = _expressions](size_t slot, duckdb::DataChunk& batch) {
            FeedBatch(*writers[slot], batch, columns, expressions);
          };
      });
  }
  return _replay.get();
}

void InvertedStoreIndex::ReplayAppend(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  auto* replay = BeginReplay();
  if (!replay) {
    return;
  }
  duckdb::DataChunk results;
  duckdb::Vector rows{duckdb::LogicalType::ROW_TYPE, nullptr, 0};
  const auto count = Evaluate(*_context, chunk, row_ids, results, rows);
  if (count != 0) {
    replay->Push(Batch(nullptr, chunk, results, rows, count));
  }
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  auto* replay = count == 0 ? nullptr : BeginReplay();
  if (!replay) {
    return;
  }
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    replay->RemoveKey(data[fmt.sel->get_index(i)]);
  }
}

bool InvertedStoreIndex::FinishReplay() {
  _context = nullptr;
  if (!_replay) {
    return false;
  }
  _replay->Finish();
  _replay.reset();
  return true;
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  if (chunk.size() == 0) {
    return {};
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayAppend(chunk, row_ids);
    return {};
  }
  duckdb::DataChunk results;
  duckdb::Vector rows{duckdb::LogicalType::ROW_TYPE, nullptr, 0};
  const auto count =
    Evaluate(conn->GetClientContext(), chunk, row_ids, results, rows);
  const auto slots = conn->SearchTxn().IndexSlots(_index_id);
  duckdb::idx_t prepared = 0;
  while (prepared < slots.size() && slots[prepared].writer) {
    ++prepared;
  }
  if (prepared == 0) {
    auto& context = conn->GetClientContext();
    auto& trx =
      conn->SearchTxn().EnsureIndexTransaction(_index_id, _storage, _config);
    const auto writer = MakeInsertWriter(trx, *_config, [&](irs::field_id id) {
      return _tokenizers.Acquire(id, context);
    });
    Feed(*writer, results, rows, count);
    return {};
  }
  if (prepared == 1) {
    Feed(*slots[0].writer, results, rows, count);
  } else if (count != 0) {
    if (!_live) {
      _live.emplace(db.GetDatabase(), prepared, kLiveFeedDepth,
                    [this, slots](size_t slot, duckdb::DataChunk& batch) {
                      FeedBatch(*slots[slot].writer, batch, _columns,
                                _expressions);
                    });
    }
    _live->Push(Batch(&conn->SearchTxn(), chunk, results, rows, count));
  }
  return {};
}

duckdb::ErrorData InvertedStoreIndex::FinishAppend() {
  if (!_live) {
    return {};
  }
  absl::Cleanup reset = [&] { _live.reset(); };
  try {
    _live->Finish();
  } catch (const std::exception& e) {
    return duckdb::ErrorData{e};
  }
  return {};
}

void InvertedStoreIndex::PrepareFeed(query::Transaction& transaction,
                                     duckdb::ClientContext& context,
                                     duckdb::idx_t rows) {
  const auto threads = duckdb::TaskScheduler::QueryThreads(context);
  const auto slots = std::clamp<duckdb::idx_t>(
    rows / kMinSlotRows, 1, std::max<duckdb::idx_t>(1, threads));
  for (duckdb::idx_t k = 0; k < slots; ++k) {
    auto& slot =
      transaction.SearchTxn().EnsureIndexSlot(_index_id, _storage, _config, k);
    if (!slot.writer) {
      slot.writer = MakeInsertWriter(
        *slot.trx, *_config, BoundTokenizers(_tokenizers.AcquireAll(context)));
    }
  }
}

bool InvertedStoreIndex::AppendLocal(
  duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
  duckdb::RowGroupCollection& source,
  const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
  duckdb::row_t row_start, duckdb::ErrorData& error) {
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    return false;
  }
  std::vector<duckdb::IndexWriteHandle<InvertedStoreIndex>> handles;
  std::vector<std::pair<InvertedStoreIndex*, std::span<search::Slot>>> indexes;
  duckdb::idx_t slots = 0;
  for (auto entry : index_list.IndexEntries()) {
    if (entry->GetBindState() != duckdb::IndexBindState::BOUND ||
        entry->GetIndexType() != kTypeName) {
      return false;
    }
    auto* inverted =
      handles.emplace_back(entry->GetWriteHandle<InvertedStoreIndex>())
        .operator->();
    const auto prepared = conn->SearchTxn().IndexSlots(inverted->_index_id);
    duckdb::idx_t ready = 0;
    while (ready < prepared.size() && prepared[ready].writer) {
      ++ready;
    }
    if (ready < 2) {
      return false;
    }
    slots = slots == 0 ? ready : std::min(slots, ready);
    indexes.emplace_back(inverted, prepared);
  }
  if (indexes.empty()) {
    return false;
  }
  const auto count = source.GetTotalRows();
  auto& context = conn->GetClientContext();
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context),
                                context.GetSchedulerAccount()};
  const auto base = static_cast<duckdb::idx_t>(duckdb::MAX_ROW_ID);
  const auto row_groups = source.GetRowGroups();
  const auto vector_start = [&](duckdb::idx_t row) {
    const auto group = row_groups->GetSegment(base + row)->GetRowStart() - base;
    return row - (row - group) % STANDARD_VECTOR_SIZE;
  };
  const auto ranges = std::max<duckdb::idx_t>(2, slots - 1);
  for (duckdb::idx_t k = 0; k < ranges; ++k) {
    const auto begin = vector_start(k * count / ranges);
    const auto end =
      k + 1 == ranges ? count : vector_start((k + 1) * count / ranges);
    if (end <= begin) {
      continue;
    }
    std::vector<RangeTask::Target> targets;
    targets.reserve(indexes.size());
    for (const auto& [inverted, prepared] : indexes) {
      targets.push_back(
        {inverted, prepared[k].writer.get(), prepared[k].trx.get()});
    }
    executor.ScheduleTask(duckdb::make_uniq<RangeTask>(
      executor, transaction, context, source, mapped_column_ids,
      std::move(targets), begin, end, row_start));
  }
  try {
    executor.WorkOnTasks();
  } catch (const std::exception& e) {
    error = duckdb::ErrorData{e};
  }
  SDB_PARK_ONCE_ON_FAILURE("inverted_local_rows_fed");
  return true;
}

void InvertedStoreIndex::Delete(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                                duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayDelete(chunk, row_ids);
    return;
  }
  if (_config->pk != PkColumnKind::Has) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("inverted index \"", name.GetIdentifierName(),
              "\" was created WITH (store_pk = 'none') and does not "
              "store row PKs: DELETE/UPDATE cannot maintain it; drop "
              "the index first or recreate it without store_pk = "
              "'none'"));
  }
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  std::vector<int64_t> rows;
  rows.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    rows.push_back(data[fmt.sel->get_index(i)]);
  }
  SDB_PARK_ONCE_ON_FAILURE("inverted_delete_rows_logged");
  auto native = _storage->AppendDeleteLog(std::move(rows));
  if (!native.empty()) {
    auto& trx =
      conn->SearchTxn().EnsureIndexTransaction(_index_id, _storage, _config);
    try {
      SDB_IF_FAILURE("inverted_rows_remove_fails") {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                        ERR_MSG("debug failure point"));
      }
      trx.Remove(irs::KeyRemoval{std::move(native)});
    } catch (const std::exception& e) {
      SDB_ERROR(SEARCH, "removing rows from index '", _index_id,
                "' failed: ", e.what(),
                "; the index replays them from the WAL on next boot");
      _storage->MarkOutOfSync();
    }
  }
}

idx_t InvertedStoreIndex::TryDelete(
  duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
  duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
  duckdb::optional_ptr<duckdb::SelectionVector>) {
  Delete(l, chunk, row_ids);
  if (deleted_sel) {
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      deleted_sel->set_index(i, i);
    }
  }
  return chunk.size();
}

duckdb::unique_ptr<duckdb::BoundIndex> InvertedStoreIndex::Create(
  duckdb::CreateIndexInput& input) {
  // Everything this needs is in the record duckdb read back: the id names the
  // entry, and the entry says the rest. No injection pass and no held
  // definition -- the registry builds the index the way it builds an ART.
  const auto& record = input.storage_info.options;
  const auto index_id = IdOption(record, kIndexIdOption);
  const auto entry = FindIndexEntry(&input.context, input.db, index_id);
  SDB_ENSURE(entry, "inverted index: catalog entry for ", index_id, " missing");
  const auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
  SDB_ENSURE(index_entry.Storage());
  auto index = duckdb::make_uniq<InvertedStoreIndex>(
    input, index_id, index_entry.table_oid, index_entry.Storage(),
    index_entry.Config(), index_entry.ResolveTokenizers(input.context),
    static_cast<bool>(index_entry.where_clause));
  if (!index_entry.Storage()->ReadOnly()) {
    index->_context = &input.context;
  }
  return std::move(index);
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  if (_storage->IsOutOfSync()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("inverted index ", _index_id,
              " is out of sync with its store table; refusing to checkpoint"));
  }
  return StorageRecord(*this);
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  return StorageRecord(*this);
}

duckdb::IndexType InvertedStoreIndex::GetInvertedIndexType() {
  duckdb::IndexType type;
  type.name = kTypeName;
  type.create_instance = &InvertedStoreIndex::Create;
  type.create_plan = &SereneDBCreateIndexPlan;
  type.defer_implicit_bind = true;
  type.remaps_columns = true;
  type.supports_sorted_by = true;
  return type;
}

PublishedInvertedIndex PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs) {
  const bool duck_table = relation.type == duckdb::CatalogType::TABLE_ENTRY;
  if (duck_table &&
      !relation.Cast<duckdb::DuckTableEntry>().GetStorage().IsMainTable()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    ERR_MSG("Transaction conflict: cannot add an index to a "
                            "table that has been altered or dropped"));
  }
  const auto options = catalog::ResolveSettings(entry.options);
  auto storage = std::make_shared<search::Store>(
    entry.catalog.Cast<catalog::SereneDBCatalog>().Directory(),
    entry.catalog.InMemory(), false, entry.catalog.GetOid(), entry.oid, options,
    entry.Config()->top_k_scorer, entry.Config()->sort, /*is_new=*/true);
  storage->ApplyOptions(options);
  entry.AdoptStorage(storage);
  if (!duck_table) {
    return {std::move(storage), 0};
  }
  auto& table = relation.Cast<duckdb::DuckTableEntry>();
  auto& data = table.GetStorage();
  const auto storage_ids = table.StorageColumnIds(entry);
  duckdb::CreateIndexInput input{
    context,      duckdb::TableIOManager::Get(data),
    data.db,      entry.index_constraint_type,
    entry.name,   storage_ids,
    bound_exprs,  duckdb::IndexStorageInfo{entry.name},
    entry.options};
  auto index = duckdb::make_uniq<InvertedStoreIndex>(
    input, entry.oid, entry.table_oid, storage, entry.Config(),
    entry.ResolveTokenizers(context), static_cast<bool>(entry.where_clause));
  auto commit_lock = data.db.GetStorageManager().GetCommitLock();
  data.AddIndex(std::move(index), entry.oid);
  const auto rowid_horizon = data.GetNextRowId();
  auto& transaction = duckdb::DuckTransaction::Get(context, data.db);
  const auto undo = transaction.GetUndoProperties();
  if (!undo.has_updates && !undo.has_deletes) {
    auto& manager = duckdb::DuckTransactionManager::Get(data.db);
    manager.WaitForDurability();
    manager.AdvanceStartTime(transaction);
  }
  return {std::move(storage), rowid_horizon};
}

}  // namespace sdb::connector
