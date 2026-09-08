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

#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/primary_key.hpp"
#include "catalog1/catalog.h"
#include "catalog1/entry/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/index_expression.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
#include "search/tick_domain.h"

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

constexpr const char* kIndexIdOption = "sdb_index_id";

duckdb::idx_t IdOption(const duckdb::case_insensitive_map_t<duckdb::Value>& o,
                       const char* key) {
  const auto it = o.find(key);
  if (it == o.end() || it->second.IsNull()) {
    return 0;
  }
  return it->second.GetValue<uint64_t>();
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

}  // namespace

struct InvertedStoreIndex::ReplaySession {
  explicit ReplaySession(InvertedStoreIndex& index)
    : trx{index.NewTransaction()},
      insert_writer{index.MakeInsertWriter(trx)},
      delete_writer{trx} {}

  irs::IndexWriter::Transaction trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
  DuckDBSearchSinkDeleteWriter delete_writer;
  uint64_t durable_offset = 0;
};

InvertedStoreIndex::InvertedStoreIndex(
  duckdb::CreateIndexInput& input, duckdb::idx_t index_id,
  std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const InvertedIndexConfig> config,
  catalog::IndexTokenizers tokenizers)
  : BoundIndex(input.name, kTypeName, input.constraint_type, input.column_ids,
               input.table_io_manager, input.unbound_expressions, input.db),
    _index_id{index_id},
    _storage{std::move(storage)},
    _config{std::move(config)},
    _tokenizers{std::move(tokenizers)} {
  SDB_ASSERT(_config);
}

InvertedStoreIndex::~InvertedStoreIndex() = default;

irs::IndexWriter::Transaction InvertedStoreIndex::NewTransaction() {
  SDB_ENSURE(_storage, "inverted index ", _index_id, ": storage missing");
  auto trx = _storage->GetTransaction();
  trx.SetFieldOptions(_config);
  return trx;
}

std::unique_ptr<DuckDBSearchSinkInsertWriter>
InvertedStoreIndex::MakeInsertWriter(irs::IndexWriter::Transaction& trx) {
  return std::make_unique<DuckDBSearchSinkInsertWriter>(
    trx, [this](irs::field_id id) { return _tokenizers.Acquire(id); },
    IndexedColumnIds(*_config), MakeEntryInfoProvider(*_config), _config->pk);
}

void InvertedStoreIndex::WriteChunk(DuckDBSearchSinkInsertWriter& writer,
                                    irs::IndexWriter::Transaction& trx,
                                    duckdb::DataChunk& chunk,
                                    duckdb::Vector& row_ids) {
  const auto total = chunk.size();
  const auto keys = std::span{_config->keys};
  duckdb::DataChunk results;
  if (!bound_expressions.empty()) {
    results.Initialize(duckdb::Allocator::DefaultAllocator(), logical_types);
    ExecuteExpressions(chunk, results);
    for (size_t i = 0; i < keys.size(); ++i) {
      const auto* entry = _config->FindEntry(keys[i].field_id);
      if (!entry || !entry->whole_value) {
        RejectJsonObjectArrayLeaves(results.data[i], total);
      }
    }
  }
  duckdb::SelectionVector sel;
  auto count = total;
  if (bound_expressions.size() > keys.size()) {
    sel.Initialize(total);
    count = SelectRows(results.data.back(), total, sel);
  }
  if (count != 0) {
    duckdb::DataChunk filtered_chunk;
    duckdb::Vector filtered_rows{row_ids.GetType(), nullptr, 0};
    auto* feed_chunk = &chunk;
    auto* feed_rows = &row_ids;
    if (count != total) {
      filtered_chunk.InitializeEmpty(chunk.GetTypes());
      filtered_chunk.Reference(chunk);
      filtered_chunk.Slice(sel, count);
      filtered_rows.Slice(row_ids, sel, count);
      results.Slice(sel, count);
      feed_chunk = &filtered_chunk;
      feed_rows = &filtered_rows;
    }
    duckdb::UnifiedVectorFormat row_fmt;
    feed_rows->ToUnifiedFormat(count, row_fmt);
    const auto* row_data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(row_fmt);
    std::vector<std::string> row_keys(count);
    std::vector<std::string_view> key_views(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      primary_key::AppendSigned(row_keys[i],
                                row_data[row_fmt.sel->get_index(i)]);
      key_views[i] = row_keys[i];
    }
    std::vector<ExpressionValue> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      const auto first = absl::c_none_of(
        keys.first(i), [&](const catalog::InvertedIndexKey& earlier) {
          return earlier.field_id == keys[i].field_id;
        });
      if (first) {
        values.push_back({keys[i].field_id, &results.data[i]});
      }
    }
    FeedChunk(writer, count, PkChunk{.keys = key_views, .column = feed_rows},
              *feed_chunk, {}, values);
  }
  trx.AdvanceQueries(1);
}

InvertedStoreIndex::ReplaySession& InvertedStoreIndex::EnsureReplaySession() {
  if (_replay) {
    return *_replay;
  }
  _replay = std::make_unique<ReplaySession>(*this);
  const auto cursor = _storage->GetRecoveryWalCursor();
  const auto& block_manager = db.GetStorageManager().GetBlockManager();
  if (cursor.generation == block_manager.GetCheckpointIteration()) {
    _replay->durable_offset = cursor.offset;
  }
  return *_replay;
}

void InvertedStoreIndex::OnReplayRange(duckdb::idx_t commit_offset) {
  _replay_commit_offset = commit_offset;
}

void InvertedStoreIndex::ReplayAppend(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  auto& session = EnsureReplaySession();
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset < session.durable_offset) {
    return;
  }
  WriteChunk(*session.insert_writer, session.trx, chunk, row_ids);
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  auto& session = EnsureReplaySession();
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset < session.durable_offset) {
    return;
  }
  const auto count = chunk.size();
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  std::string key;
  FeedDeletes(session.delete_writer, key, count,
              [&](size_t i) { return data[fmt.sel->get_index(i)]; });
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  if (session.trx.GetQueries() != 0) {
    session.trx.RegisterFlush();
    const auto last_tick =
      search::TickDomain::Instance().Advance(session.trx.GetQueries() + 1);
    auto& storage_manager = db.GetStorageManager();
    _storage->RecordFlushCursor(
      last_tick, search::WalCursor{
                   storage_manager.GetBlockManager().GetCheckpointIteration(),
                   storage_manager.GetWALSize()});
    SDB_ENSURE(session.trx.Commit(last_tick),
               "inverted index replay: commit failed for index ", _index_id);
  }
  _replay.reset();
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
  SDB_ENSURE(_storage, "inverted index ", _index_id, ": storage missing");
  auto& trx = conn->EnsureIndexTransaction(_index_id, _storage, _config);
  const auto writer = MakeInsertWriter(trx);
  WriteChunk(*writer, trx, chunk, row_ids);
  conn->RegisterSearchFlush();
  return {};
}

duckdb::ErrorData InvertedStoreIndex::Append(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

duckdb::ErrorData InvertedStoreIndex::Insert(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
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
  if (!_config->pk.index_term) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("inverted index \"", name.GetIdentifierName(),
              "\" was created WITH (store_pk = 'none') and does not "
              "index row PKs: DELETE/UPDATE cannot maintain it; drop "
              "the index first or recreate it without store_pk = "
              "'none'"));
  }
  SDB_ENSURE(_storage, "inverted index ", _index_id, ": storage missing");
  auto& trx = conn->EnsureIndexTransaction(_index_id, _storage, _config);
  const auto remove = [&](size_t n, auto&& row_at) {
    DuckDBSearchSinkDeleteWriter writer{trx};
    std::string key;
    FeedDeletes(writer, key, n, row_at);
  };
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  if (_storage->IsDeleteLogOpen()) {
    const auto log_begin = _storage->DeleteLogRowidBegin();
    const auto log_end = _storage->DeleteLogRowidEnd();
    std::vector<int64_t> native;
    std::vector<int64_t> logged;
    native.reserve(count);
    logged.reserve(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const int64_t row = data[fmt.sel->get_index(i)];
      (row < log_begin || row >= log_end ? native : logged).push_back(row);
    }
    // Reads like a use-after-move and is not: AppendDeleteLog only takes the
    // vector when it accepts it, and returns false without touching it once the
    // log is latched. Then these rows are past publication and delete natively.
    if (!logged.empty() && !_storage->AppendDeleteLog(std::move(logged))) {
      absl::c_move(logged, std::back_inserter(native));
    }
    if (!native.empty()) {
      remove(native.size(), [&](size_t i) { return native[i]; });
    }
  } else {
    remove(count, [&](size_t i) { return data[fmt.sel->get_index(i)]; });
  }
  conn->RegisterSearchFlush();
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

std::string InvertedStoreIndex::ToString(duckdb::IndexLock&, bool) {
  return "inverted store index";
}

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
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
  // A rebind (an ALTER-driven table rebuild, a re-bind after replay) must not
  // open a second writer over the same directory, so it adopts the storage the
  // index already registered under this name is holding.
  std::shared_ptr<search::InvertedIndexStorage> storage;
  auto& indexes = entry->Cast<duckdb::DuckIndexEntry>().GetDataTableInfo();
  for (auto& index : indexes.GetIndexes().Indexes()) {
    if (index.IsBound() && index.GetIndexName() == input.name &&
        index.GetIndexType() == std::string{kTypeName}) {
      storage = index.Cast<InvertedStoreIndex>().Storage();
      break;
    }
  }
  const auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
  return duckdb::make_uniq<InvertedStoreIndex>(
    input, index_id, std::move(storage), index_entry.Config(),
    index_entry.ResolveTokenizers(input.context));
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  if (_storage) {
    SDB_ENSURE(!_storage->IsOutOfSync(), "inverted index ", _index_id,
               " is out of sync with its store table; refusing to checkpoint");
    _storage->CheckpointRefresh();
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
  return type;
}

std::shared_ptr<search::InvertedIndexStorage> PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs) {
  const auto& options = entry.Config()->settings;
  auto storage = search::InvertedIndexStorage::Create(
    entry.catalog.GetOid(), entry.schema.oid, relation.oid, entry.oid, options,
    entry.TopKScorer(context), /*is_new=*/true);
  storage->ApplyOptions(options);
  entry.AdoptStorage(storage);
  auto* table = dynamic_cast<duckdb::DuckTableEntry*>(&relation);
  if (table == nullptr) {
    return storage;
  }
  auto& data = table->GetStorage();
  duckdb::CreateIndexInput input{
    context,      duckdb::TableIOManager::Get(data),
    data.db,      entry.index_constraint_type,
    entry.name,   entry.column_ids,
    bound_exprs,  duckdb::IndexStorageInfo{entry.name},
    entry.options};
  data.GetDataTableInfo()->GetIndexes().AddIndex(
    duckdb::make_uniq<InvertedStoreIndex>(input, entry.oid, storage,
                                          entry.Config(),
                                          entry.ResolveTokenizers(context)));
  return storage;
}

}  // namespace sdb::connector
