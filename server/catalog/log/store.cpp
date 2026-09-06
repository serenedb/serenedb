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

#include "catalog/log/store.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <filesystem>
#include <ranges>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/file_utils.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/table.h"
#include "connector/inverted_store_index.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

// Compact once dead records dominate and the file is worth rewriting.
constexpr uint64_t kCompactMinBytes = 1U << 20U;

}  // namespace

duckdb::optional_ptr<duckdb::TableCatalogEntry> HostTableEntry(
  duckdb::AttachedDatabase& db, uint64_t catalog_id) {
  if (TryGetCatalog() == nullptr) {
    return nullptr;
  }
  // Off this catalog's own sets, not through the database manager: an attach
  // reads its own tables back before the attachment is in it.
  auto& duck_catalog = db.GetCatalog();
  if (duck_catalog.GetCatalogType() != catalog::kSereneDBCatalogType) {
    return nullptr;
  }
  auto& catalog = duck_catalog.Cast<catalog::SereneDBCatalog>();
  return catalog.LookupTableById(catalog.CommittedRead(), catalog_id);
}

ObjectId StoreDatabaseId(duckdb::AttachedDatabase& db) {
  auto& catalog = db.GetCatalog();
  if (catalog.GetCatalogType() != catalog::kSereneDBCatalogType) {
    return {};
  }
  return catalog.Cast<catalog::SereneDBCatalog>().GetDatabaseId();
}

bool IsStoreDatabase(duckdb::AttachedDatabase& db) {
  return StoreDatabaseId(db).isSet();
}

duckdb::shared_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  duckdb::ClientContext& context, ObjectId database_id) {
  // Through the name rather than a scan of the database manager: the
  // attachment alias is the database name, and every read path that resolves a
  // store table goes through here.
  auto& manager = duckdb::DatabaseManager::Get(context);
  auto database = catalog::FindDatabase(&context, database_id);
  if (!database) {
    return nullptr;
  }
  const duckdb::Identifier name{database->name.GetIdentifierName()};
  // Through the transaction first, which is what registers the attachment as
  // referenced by it, and then for the reference itself.
  if (!manager.GetDatabase(context, name)) {
    return nullptr;
  }
  return manager.GetDatabase(name);
}

duckdb::shared_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  ObjectId database_id) {
  auto database = catalog::FindDatabase(nullptr, database_id);
  if (!database) {
    return nullptr;
  }
  return duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
    .GetDatabase(duckdb::Identifier{database->name.GetIdentifierName()});
}

duckdb::unique_ptr<duckdb::CreateIndexInfo> MakeStoreIndexInfo(
  const duckdb::CreateTableInfo& table, const CreateIndexInfo& index) {
  if (catalog::ReadTableEngineTag(table.tags) != TableEngine::Transactional) {
    return nullptr;
  }
  auto info = duckdb::make_uniq<duckdb::CreateIndexInfo>();
  info->oid = index.GetId().id();
  info->SetIndexName(duckdb::Identifier{index.GetName()});
  info->index_type = std::string{index.index_type};
  // What the registry resolves its objects by when duckdb builds this index
  // back: the entry says which index and which relation, and the objects say
  // the rest.
  info->options[connector::InvertedStoreIndex::kTableIdOption] =
    duckdb::Value::UBIGINT(catalog::IdOf(table).id());
  info->options[connector::InvertedStoreIndex::kIndexIdOption] =
    duckdb::Value::UBIGINT(index.GetId().id());

  if (!index.IsInverted()) {
    // The store's half is duckdb's own ART, whatever serenedb calls the kind.
    info->index_type = duckdb::ART::TYPE_NAME;
    info->constraint_type = index.constraint_type;
    for (const auto& key : index.parsed_expressions) {
      info->parsed_expressions.push_back(key->Copy());
    }
    return info;
  }

  // Inverted index: injected as a bound index built straight from the
  // catalog objects, so the info only names the target; the referenced
  // columns just have to exist.
  const auto& inverted = *index.GetIndex();
  if (inverted.GetReferencedColumns().empty()) {
    return nullptr;
  }
  for (auto col_id : inverted.GetReferencedColumns()) {
    if (!catalog::ColumnById(table, col_id)) {
      return nullptr;
    }
  }
  return info;
}

bool IsPlainStoreIndex(const duckdb::CreateIndexInfo& info) noexcept {
  return info.index_type == duckdb::ART::TYPE_NAME;
}

duckdb::AlterEntryData StoreTarget(duckdb::OnEntryNotFound if_not_found) {
  return duckdb::AlterEntryData{duckdb::QualifiedName{}, if_not_found};
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, duckdb::Catalog& catalog, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found) {
  auto& sdb_catalog = catalog.Cast<catalog::SereneDBCatalog>();
  auto entry = sdb_catalog.LookupTableById(
    sdb_catalog.GetCatalogTransaction(context), table_id.id());
  if (!entry) {
    SDB_ENSURE(if_not_found == duckdb::OnEntryNotFound::RETURN_NULL,
               "relation ", table_id.id(), " does not exist");
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found) {
  auto db = TryStoreDatabase(context, database_id);
  if (!db) {
    SDB_ENSURE(if_not_found == duckdb::OnEntryNotFound::RETURN_NULL,
               "database ", database_id.id(), " is not attached");
    return nullptr;
  }
  return GetStoreTableEntry(context, db->GetCatalog(), table_id, if_not_found);
}

// A store op is the data half of a definition change, and it belongs to the
// statement that decided it: applied here rather than parked, so the rows and
// the entry version in front of them are the same transaction's.
void CatalogStore::ApplyStoreOp(duckdb::ClientContext* context,
                                store_op::Targeted op) {
  absl::MutexLock store_lock{&_store_mutex};
  RunStoreOps(context, {&op, 1});
}

void StoreAlter(duckdb::ClientContext* context, ObjectId database_id,
                ObjectId relation, duckdb::unique_ptr<duckdb::AlterInfo> info) {
  info->oid = relation.id();
  GetCatalogStore().ApplyStoreOp(context,
                                 {database_id, relation, std::move(info)});
}

void StoreCreateIndex(duckdb::ClientContext* context, ObjectId database_id,
                      duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
                      duckdb::unique_ptr<duckdb::CreateTableInfo> table,
                      ObjectId relation_id, std::shared_ptr<const Index> index,
                      std::shared_ptr<search::InvertedIndexStorage> storage) {
  GetCatalogStore().ApplyStoreOp(
    context, {database_id, relation_id, std::move(info), std::move(table),
              std::move(index), std::move(storage)});
}

void StoreDropIndex(duckdb::ClientContext* context, ObjectId database_id,
                    ObjectId relation_id, std::string_view name) {
  auto info = duckdb::make_uniq<duckdb::DropInfo>();
  info->type = duckdb::CatalogType::INDEX_ENTRY;
  info->SetName(duckdb::Identifier{name});
  GetCatalogStore().ApplyStoreOp(context,
                                 {database_id, relation_id, std::move(info)});
}

void StoreRenameIndex(duckdb::ClientContext* context, ObjectId database_id,
                      ObjectId relation_id, std::string_view from,
                      std::string_view to) {
  duckdb::AlterEntryData target{
    duckdb::QualifiedName{duckdb::Identifier{}, duckdb::Identifier{},
                          duckdb::Identifier{from}},
    duckdb::OnEntryNotFound::RETURN_NULL};
  GetCatalogStore().ApplyStoreOp(context,
                                 {database_id, relation_id,
                                  duckdb::make_uniq<duckdb::RenameTableInfo>(
                                    target, duckdb::Identifier{to})});
}

CatalogStore::CatalogStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
  // Past the band the fixed system ids are carved out of, before anything a
  // catalog record can name is allocated. duckdb's own boot entries sit below
  // it and are never written down, so they cost nothing here.
  RestoreId(id::kMaxSystem.id());
  IdAllocator().SetOidReservationSink(&CatalogStore::ReserveOids);
}

CatalogStore::~CatalogStore() {
  IdAllocator().SetOidReservationSink(nullptr);
  gInstance = nullptr;
}

std::string CatalogStore::DatabaseFilePath(ObjectId database_id) {
  return basics::file_utils::BuildFilename(
    std::string{GetCatalogStore().DataDirectory()},
    absl::StrCat(database_id.id(), ".db"));
}

std::vector<ObjectId> CatalogStore::DatabaseFileIds() {
  namespace fs = std::filesystem;
  std::vector<ObjectId> ids;
  std::error_code ec;
  for (const auto& entry :
       fs::directory_iterator{GetCatalogStore().DataDirectory(), ec}) {
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const auto name = entry.path().filename().string();
    if (!name.ends_with(".db")) {
      continue;
    }
    uint64_t id = 0;
    if (absl::SimpleAtoi(std::string_view{name}.substr(0, name.size() - 3),
                         &id)) {
      ids.emplace_back(id);
    }
  }
  return ids;
}

void CatalogStore::Initialize(std::string_view database_directory) {
  _directory = basics::file_utils::BuildFilename(
    std::string{database_directory}, std::string{StaticStrings::kCatalogRoot});
  _data_directory = basics::file_utils::BuildFilename(
    std::string{database_directory},
    std::string{StaticStrings::kDataStoreRoot});
  for (const auto* directory : {&_directory, &_data_directory}) {
    std::error_code ec;
    std::filesystem::create_directories(*directory, ec);
    if (ec) {
      SDB_FATAL(STARTUP, "catalog: cannot create directory '", *directory,
                "': ", ec.message());
    }
  }
}

void CatalogStore::MaybeCompact() {
  auto wal = catalog::ClusterCatalogWal();
  if (!wal) {
    return;
  }
  const auto size = wal->GetStorageManager().GetWALSize();
  if (size < kCompactMinBytes) {
    return;
  }
  // Only past a doubling of the last written-out state: with no compaction
  // behind it yet (_live_bytes == 0), this road stays quiet. Compacting from
  // the hot path without that anchor is not load-safe today -- entries read
  // mid-commit surface null definitions -- so the anchor doubles as the gate.
  if (_live_bytes == 0 || size < _live_bytes * kLiveBytesGrowth) {
    return;
  }
  Compact();
}

void CatalogStore::TryCompact() {
  // Teardown can commit with the catalog already gone; a fold reads it.
  if (TryGetCatalog() == nullptr) {
    return;
  }
  absl::MutexLock lock{&_mutex};
  MaybeCompact();
}

void CatalogStore::CompactNow() {
  absl::MutexLock lock{&_mutex};
  Compact();
}

namespace {

using CheckpointEntries = std::vector<CatalogStore::CheckpointRecord>;

// Sorted by the id every serenedb entry carries, which is also creation order
// -- what makes a checkpoint reproducible from one run to the next.
void SortById(CheckpointEntries& entries, size_t from) {
  std::sort(entries.begin() + static_cast<ptrdiff_t>(from), entries.end(),
            [](const CatalogStore::CheckpointRecord& lhs,
               const CatalogStore::CheckpointRecord& rhs) {
              return lhs.oid < rhs.oid;
            });
}

template<typename Entry>
std::vector<Entry*> DatabaseEntriesOf(duckdb::ClientContext* context,
                                      ObjectId database) {
  std::vector<Entry*> found;
  catalog::Visit<Entry>(context, database, [&](const Entry& entry) {
    found.push_back(const_cast<Entry*>(&entry));
  });
  std::ranges::sort(found, {}, [](const Entry* entry) { return entry->oid; });
  return found;
}

}  // namespace

CatalogStore::CheckpointRecord::CheckpointRecord(
  const duckdb::CatalogEntry& entry)
  : oid{entry.oid}, info{entry.GetInfo()}, permissions{entry.permissions} {}

CheckpointEntries CatalogStore::CheckpointEntriesOf() {
  // A checkpoint is the catalog written out, not a second way of writing it --
  // and it is replayed like any other run of records, so it walks the tree top
  // down. A definition arriving before its parent has nowhere to hang, and the
  // kinds inside a schema are ordered the way a statement could have created
  // them: what a body names before the body, an owned sequence after the table
  // that owns it.
  //
  // Off the committed catalog: a rewrite runs with the mutations excluded, so
  // what is committed is everything there is to write.
  CheckpointEntries out;
  catalog::VisitRoleEntries(
    nullptr, [&](catalog::SereneDBRoleEntry& role) { out.emplace_back(role); });
  SortById(out, 0);

  std::vector<catalog::SereneDBDatabaseEntry*> databases;
  catalog::VisitDatabases(nullptr,
                          [&](catalog::SereneDBDatabaseEntry& database) {
                            databases.push_back(&database);
                          });
  std::ranges::sort(databases, {},
                    [](const catalog::SereneDBDatabaseEntry* database) {
                      return database->oid;
                    });

  for (auto* database : databases) {
    const ObjectId db_id{database->oid};
    out.emplace_back(*database);
    // Foreign servers are database children, so they come out of the catalog's
    // own set rather than a schema's.
    const auto servers_from = out.size();
    catalog::VisitForeignServers(
      nullptr, db_id, [&](const catalog::SereneDBForeignServerEntry& server) {
        out.emplace_back(
          const_cast<catalog::SereneDBForeignServerEntry&>(server));
      });
    SortById(out, servers_from);

    // Once for the whole database, because the walk is per database and not per
    // schema; each is filed under the schema it belongs to below.
    const auto tokenizers =
      DatabaseEntriesOf<catalog::SereneDBTokenizerEntry>(nullptr, db_id);
    const auto types =
      DatabaseEntriesOf<duckdb::TypeCatalogEntry>(nullptr, db_id);
    // A function occupies the scalar slot and a table function the other, and
    // both are filed under the schema they were created in.
    auto functions =
      DatabaseEntriesOf<duckdb::ScalarMacroCatalogEntry>(nullptr, db_id);
    const auto table_functions =
      DatabaseEntriesOf<duckdb::TableMacroCatalogEntry>(nullptr, db_id);
    const auto sequences =
      DatabaseEntriesOf<catalog::SereneDBSequenceEntry>(nullptr, db_id);
    const auto tables =
      DatabaseEntriesOf<catalog::SereneDBTableEntry>(nullptr, db_id);
    const auto views =
      DatabaseEntriesOf<duckdb::ViewCatalogEntry>(nullptr, db_id);
    const auto indexes =
      DatabaseEntriesOf<catalog::SereneDBIndexEntry>(nullptr, db_id);

    const auto put_indexes = [&](ObjectId relation_id) {
      for (auto* index : indexes) {
        if (index->GetRelationId() == relation_id) {
          out.emplace_back(*index);
        }
      }
    };
    // `owner` is the table a sequence is owned by, unset for the free-standing
    // ones. A sequence's name is always in its schema's relation namespace, so
    // the owning table decides only where in the walk it comes out.
    const auto put_sequences = [&](ObjectId schema_id, ObjectId owner) {
      for (auto* sequence : sequences) {
        if (ObjectId{sequence->ParentSchema().oid} == schema_id &&
            sequence->GetOwnerTableId() == owner) {
          out.emplace_back(*sequence);
        }
      }
    };
    const auto in_schema = [](const auto* entry, ObjectId schema_id) {
      return ObjectId{entry->ParentSchema().oid} == schema_id;
    };

    std::vector<catalog::SereneDBSchemaEntry*> schemas;
    catalog::VisitSchemas(nullptr, db_id,
                          [&](catalog::SereneDBSchemaEntry& schema) {
                            schemas.push_back(&schema);
                          });
    std::ranges::sort(
      schemas, {},
      [](const catalog::SereneDBSchemaEntry* schema) { return schema->oid; });
    for (auto* schema : schemas) {
      const ObjectId schema_id{schema->oid};
      out.emplace_back(*schema);
      for (auto* tokenizer : tokenizers) {
        if (in_schema(tokenizer, schema_id)) {
          out.emplace_back(*tokenizer);
        }
      }
      for (auto* type : types) {
        if (in_schema(type, schema_id)) {
          out.emplace_back(*type);
        }
      }
      // Free-standing sequences before anything that can name one in a DEFAULT;
      // the ones a table owns are written after that table, below.
      put_sequences(schema_id, ObjectId{});
      for (auto* function : functions) {
        if (in_schema(function, schema_id)) {
          out.emplace_back(*function);
        }
      }
      for (auto* function : table_functions) {
        if (in_schema(function, schema_id)) {
          out.emplace_back(*function);
        }
      }
      for (auto* table : tables) {
        if (!in_schema(table, schema_id)) {
          continue;
        }
        out.emplace_back(*table);
        // Owned sequences are entries of their own, so they come back as such
        // rather than riding this one.
        put_sequences(schema_id, ObjectId{table->oid});
        put_indexes(ObjectId{table->oid});
      }
      for (auto* view : views) {
        if (!in_schema(view, schema_id)) {
          continue;
        }
        out.emplace_back(*view);
        put_indexes(ObjectId{view->oid});
      }
    }
  }
  return out;
}

void CatalogStore::Compact() {
  // The catalog is where a definition lives, so the checkpoint is read out of
  // it -- with no mutation excluded. What the log held before the read is the
  // marker: a commit landing during it splices first, so the rewrite sees the
  // count moved and abandons rather than swapping the commit's records away.
  const auto expected_written = catalog::ClusterCatalogWalSize().appended_bytes;
  const auto entries = CheckpointEntriesOf();
  catalog::RewriteClusterCatalogWal(
    expected_written, [&](duckdb::WriteAheadLog& wal) {
      for (const auto& entry : entries) {
        wal.WriteCreateEntry(*entry.info, entry.permissions);
      }
      catalog::WriteOidHorizonTo(wal, IdAllocator().OidReservation());
      // The rewrite holds the log's lock, which is also what guards the map.
      std::vector<uint64_t> seq_ids;
      seq_ids.reserve(_sequences.size());
      for (const auto& [id, value] : _sequences) {
        seq_ids.push_back(id);
      }
      absl::c_sort(seq_ids);
      for (const auto id : seq_ids) {
        catalog::WriteSequenceValueTo(wal, ObjectId{id}, _sequences[id],
                                      /*max_merge=*/false);
      }
    });
}

namespace {

// The relation a batch of store ops reshapes, as the user named it. Every op
// carries the store table it targets; the first one is the batch's subject,
// which is what an error about the batch has to name. Off the transaction's own
// view, because the batch that lost the race is often the one whose target the
// winner dropped, and the committed catalog no longer holds it.
std::string StoreOpsSubject(duckdb::ClientContext* context,
                            std::span<const store_op::Targeted> ops) {
  for (const auto& targeted : ops) {
    const auto table_id = targeted.relation_id;
    if (!table_id.isSet()) {
      continue;
    }
    if (context == nullptr) {
      continue;
    }
    if (const auto* table =
          catalog::FindSession<SereneDBTableEntry>(*context, table_id)) {
      return std::string{table->name.GetIdentifierName()};
    }
  }
  return {};
}

}  // namespace

void CatalogStore::RunStoreOps(duckdb::ClientContext* context,
                               std::span<const store_op::Targeted> store_ops) {
  absl::Status r;
  try {
    r = GetDataStore().ApplyStoreOps(context, store_ops);
  } catch (const duckdb::TransactionException& e) {
    // Said in the user's own terms: duckdb's conflict text names the store
    // table, which is not a name the statement used.
    r = absl::AbortedError(e.what());
  }
  if (r.ok()) {
    return;
  }
  if (absl::IsAborted(r)) {
    // Another transaction reshaped the same rows. Retryable, and the same
    // 40001 a concurrently dropped target raises -- not duckdb's internal
    // conflict text, which names the store table rather than the user's.
    const auto name = StoreOpsSubject(context, store_ops);
    if (name.empty()) {
      // Nothing to name: the batch reshapes no relation the catalog still
      // holds. Same answer the entry write gives when it cannot name one.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                      ERR_MSG("could not serialize access due to concurrent "
                              "DDL on the same object"));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of table \"",
              name, "\""));
  }
  THROW_SQL_ERROR(ERR_MSG(r.message()));
}

void CatalogStore::DropSequence(ObjectId sequence_id) {
  ApplySequenceDropped(sequence_id);
  catalog::WriteSequenceDropped(sequence_id);
}

std::vector<ObjectId> CatalogStore::SequenceIds() const {
  const auto lock = catalog::LockClusterCatalogWal();
  std::vector<ObjectId> ids;
  ids.reserve(_sequences.size());
  for (const auto& [id, value] : _sequences) {
    ids.emplace_back(id);
  }
  absl::c_sort(ids, [](auto l, auto r) { return l.id() < r.id(); });
  return ids;
}

std::optional<uint64_t> CatalogStore::TryGetBootSequenceValue(
  ObjectId sequence_id) const {
  const auto lock = catalog::LockClusterCatalogWal();
  const auto it = _sequences.find(sequence_id.id());
  if (it == _sequences.end()) {
    return std::nullopt;
  }
  return it->second;
}

void CatalogStore::ApplySequenceValue(ObjectId sequence_id, uint64_t value,
                                      bool max_merge) {
  const auto seq_lock = catalog::LockClusterCatalogWal();
  auto [it, inserted] = _sequences.try_emplace(sequence_id.id(), value);
  if (!inserted) {
    it->second = max_merge ? std::max(it->second, value) : value;
  }
}

void CatalogStore::ApplySequenceDropped(ObjectId sequence_id) {
  const auto seq_lock = catalog::LockClusterCatalogWal();
  _sequences.erase(sequence_id.id());
}

void CatalogStore::PutSequenceValue(ObjectId sequence_id, uint64_t value) {
  ApplySequenceValue(sequence_id, value, /*max_merge=*/false);
  catalog::WriteSequenceValue(sequence_id, value, /*max_merge=*/false);
}

bool CatalogStore::ReserveOids(uint64_t horizon) {
  // Replay allocates ids of its own -- deserializing a table default-constructs
  // its columns -- while the log is still being read. Nothing named after one
  // of those ever reaches disk, and boot raises the counter past the horizon it
  // just read, so writing them down would say nothing -- and the false return
  // keeps the allocator's horizon down until the log is up to take a record.
  return catalog::WriteOidHorizon(horizon);
}

void CatalogStore::AdvanceSequenceValue(ObjectId sequence_id, uint64_t value) {
  ApplySequenceValue(sequence_id, value, /*max_merge=*/true);
  catalog::WriteSequenceValue(sequence_id, value, /*max_merge=*/true);
}

uint64_t CatalogStore::GetSequenceValue(ObjectId sequence_id) {
  return TryGetBootSequenceValue(sequence_id).value_or(0);
}

CatalogStore& GetCatalogStore() { return *CatalogStore::gInstance; }

}  // namespace sdb::catalog
