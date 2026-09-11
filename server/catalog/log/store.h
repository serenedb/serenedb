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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/catalog/catalog_permissions.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/entry.h"
#include "catalog/fwd.h"
#include "catalog/log/store_op.h"

namespace duckdb {

class AttachedDatabase;

}  // namespace duckdb
namespace sdb::catalog {

// duckdb's DBConfig::host_table_provider: the entry `catalog_id` names in `db`.
// A checkpoint records only the rows of such a table, so the load reads the
// definition off this entry and hands it the rows it was checkpointed with.
duckdb::optional_ptr<duckdb::TableCatalogEntry> HostTableEntry(
  duckdb::AttachedDatabase& db, uint64_t catalog_id);

// True for an attachment that hosts serenedb store tables, i.e. one of the
// per-database serenedb attachments.
bool IsStoreDatabase(duckdb::AttachedDatabase& db);
// The serenedb id of `db`, or an unset id when it is not a serenedb
// attachment.
ObjectId StoreDatabaseId(duckdb::AttachedDatabase& db);
// The attachment holding `database_id`'s rows. Null when it is not attached --
// a database dropped by another session, or one whose attach has not run yet.
//
// Held, not pointed at: a DROP DATABASE in another session detaches while this
// one is still reading, and the close waits for the last reference. A caller
// that keeps the attachment past one statement keeps this alive with it.
duckdb::shared_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  duckdb::ClientContext& context, ObjectId database_id);
// The same off the committed catalog, for the background paths that hold no
// client context.
duckdb::shared_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  ObjectId database_id);

// The table entry `table_id` names, in the catalog that owns it.
duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, duckdb::Catalog& catalog, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found);
duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found);

// Store mirror of an index as duckdb's own CREATE INDEX, or null when the index
// is not mirrored (non-Transactional table, expression/INCLUDE columns, or
// ART-unfriendly key types). The relation is left unnamed: the executor fills
// it in off the entry it resolves by id, which is the current one.
duckdb::unique_ptr<duckdb::CreateIndexInfo> MakeStoreIndexInfo(
  const duckdb::CreateTableInfo& table, const CreateIndexInfo& index);
// True for the mirror of an ART, which is built by a plan; an inverted one is
// injected from the catalog objects instead.
bool IsPlainStoreIndex(const duckdb::CreateIndexInfo& info) noexcept;

// An AlterInfo names its target twice: by identity, which is what resolves it,
// and by name, which is what an error message shows. A store op carries only
// the identity, so the name is left for the executor to fill in off the entry
// it resolves -- the current one, not the one the op was written against.
duckdb::AlterEntryData StoreTarget(duckdb::OnEntryNotFound if_not_found =
                                     duckdb::OnEntryNotFound::THROW_EXCEPTION);

// The data half of a definition change: DDL run against the owning database's
// attachment. Every op names the database whose file holds the rows, and
// `context` is the statement it belongs to -- null for the paths that have
// none: boot and teardown.
//
// duckdb's own ALTER against the rows of `relation`.
void StoreAlter(duckdb::ClientContext* context, ObjectId database_id,
                ObjectId relation, duckdb::unique_ptr<duckdb::AlterInfo> info);
// `info` is what MakeStoreIndexInfo produced. An inverted index is built from
// the catalog objects rather than by a plan, so those ride along, and the
// directory the CREATE opened rides too -- the op injects before the entry
// that will carry the handle is visible anywhere else.
void StoreCreateIndex(duckdb::ClientContext* context, ObjectId database_id,
                      duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
                      duckdb::unique_ptr<duckdb::CreateTableInfo> table,
                      ObjectId relation_id, std::shared_ptr<const Index> index,
                      std::shared_ptr<search::InvertedIndexStorage> storage);
// `name` is what the physical index is filed under, i.e. the catalog name it
// mirrors.
void StoreDropIndex(duckdb::ClientContext* context, ObjectId database_id,
                    ObjectId relation_id, std::string_view name);
// Moves the physical index a rename left behind.
void StoreRenameIndex(duckdb::ClientContext* context, ObjectId database_id,
                      ObjectId relation_id, std::string_view from,
                      std::string_view to);

// The catalog's persistent form: one duckdb WriteAheadLog under
// <datadir>/engine_catalog/, written from the commit that made the entries and
// replayed at boot. Store-table DDL executes against the owning database's
// attachment (see DataStore), on the statement's own transaction, so the data
// half commits with the statement.
//
// Two durability domains cannot be made atomic by ordering alone, so one of
// them has to be the decision point, and it is the catalog: it appends first
// and may fail, which is a clean abort and an ordinary SQL error -- what an
// append has to be once the log is a consensus log and losing leadership is
// routine rather than fatal. The database's own commit follows.
//
// A crash between the two leaves a definition whose rows the data file does not
// hold yet. Nothing has to be reconciled for that: the rows of a table are
// built from its definition when the file is opened.
class CatalogStore {
 public:
  struct Key {
    ObjectId parent_id;
    duckdb::CatalogType type{duckdb::CatalogType::INVALID};
    ObjectId id;
  };

  // One checkpoint record, copied off the entry the moment the walk saw it:
  // the walk excludes no mutation, so nothing of the entry is kept.
  struct CheckpointRecord {
    CheckpointRecord(const duckdb::CatalogEntry& entry);

    uint64_t oid;
    duckdb::unique_ptr<duckdb::CreateInfo> info;
    duckdb::CatalogPermissions permissions;
  };

  CatalogStore();
  ~CatalogStore();

  // False once the store is gone; a transaction commit hook can outlive it
  // during teardown.
  static bool Available() noexcept { return gInstance != nullptr; }

  // Resolves the directories the catalog and the data files live in. Fatal on
  // failure.
  void Initialize(std::string_view database_directory);

  // One store op, run against the database it names. The data work takes
  // duckdb's WAL and table locks and DDL operators call in here already holding
  // table locks, so it runs under the store lock -- which owns the data
  // connection -- and never under the one an append takes.
  void ApplyStoreOp(duckdb::ClientContext* context, store_op::Targeted op);

  // A sequence's counter, which lives outside the definition tree and so is
  // covered by no entry record. Appends inline, whatever statement the caller
  // is in.
  void DropSequence(ObjectId sequence_id);

  std::optional<uint64_t> TryGetBootSequenceValue(ObjectId sequence_id) const;
  // Every counter currently held, ordered by id: what boot reconciles against
  // the catalog, wiping the ones whose sequence no record names anymore.
  std::vector<ObjectId> SequenceIds() const;

  // The counter a replayed record states. `max_merge` keeps a floor: horizon
  // bumps append outside the sequence lock, so records for one sequence can
  // land out of order and any order has to replay to the highest.
  void ApplySequenceValue(ObjectId sequence_id, uint64_t value, bool max_merge);
  void ApplySequenceDropped(ObjectId sequence_id);

  // duckdb's DatabaseManager oid-reservation sink: writes down that every id
  // up to `horizon` is spent, before the allocator hands any of them out. The
  // allocator owns how far ahead that reaches and when to ask; this owns only
  // making it durable -- false when the log cannot take the record yet.
  static bool ReserveOids(uint64_t horizon);

  void PutSequenceValue(ObjectId sequence_id, uint64_t value);
  // Max-merge horizon bump; safe to call concurrently
  // for one sequence, appends group-commit. Returns once durable.
  void AdvanceSequenceValue(ObjectId sequence_id, uint64_t value);
  // Missing counter reads as 0.
  uint64_t GetSequenceValue(ObjectId sequence_id);

  // Folds the log if it is worth folding. A rewrite excludes no mutation: it
  // reads what is committed and abandons if a commit lands meanwhile, and the
  // next append attempts it again. The caller must hold no lock of this class.
  void TryCompact();
  // Rewrites the file now, whatever the thresholds say (fault injection).
  void CompactNow();

  std::string_view WalDirectory() const noexcept { return _directory; }
  std::string_view DataDirectory() const noexcept { return _data_directory; }

  // <datadir>/engine_duckdb/<id>.db -- the database's own duckdb file, holding
  // its rows and its data WAL. Created by the attach and removed once no
  // committed catalog record names it.
  static std::string DatabaseFilePath(ObjectId database_id);
  // Ids of the database files present on disk, whatever the catalog says.
  static std::vector<ObjectId> DatabaseFileIds();

 private:
  friend CatalogStore& GetCatalogStore();

  inline static CatalogStore* gInstance = nullptr;
  // The catalog's entries as the log files them, parents ahead of children: a
  // checkpoint is replayed like any other run of records, so it has to arrive
  // in an order every definition's ancestry is already in.
  static std::vector<CheckpointRecord> CheckpointEntriesOf();
  void MaybeCompact() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // Rewrites the file as a checkpoint: the catalog's definitions and the state
  // the store owns, read with no mutation excluded -- the rewrite abandons
  // when a commit lands between the read and the swap.
  void Compact() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  // Hands the batch to the databases its ops name. Throws the store's error as
  // a SQL error.
  void RunStoreOps(duckdb::ClientContext* context,
                   std::span<const store_op::Targeted> store_ops)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_store_mutex);
  std::string _directory;
  std::string _data_directory;

  mutable absl::Mutex _mutex;
  // Serializes store-side DDL: it owns the data-DB connection. The data work
  // under it takes duckdb's WAL/table locks, so it must never nest inside
  // _mutex (only the reverse: _store_mutex -> _mutex around appends).
  absl::Mutex _store_mutex;

  // Size of the file the last compaction produced, i.e. the live state written
  // out whole. Record counts say nothing about reclaimable bytes -- one dead
  // definition of a 1000-column table outweighs thousands of dead
  // an authoritative value -- so growth past a multiple of this triggers as
  // well.
  uint64_t _live_bytes ABSL_GUARDED_BY(_mutex) = 0;
  static constexpr uint64_t kLiveBytesGrowth = 2;
  // How often the sequence path bothers to look; the check needs _mutex, and a
  // nextval must not serialize on it.
  static constexpr uint64_t kSequenceCompactCheck = 4096;

  // Guarded by the cluster catalog log's lock (LockClusterCatalogWal): the map
  // mirrors the log's sequence records, staged beside the file they replay
  // from.
  containers::FlatHashMap<uint64_t, uint64_t> _sequences;
};

CatalogStore& GetCatalogStore();

}  // namespace sdb::catalog
