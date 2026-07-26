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
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/entry.h"
#include "catalog/store/store_op.h"
#include "catalog/store/wal.h"
#include "catalog/store/wal_entry.h"

namespace duckdb {

class AttachedDatabase;
class Constraint;
struct CreateIndexInfo;

}  // namespace duckdb
namespace sdb::catalog {

class CreateTableInfo;

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
duckdb::optional_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
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

// Store mirror of an index, or nullopt when the index is not mirrored
// (non-Transactional table, expression/INCLUDE columns, or ART-unfriendly
// key types).
std::optional<StoreIndexDef> MakeStoreIndexDef(
  const CreateTableInfo& table, const CreateIndexInfoBase& index);

// The catalog's persistent form: an append-only log of record frames under
// <datadir>/engine_catalog/, replayed into resident maps at boot. Store-table
// DDL executes against the owning database's attachment (see DataStore), on
// the statement's own transaction, so the data half commits with the statement.
//
// Two durability domains cannot be made atomic by ordering alone, so one of
// them has to be the decision point, and it is the catalog:
//
//   1. the catalog commits first, and may fail. The log position moves N to
//      N+M, and a failure means nothing happened -- a clean abort and an
//      ordinary SQL error, which is what an append has to be once the log is
//      a consensus log and losing leadership is routine rather than fatal;
//   2. then the database's duckdb commit, carrying the store change and the
//      new position N+M in one atomic commit. It lands or it does not; there
//      is no third state.
//
// A crash between the two leaves the database's committed position behind the
// log tail. Boot compares one number per database and replays exactly the
// frames in between -- never a scan, never a diff of catalog against store --
// which is why every frame that has a data half carries it (wal::StoreOps).
//
// See docs/catalog-wal-operations.md for the per-operation mapping.
class CatalogStore {
 public:
  struct Key {
    ObjectId parent_id;
    duckdb::CatalogType type{duckdb::CatalogType::INVALID};
    ObjectId id;
  };

  // Transient handle for Write's callback. The two halves are separate because
  // they are separate contracts: `catalog()` appends durable WAL entries,
  // `store()` queues DDL for the owning database's attachment, and Write
  // orders one against the other. The whole batch acks atomically at its WAL
  // append.
  class WriteContext {
   public:
    // The durable half.
    class Catalog {
     public:
      // Creates or replaces a table definition. `mode` is what the statement
      // asked for, not what the catalog happens to hold: replaying a replace
      // whose target a concurrent commit dropped has to be refused rather than
      // resurrect it. The sequences its SERIAL columns own are part of the same
      // operation, so they ride along -- only a create names any.
      void PutTable(const CreateTableInfo& table, wal::PutMode mode,
                    Permissions perm,
                    std::vector<wal::OwnedSequence> sequences = {});
      // Creates or replaces an object whose definition is the CreateInfo its
      // catalog entry is built from -- the shape every kind moves to. `perm` is
      // the owner and ACL, which duckdb's CreateInfo has no room for. An index
      // goes through here too: `parent_id` is its schema, and the relation it
      // covers rides on the info.
      void PutEntry(ObjectId parent_id, duckdb::CatalogType type, ObjectId id,
                    wal::PutMode mode,
                    std::shared_ptr<const duckdb::CreateInfo> info,
                    Permissions perm = {});
      void DropObject(ObjectId parent_id, duckdb::CatalogType type,
                      ObjectId id);
      // Opens a drop. Takes effect at once (the object stops being visible) and
      // stays open until PrepareCommit closes it, which is when the whole
      // structural subtree goes -- so a cascade needs no per-object drop. The
      // record carries the reclamation subtree, so a boot that finds the drop
      // open rebuilds the async sweep from it alone.
      void DropPrepare(wal::DropPrepare drop);
      // Closes the drop open for `id`: its artifacts are gone, so the subtree
      // is reclaimed.
      void PrepareCommit(ObjectId id);
      // Erases every definition directly under `parent_id`.
      void DropChildren(ObjectId parent_id);
      void SetSequence(ObjectId sequence_id, uint64_t value);
      void DropSequence(ObjectId sequence_id);

     private:
      friend class WriteContext;
      friend class CatalogStore;

      std::vector<wal::Entry> _entries;
    };

    // The data half: DDL run against the owning database's attachment. Every
    // op names the database whose file holds the rows.
    class Store {
     public:
      void CreateTable(ObjectId database_id, ObjectId table);
      void DropTable(ObjectId database_id, ObjectId table);
      void DropColumn(ObjectId database_id, ObjectId table, std::string name,
                      ObjectId column_id);
      void RenameColumn(ObjectId database_id, ObjectId table, std::string name,
                        std::string new_name);
      // Adds a column. `type_sql` is the SQL type text; `default_sql` is the
      // DEFAULT expression text (empty for none) used to backfill existing
      // rows.
      void AddColumn(ObjectId database_id, ObjectId table, std::string name,
                     std::string type_sql, std::string default_sql,
                     duckdb::CompressionType compression);
      // Changes a column's type. `using_sql` is the USING cast text (empty for
      // the implicit cast).
      void ChangeColumnType(ObjectId database_id, ObjectId table,
                            std::string name, std::string type_sql,
                            std::string using_sql);
      // Removes the CHECK constraint with this expression text.
      void DropCheck(ObjectId database_id, ObjectId table, std::string expr);
      void DropNotNull(ObjectId database_id, ObjectId table,
                       std::string column);
      void AddNotNull(ObjectId database_id, ObjectId table, std::string column);
      // Adds the CHECK constraint with this expression text; the store verifies
      // it against existing rows (mirrors DropCheck).
      void AddCheck(ObjectId database_id, ObjectId table, std::string expr);
      // Adds a PRIMARY KEY (recreates storage, validates existing rows: no
      // duplicates, no nulls). `columns` are store-table column names in key
      // order; `constraint` names the index duckdb builds for it.
      void AddPrimaryKey(ObjectId database_id, ObjectId table,
                         std::string constraint,
                         std::vector<std::string> columns);
      // Adds a UNIQUE constraint over `columns` (recreate + existing-row dup
      // validation).
      void AddUnique(ObjectId database_id, ObjectId table,
                     std::string constraint, std::vector<std::string> columns);
      // Inverted defs carry the catalog objects so the executor can build the
      // injected bound index; ART defs run as store-side SQL.
      void CreateIndex(ObjectId database_id, StoreIndexDef def,
                       TableInfoRef table, IndexInfoRef index);
      // `name` is what the physical index is filed under, i.e. the catalog
      // name it mirrors. Empty for a drop reopened at boot, whose record does
      // not carry one and whose removal the original frame already made.
      void DropIndex(ObjectId database_id, ObjectId index_id,
                     ObjectId relation_id, std::string_view name);
      // Moves the physical index a rename left behind.
      void RenameIndex(ObjectId database_id, ObjectId relation_id,
                       ObjectId index_id, std::string_view from,
                       std::string_view to);

      // Every op one ALTER TABLE implies, derived from the two versions of the
      // definition rather than from the statement: a rewrite states what the
      // table now is, and which columns and constraints moved is the
      // difference between the two. Nothing for a table with no store table of
      // its own -- a search-backed one owns its storage.
      void ReshapeTable(ObjectId database_id, ObjectId table,
                        const CreateTableInfo& before,
                        const CreateTableInfo& after);

     private:
      friend class WriteContext;
      friend class CatalogStore;

      std::vector<store_op::Targeted> _ops;
    };

    WriteContext(const WriteContext&) = delete;
    WriteContext& operator=(const WriteContext&) = delete;

    Catalog& catalog() { return _catalog; }
    Store& store() { return _store; }

   private:
    friend class CatalogStore;

    WriteContext() = default;

    Catalog _catalog;
    Store _store;
  };

  CatalogStore();
  ~CatalogStore();

  // False once the store is gone; a transaction commit hook can outlive it
  // during teardown.
  static bool Available() noexcept { return gInstance != nullptr; }

  // Resolves the directories the catalog and the data files live in. Fatal on
  // failure.
  void Initialize(std::string_view database_directory);
  void Shutdown();

  // Opens <datadir>/engine_catalog/catalog.wal and replays it. The state the
  // store owns -- open drops, sequence counters, the id horizon, the
  // outstanding store work -- is folded in here; every definition record goes
  // to `apply`, frame by frame, in the order the records were decided. That is
  // the catalog, which is the only place a definition lands.
  void Replay(absl::FunctionRef<void(std::span<const wal::Entry>)> apply);

  // `context` is the statement the batch belongs to; its store ops run in that
  // statement's transaction and its constructive entries are appended when that
  // transaction commits. Null for boot and background drop tasks, which have no
  // statement of their own and append inline.
  //
  // `performed` is handed the batch's records once the store half has run and
  // the frame is queued or appended -- it is where the caller takes their
  // effect into the catalog, so the records are what the in-memory catalog is
  // built from rather than a second description of the same batch. Not called
  // for an empty batch.
  void Write(duckdb::ClientContext* context,
             absl::FunctionRef<void(WriteContext&)> fill,
             absl::FunctionRef<void(std::span<const wal::Entry>)> performed);
  void Write(duckdb::ClientContext* context,
             absl::FunctionRef<void(WriteContext&)> fill) {
    Write(context, fill, [](std::span<const wal::Entry>) {});
  }
  // Shorthand for the batches that have no statement to defer to.
  void Write(absl::FunctionRef<void(WriteContext&)> fill) {
    Write(nullptr, fill);
  }
  // Appends one frame of records built by the caller, for a batch that has no
  // statement to defer to and hands the same records to the applier itself.
  void WriteFrame(std::span<const wal::Entry> entries);
  // One-entry shorthands, likewise context-free: they append inline, so a
  // mutator that runs inside a user statement must take the Write overload
  // above instead, or its frame outlives a ROLLBACK.
  void DropObject(ObjectId parent_id, duckdb::CatalogType type, ObjectId id);
  void DropPrepare(wal::DropPrepare drop);
  // Closes the drop open for `id`, once the async sweep has removed its
  // artifacts.
  void PrepareCommit(ObjectId id);
  // Pair with DropObject(..., SEQUENCE_ENTRY, id) to fully drop.
  void DropSequence(ObjectId sequence_id);

  // Every drop still open, ordered by id. Such an id is spent whether or not
  // its reclamation completes, so boot raises the id counter past it --
  // reissuing it would collide with whatever the store still holds under it.
  std::vector<ObjectId> AllOpenDrops() const;
  // The record that opened the drop of `id`, or nullopt when nothing is open
  // for it. It carries the whole reclamation subtree, so boot rebuilds the
  // async sweep from it rather than from the definitions the drop removed.
  std::optional<wal::DropPrepare> OpenDrop(ObjectId id) const;
  std::optional<uint64_t> TryGetBootSequenceValue(ObjectId sequence_id) const;

  // duckdb's DatabaseManager oid-reservation sink: writes down that every id
  // up to `horizon` is spent, before the allocator hands any of them out. The
  // allocator owns how far ahead that reaches and when to ask; this owns only
  // making it durable.
  static void ReserveOids(uint64_t horizon);

  void PutSequenceValue(ObjectId sequence_id, uint64_t value);
  // Max-merge horizon bump (wal::BumpSequence); safe to call concurrently
  // for one sequence, appends group-commit. Returns once durable.
  void AdvanceSequenceValue(ObjectId sequence_id, uint64_t value);
  // Missing counter reads as 0.
  uint64_t GetSequenceValue(ObjectId sequence_id);

  // The catalog log is written from the commit: `frames` are the records a
  // finishing transaction produced and `publish` makes their effect visible in
  // the catalog, and the two happen as one step. A checkpoint is the catalog
  // written out, so a durable frame the catalog cannot answer for would be lost
  // by the next rewrite -- and none can exist, because a rewrite has to hold
  // both this lock and the catalog's, and this pair holds them together.
  //
  // The caller holds Catalog::_mutex. Throws when the append fails, before
  // `publish` has run: nothing is durable and nothing is visible, which is
  // what lets a commit be refused rather than aborting the process -- the
  // failure mode of a consensus log, where losing leadership is routine.
  //
  // Returns the log position of the last frame, zero when there were none;
  // that is what the data commit records.
  //
  // `context` is the transaction that is committing, or null for the paths
  // with none. A checkpoint fired from inside a commit reads the kinds whose
  // entry is the object through it: duckdb has not made those entries visible
  // to anybody else yet, and a committed read would rewrite the log without the
  // records this very commit appended.
  uint64_t CommitFrames(duckdb::ClientContext* context,
                        std::span<const std::vector<wal::Entry>> frames,
                        absl::FunctionRef<void()> publish);

  // The log position of the last frame appended.
  uint64_t LogPosition() const;

  // The database's data half is durable up to `position`: forget the batches at
  // or below it. Called from the commit hook of the transaction that carried
  // it, and at boot once a database's replay has caught it up.
  void AckDatabasePosition(ObjectId database_id, uint64_t position);

  // The frames naming `database_id` whose data half is not known to be durable,
  // ordered by position -- the boot gap, and the whole of the outstanding work.
  struct PendingBatch {
    uint64_t position = 0;
    std::shared_ptr<const std::vector<store_op::Targeted>> ops;
  };
  std::vector<PendingBatch> PendingFor(ObjectId database_id,
                                       uint64_t committed_position) const;
  // Forgets the outstanding work of every database not in `live`. Boot reads
  // the whole log, so it starts out holding the batches of databases that have
  // since been dropped; nothing will ever replay those, and leaving them would
  // stop the log from ever folding again.
  void ForgetUnackedExcept(std::span<const ObjectId> live);

  CatalogWal::Stats WalStats() const { return _wal.GetStats(); }
  // Folds the log if it is worth folding. A rewrite reads the catalog and has
  // to exclude the commits that write it, so this takes the catalog mutex --
  // and only tries, so a caller that already holds it (a mutation on its way
  // out) simply skips, and the next append attempts it again. The caller must
  // hold no lock of this class.
  void TryCompact();
  // Rewrites the file now, whatever the thresholds say (fault injection). The
  // caller already holds the catalog mutex.
  void CompactNow();

  // What a checkpoint would write, for introspection (sdb_catalog_snapshot):
  // the catalog's definitions in the order and under the keys the log files
  // them by, the drops still open, and the sequence counters.
  void VisitSnapshot(
    absl::FunctionRef<void(Key, std::shared_ptr<const duckdb::CreateInfo>)>
      info_visitor,
    absl::FunctionRef<void(ObjectId, uint64_t)> sequence_visitor);

  // Decodes one wal frame (sdb_catalog_wal).
  static wal::ParsedFrame ParseFrame(std::span<const uint8_t> frame);

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

  // Appends one frame and returns the log position it landed at.
  uint64_t AppendFrame(std::span<const wal::Entry> entries)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // Append + apply, the pair every durable batch performs. Returns the frame's
  // log position.
  uint64_t Commit(std::span<const wal::Entry> entries)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // Folds a frame into the state the store owns: the open drops, the sequence
  // counters, the id horizon and the outstanding store work. Definitions are
  // not state here -- they are the catalog's, which is where replay puts them.
  // `position` is the frame's log position, which the store-op record is filed
  // under.
  void ApplyEntries(std::span<const wal::Entry> entries, uint64_t position)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // Reclaims the subtree of the drop open for `id`. No-op if nothing is open
  // for it.
  void FinishOpen(ObjectId id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // The catalog's definitions as the log files them, parents ahead of children:
  // a checkpoint is replayed like any other run of records, so it has to arrive
  // in an order every definition's ancestry is already in.
  static std::vector<wal::Entry> CheckpointDefinitions(
    duckdb::ClientContext* context);
  void MaybeCompact(duckdb::ClientContext* context)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  // Counts a sequence append toward compaction and, occasionally, acts on it.
  void NoteSequenceAppend();
  // Rewrites the file as a checkpoint: the catalog's definitions and the state
  // the store owns. The caller holds the catalog mutex as well, which is what
  // makes the catalog it reads account for every frame in the log it replaces.
  void Compact(duckdb::ClientContext* context)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  // Hands the batch to the databases its ops name. Throws the store's error as
  // a SQL error. `position` is stamped on the transaction the ops run in when
  // they run on one of the store connection's own, so an inline batch records
  // its position the same way a statement's does.
  void RunStoreOps(duckdb::ClientContext* context,
                   std::span<const store_op::Targeted> store_ops,
                   uint64_t position)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_store_mutex);

  CatalogWal _wal;
  std::string _directory;
  std::string _data_directory;

  mutable absl::Mutex _mutex;
  // Serializes store-side DDL: it owns the data-DB connection. The data work
  // under it takes duckdb's WAL/table locks, so it must never nest inside
  // _mutex (only the reverse: _store_mutex -> _mutex around appends).
  absl::Mutex _store_mutex;

  // Every drop whose reclamation has not landed, keyed by the id it names. The
  // DropPrepare already took effect -- the object is invisible -- and what is
  // held is the record of what still has to be reclaimed.
  //
  // Compaction carries these rather than waiting for them: a cascade reclaim
  // can hold one open for as long as the artifact removal takes.
  containers::FlatHashMap<uint64_t, wal::DropPrepare> _open
    ABSL_GUARDED_BY(_mutex);
  // Encoding buffer for AppendFrame, kept at the high-water mark of the DDL
  // frames seen so far.
  duckdb::MemoryStream _frame_scratch ABSL_GUARDED_BY(_mutex);
  // The log position of the last frame appended. Counts frames, not records: a
  // frame is one batch and lands atomically, so it is the unit a database can
  // be in step with -- and the unit a consensus log would replicate.
  uint64_t _position ABSL_GUARDED_BY(_mutex) = 0;
  // Per database, the batches whose data half is not known to be durable, in
  // position order. A live batch is here from its append until the commit that
  // carried it acks; a replayed one until boot has caught the database up.
  // Compaction folds the log into state, so it has to wait for this to empty --
  // otherwise the very records the gap replay needs are the ones it discards.
  containers::FlatHashMap<uint64_t, std::vector<PendingBatch>> _unacked
    ABSL_GUARDED_BY(_mutex);
  // Records appended since the last checkpoint, against the number that
  // checkpoint wrote. The file is worth rewriting once it holds as much again
  // as the state costs -- the same doubling the byte rule below applies,
  // counted in records so a sequence-heavy workload, whose appends never grow
  // the state, still folds. Atomic: the sequence path appends off _mutex.
  std::atomic<uint64_t> _records_since_checkpoint{0};
  uint64_t _checkpoint_records ABSL_GUARDED_BY(_mutex) = 0;
  // Size of the file the last compaction produced, i.e. the live state written
  // out whole. Record counts say nothing about reclaimable bytes -- one dead
  // definition of a 1000-column table outweighs thousands of dead
  // SetSequence -- so growth past a multiple of this triggers as well.
  uint64_t _live_bytes ABSL_GUARDED_BY(_mutex) = 0;
  static constexpr uint64_t kLiveBytesGrowth = 2;
  // How often the sequence path bothers to look; the check needs _mutex, and a
  // nextval must not serialize on it.
  static constexpr uint64_t kSequenceCompactCheck = 4096;

  mutable absl::Mutex _seq_mutex;
  containers::FlatHashMap<uint64_t, uint64_t> _sequences
    ABSL_GUARDED_BY(_seq_mutex);
};

CatalogStore& GetCatalogStore();

// Hands the log position to the transaction `context` is about to commit on
// `database_id`, so its duckdb commit carries the store change and the position
// in one atomic write. Does nothing when the database is not attached on this
// transaction, which is the case for a batch with no data half.
void RecordCatalogPositionOnCommit(duckdb::ClientContext& context,
                                   ObjectId database_id, uint64_t position);

}  // namespace sdb::catalog
