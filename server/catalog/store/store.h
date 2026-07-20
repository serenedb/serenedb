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
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/object.h"
#include "catalog/store/wal.h"

namespace sdb::catalog {

// One serenedb table as a real table in the store database holding the row
// data. Every store-side name is derived from the catalog id (t<table_id>,
// c<column_id>, i<index_id>): ids are immutable, so facade renames never
// touch the store and recovery keys are rename-proof.
struct StoreTableColumn {
  std::string name;
  duckdb::LogicalType type;
};

struct StoreForeignKey {
  std::vector<std::string> columns;
  std::string referenced_table;
  std::vector<std::string> referenced_columns;
};

struct StoreTableDef {
  std::string name;
  std::vector<StoreTableColumn> columns;
  // Indices into `columns`.
  std::vector<size_t> not_null;
  std::vector<std::string> pk_columns;
  std::vector<std::vector<std::string>> unique_constraints;
  std::vector<StoreForeignKey> foreign_keys;
  std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> checks;
};

inline constexpr std::string_view kStoreDatabaseName = "__sdb_data";

std::string StoreTableName(ObjectId table_id);

std::string StoreColumnName(ObjectId column_id);

std::string StoreIndexName(ObjectId index_id);

// Id of a store-side name ("t<id>" / "c<id>" / "i<id>") for the given
// prefix, or nullopt when `name` is not of that shape.
std::optional<ObjectId> ParseStoreId(char prefix, std::string_view name);

// The store-side duckdb entry of a catalog table.
duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found);

struct StoreIndexDef {
  enum class Kind : uint8_t {
    // Native ART index on the store table (btree/secondary in PG terms).
    Plain,
    // Inverted-index linkage: store-side BoundIndex feeding iresearch.
    Inverted,
  };

  std::string table;
  ObjectId table_id;
  ObjectId index_id;
  // Plain (ART): per-key SQL rendered in order, ready to drop into the index
  // key list -- a quoted column identifier, or a parenthesized expression such
  // as "(j + k)". Empty for inverted indexes.
  std::vector<std::string> keys;
  Kind kind = Kind::Inverted;
  bool unique = false;
};

class Table;
class Index;

StoreTableDef MakeStoreTableDef(const Table& table);

// Renders one scalar SQL expression with facade column references mapped to
// the store table's c<id> columns; nullopt when the text does not parse as
// exactly one expression.
std::optional<std::string> RewriteExprToStoreNames(std::string_view sql_expr,
                                                   const Table& table);

// The inverse: maps c<id> references back to facade column names (used when
// rendering store-side error texts for users).
std::optional<std::string> RewriteExprToFacadeNames(std::string_view sql_expr,
                                                    const Table& table);

// Store mirror of an index, or nullopt when the index is not mirrored
// (non-Transactional table, expression/INCLUDE columns, or ART-unfriendly
// key types).
std::optional<StoreIndexDef> MakeStoreIndexDef(const Table& table,
                                               const Index& index);

// The catalog's persistent form: an append-only WAL of record frames under
// <datadir>/engine_catalog/, replayed into resident maps at boot. Store-table
// DDL executes against the separate data database (see DataStore) around the
// WAL append -- the append is the DDL's single ack point:
//
//   - constructive store ops commit on the data DB BEFORE the append (a crash
//     leaves an orphan the boot reconciler drops);
//   - destructive store ops run AFTER the append (a crash leaves leftovers
//     the reconciler finishes);
//   - order-sensitive batches (mixed classes, column retypes) bracket the
//     data transaction with a PendingAlter flag: flag append, data commit,
//     final append with the definitions; the reconciler rolls forward or
//     back by comparing store shapes against the flag's payload.
class CatalogStore {
 public:
  struct Key {
    ObjectId parent_id;
    ObjectType type{ObjectType::Invalid};
    ObjectId id;
  };

  enum class Op : uint8_t {
    PutDefinition,
    DropDefinition,
    PutSequence,
    DropSequence,
    DropByParentType,
    DropByParent,
    // Monotonic sequence-horizon bump: replays as max-merge, so appends may
    // land out of order (they run outside the sequence lock and group-commit
    // freely). PutSequence stays the ordered, authoritative assign (setval,
    // creation seed, compaction snapshot).
    AdvanceSequence,
    CreateStoreTable,
    DropStoreTable,
    DropStoreColumn,
    AddStoreColumn,
    ChangeStoreColumnType,
    DropStoreForeignKey,
    DropStoreCheck,
    DropStoreNotNull,
    AddStoreNotNull,
    AddStoreCheck,
    AddStorePrimaryKey,
    AddStoreUnique,
    CreateStoreIndex,
    DropStoreIndex,
  };

  struct Entry {
    Op op;
    Key key;
    uint64_t sequence_value = 0;
    std::string def;
    // CreateStoreTable: the full definition; other store-table ops use
    // only `store_table.name` (+ `name_a`/`name_b` arguments).
    StoreTableDef store_table;
    StoreIndexDef store_index;
    // CreateStoreIndex on an inverted index only: the executor builds the
    // injected bound index from these. Never serialized.
    std::shared_ptr<const Table> table_obj;
    std::shared_ptr<const Index> index_obj;
    std::string name_a;
    std::string name_b;
  };

  // Transient handle for Write's callback. Caller mixes Put*/Drop* calls;
  // the whole batch acks atomically at its WAL append.
  class WriteContext {
   public:
    WriteContext(const WriteContext&) = delete;
    WriteContext& operator=(const WriteContext&) = delete;

    void PutDefinition(ObjectId parent_id, ObjectType type, ObjectId id,
                       std::string_view def);
    void PutSequence(ObjectId sequence_id, uint64_t value);
    void DropDefinition(ObjectId parent_id, ObjectType type, ObjectId id);
    void DropSequence(ObjectId sequence_id);
    void DropEntry(ObjectId parent_id, ObjectType type);
    void DropEntry(ObjectId parent_id);
    void WriteTombstone(ObjectId parent_id, ObjectId id);

    void CreateStoreTable(StoreTableDef def);
    void DropStoreTable(std::string name);
    void DropStoreColumn(std::string table, std::string name);
    // Adds a column. `type_sql` is the SQL type text; `default_sql` is the
    // DEFAULT expression text (empty for none) used to backfill existing rows.
    void AddStoreColumn(std::string table, std::string name,
                        std::string type_sql, std::string default_sql);
    // Changes a column's type. `using_sql` is the USING cast text (empty for
    // the implicit cast).
    void ChangeStoreColumnType(std::string table, std::string name,
                               std::string type_sql, std::string using_sql);
    // Removes the FK linkage entry on `table` that references/backs
    // `other` (symmetric: PK-side back-reference or the FK itself).
    void DropStoreForeignKey(std::string table, std::string other);
    // Removes the CHECK constraint with this expression text.
    void DropStoreCheck(std::string table, std::string expr);
    void DropStoreNotNull(std::string table, std::string column);
    void AddStoreNotNull(std::string table, std::string column);
    // Adds the CHECK constraint with this expression text; the store verifies
    // it against existing rows (mirrors DropStoreCheck).
    void AddStoreCheck(std::string table, std::string expr);
    // Adds a PRIMARY KEY (recreates storage, validates existing rows: no
    // duplicates, no nulls). `columns` are store-table column names in key
    // order.
    void AddStorePrimaryKey(std::string table,
                            std::vector<std::string> columns);
    // Adds a UNIQUE constraint over `columns` (recreate + existing-row dup
    // validation).
    void AddStoreUnique(std::string table, std::vector<std::string> columns);
    // Inverted defs carry the catalog objects so the executor can build the
    // injected bound index; ART defs run as store-side SQL.
    void CreateStoreIndex(StoreIndexDef def, std::shared_ptr<const Table> table,
                          std::shared_ptr<const Index> index);
    void DropStoreIndex(ObjectId index_id, ObjectId relation_id);

   private:
    friend class CatalogStore;

    WriteContext() = default;

    std::vector<Entry> _entries;
  };

  inline static CatalogStore* gInstance = nullptr;
  static CatalogStore& instance() noexcept { return *gInstance; }

  CatalogStore();
  ~CatalogStore();

  // Opens <datadir>/engine_catalog/catalog.wal, replays it into the resident
  // maps, and seeds the system database. Fatal on failure.
  void Initialize(std::string_view database_directory);
  void Shutdown();

  void CreateDefinition(ObjectId parent_id, ObjectType type, ObjectId id,
                        std::string_view def);
  void Write(absl::FunctionRef<void(WriteContext&)> fill);
  void DropDefinition(ObjectId parent_id, ObjectType type, ObjectId id);
  // Pair with DropDefinition(..., ObjectType::Sequence, id) to fully drop.
  void DropSequence(ObjectId sequence_id);
  void DropEntry(ObjectId parent_id, ObjectType type);
  void DropEntry(ObjectId parent_id);
  void WriteTombstone(ObjectId parent_id, ObjectId id);

  // Visits (parent_id, type) definitions ordered by id. Returning false from
  // the visitor stops the iteration.
  void VisitDefinitions(ObjectId parent_id, ObjectType type,
                        absl::FunctionRef<bool(Key, std::string_view)> visitor);

  // Visits a copied def list, so visitors may nest visits or write back into
  // the store (boot registration and drop planning both do).
  void VisitBoot(ObjectId parent_id, ObjectType type,
                 absl::FunctionRef<bool(Key, std::string_view)> visitor) const;
  bool TryGetBootSequenceValue(ObjectId sequence_id, uint64_t& value) const;

  void PutSequenceValue(ObjectId sequence_id, uint64_t value);
  // Max-merge horizon bump (Op::AdvanceSequence); safe to call concurrently
  // for one sequence, appends group-commit. Returns once durable.
  void AdvanceSequenceValue(ObjectId sequence_id, uint64_t value);
  // Missing counter reads as 0.
  void GetSequenceValue(ObjectId sequence_id, uint64_t& value);

  // Boot-time sanity: the store table of a catalog table exists and its
  // column names/types match. Assert-only (no-op in release builds).
  void ValidateStoreTable(const StoreTableDef& def);

  // The pending order-sensitive batch, if a crash interrupted one: the
  // definitions its final append would have written. Consumed by the boot
  // reconciler, which rolls forward (CommitPendingAlter) once the data DB
  // matches the payload, or back (AbortPendingAlter).
  std::optional<std::vector<Entry>> TakePendingAlter();
  void CommitPendingAlter(std::vector<Entry> records);
  void AbortPendingAlter();

  CatalogWal::Stats WalStats() const { return _wal.GetStats(); }

  // Snapshot of the resident record maps for introspection
  // (sdb_catalog_snapshot); ordered like a compaction would write.
  void VisitSnapshot(
    absl::FunctionRef<void(Key, std::string_view)> def_visitor,
    absl::FunctionRef<void(ObjectId, uint64_t)> sequence_visitor);

  // Decodes one wal frame's records (sdb_catalog_wal).
  static std::vector<Entry> ParseFrame(std::span<const uint8_t> frame);

  std::string_view WalDirectory() const noexcept { return _directory; }

 private:
  struct BootDef {
    ObjectId id;
    std::string def;
  };

  using DefMap =
    containers::FlatHashMap<std::pair<uint64_t, uint8_t>, std::vector<BootDef>>;

  void AppendBatch(std::span<const Entry> records)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void ApplyRecords(std::span<const Entry> records)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void MaybeCompact() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void EnsureSystemDatabase();

  CatalogWal _wal;
  std::string _directory;

  mutable absl::Mutex _mutex;
  DefMap _defs ABSL_GUARDED_BY(_mutex);
  uint64_t _live_records ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _dead_records ABSL_GUARDED_BY(_mutex) = 0;

  mutable absl::Mutex _seq_mutex;
  containers::FlatHashMap<uint64_t, uint64_t> _sequences
    ABSL_GUARDED_BY(_seq_mutex);
};

CatalogStore& GetCatalogStore();

}  // namespace sdb::catalog
