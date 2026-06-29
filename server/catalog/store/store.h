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

#include <atomic>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "catalog/object.h"

namespace sdb::catalog {

// One serenedb table as a real table in the store database holding the row
// data. `name` is the full pg path as one identifier
// ("<database>.<schema>.<table>"); it is write-only -- identity stays
// ObjectId, nothing ever parses it back.
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
  ObjectId table_id;
  std::vector<StoreTableColumn> columns;
  // Indices into `columns`.
  std::vector<size_t> not_null;
  std::vector<std::string> pk_columns;
  std::vector<std::vector<std::string>> unique_constraints;
  std::vector<StoreForeignKey> foreign_keys;
  std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> checks;
};

inline constexpr std::string_view kStoreDatabaseName = "__sdb_store";

std::string StoreTableName(std::string_view database, std::string_view schema,
                           std::string_view table);

// Store-table name of a tombstoned (pending-drop / not-yet-committed CTAS)
// table. Composable from the ObjectId alone so drop recovery never needs the
// original names; cannot collide with StoreTableName output (no dots).
std::string DroppedStoreTableName(ObjectId table_id);

// Store-side name of an index: id-based ("sdb_idx_<id>"), so drops and
// recovery never need the user-facing name and facade renames are
// metadata-only.
std::string StoreIndexName(ObjectId index_id);

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
  // Inverted: raw column names for `USING inverted(...)`.
  std::vector<std::string> columns;
  // Plain (ART): per-key SQL rendered in order, ready to drop into the index
  // key list -- a quoted column identifier, or a parenthesized expression such
  // as "(j + k)". Empty for inverted indexes.
  std::vector<std::string> keys;
  Kind kind = Kind::Inverted;
  bool unique = false;
};

class Table;
class Index;

StoreTableDef MakeStoreTableDef(std::string_view database,
                                std::string_view schema, const Table& table);

// Store mirror of an index, or nullopt when the index is not mirrored
// (non-Transactional table, expression/INCLUDE columns, or ART-unfriendly
// key types).
std::optional<StoreIndexDef> MakeStoreIndexDef(std::string_view database,
                                               std::string_view schema,
                                               const Table& table,
                                               const Index& index);

// Catalog persistence: definitions and sequence counters stored as rows in
// tables of the engine's single-file database, attached as "__sdb_store".
// Writes are delete+insert by key; the tables carry no indexes, every read
// is a filtered scan (the catalog is tiny, the per-commit WAL fsync
// dominates any scan cost). The same database holds the store tables
// carrying serenedb table data; their DDL rides the same transaction as the
// catalog rows.
class CatalogStore {
 public:
  struct Key {
    ObjectId parent_id;
    ObjectType type{ObjectType::Invalid};
    ObjectId id;
  };

  // Transient handle for Write's callback. Caller mixes Put*/Drop* calls;
  // the whole batch commits atomically.
  class WriteContext {
   public:
    WriteContext(const WriteContext&) = delete;
    WriteContext& operator=(const WriteContext&) = delete;

    void PutDefinition(ObjectId parent_id, ObjectType type, ObjectId id,
                       std::string_view def);
    void PutSequence(ObjectId sequence_id, uint64_t value);
    void DropDefinition(ObjectId parent_id, ObjectType type, ObjectId id);
    void DropSequence(ObjectId sequence_id);
    void WriteTombstone(ObjectId parent_id, ObjectId id);

    void CreateStoreTable(StoreTableDef def);
    void DropStoreTable(std::string name);
    void RenameStoreTable(std::string name, std::string new_name);
    void RenameStoreColumn(std::string table, std::string name,
                           std::string new_name);
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
    void CreateStoreIndex(StoreIndexDef def);
    void DropStoreIndex(ObjectId index_id);

   private:
    friend class CatalogStore;

    enum class Op : uint8_t {
      PutDefinition,
      DropDefinition,
      PutSequence,
      DropSequence,
      CreateStoreTable,
      DropStoreTable,
      RenameStoreTable,
      RenameStoreColumn,
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
      // only `store_table.name` (+ `name_a`/`name_b` rename arguments).
      StoreTableDef store_table;
      StoreIndexDef store_index;
      std::string name_a;
      std::string name_b;
    };

    WriteContext() = default;

    std::vector<Entry> _entries;
  };

  inline static CatalogStore* gInstance = nullptr;
  static CatalogStore& instance() noexcept { return *gInstance; }

  CatalogStore();
  ~CatalogStore();

  // Attaches <datadir>/<store root>/store.db, creates the tables on first
  // boot, and seeds the system database. Fatal on failure.
  void Initialize(std::string_view database_directory);
  void Shutdown();

  Result CreateDefinition(ObjectId parent_id, ObjectType type, ObjectId id,
                          std::string_view def);
  Result Write(absl::FunctionRef<void(WriteContext&)> fill);
  Result DropDefinition(ObjectId parent_id, ObjectType type, ObjectId id);
  // Pair with DropDefinition(..., ObjectType::Sequence, id) to fully drop.
  Result DropSequence(ObjectId sequence_id);
  Result DropEntry(ObjectId parent_id, ObjectType type);
  Result DropEntry(ObjectId parent_id);
  Result WriteTombstone(ObjectId parent_id, ObjectId id);

  // Visits (parent_id, type) definitions ordered by id. A non-ok visitor
  // result stops the iteration and is returned.
  Result VisitDefinitions(
    ObjectId parent_id, ObjectType type,
    absl::FunctionRef<Result(Key, std::string_view)> visitor);

  // Boot load: one in-pipeline pass per table -- the sdb_init_catalog /
  // sdb_init_sequences in-out table functions consume the scan's vectors
  // directly (no result materialization). The loaded state serves the
  // hierarchical catalog walk (which must not issue per-(parent,type)
  // scans) and the per-sequence counter reads, until ReleaseBootState().
  Result LoadBootState();
  Result VisitBoot(
    ObjectId parent_id, ObjectType type,
    absl::FunctionRef<Result(Key, std::string_view)> visitor) const;
  bool TryGetBootSequenceValue(ObjectId sequence_id, uint64_t& value) const;
  void ReleaseBootState();

  // In-out function consumers; valid only inside LoadBootState().
  void BootConsumeCatalog(duckdb::DataChunk& input);
  void BootConsumeSequences(duckdb::DataChunk& input);
  uint64_t BootDefsLoaded() const;
  uint64_t BootSequencesLoaded() const;

  Result PutSequenceValue(ObjectId sequence_id, uint64_t value);
  // Missing counter reads as 0.
  Result GetSequenceValue(ObjectId sequence_id, uint64_t& value);

  // Boot-time sanity: the store table of a catalog table exists and its
  // column names/types match. Assert-only (no-op in release builds).
  void ValidateStoreTable(const StoreTableDef& def);

 private:
  struct BootDef {
    ObjectId id;
    std::string def;
  };

  Result ExecuteEntries(std::vector<WriteContext::Entry>& entries);
  Result ExecuteCreateStoreTable(const StoreTableDef& def);
  Result ExecuteCreateStoreTableImpl(const StoreTableDef& def,
                                     bool with_checks);
  void EnsureSystemDatabase();

  std::atomic<bool> _boot_loading = false;
  containers::FlatHashMap<std::pair<uint64_t, uint8_t>, std::vector<BootDef>>
    _boot_defs;
  containers::FlatHashMap<uint64_t, uint64_t> _boot_sequences;

  mutable absl::Mutex _mutex;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  duckdb::unique_ptr<duckdb::PreparedStatement> _delete_definition;
  duckdb::unique_ptr<duckdb::PreparedStatement> _insert_definition;
  duckdb::unique_ptr<duckdb::PreparedStatement> _delete_by_parent_type;
  duckdb::unique_ptr<duckdb::PreparedStatement> _delete_by_parent;
  duckdb::unique_ptr<duckdb::PreparedStatement> _select_definitions;
  duckdb::unique_ptr<duckdb::PreparedStatement> _delete_sequence_batch;
  duckdb::unique_ptr<duckdb::PreparedStatement> _insert_sequence_batch;

  mutable absl::Mutex _seq_mutex;
  duckdb::unique_ptr<duckdb::Connection> _seq_conn;
  duckdb::unique_ptr<duckdb::PreparedStatement> _select_sequence;
  duckdb::unique_ptr<duckdb::PreparedStatement> _delete_sequence;
  duckdb::unique_ptr<duckdb::PreparedStatement> _insert_sequence;
};

CatalogStore& GetCatalogStore();

// Registers the boot in-out table functions (sdb_init_catalog,
// sdb_init_sequences) with the instance.
void RegisterCatalogStoreFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::catalog
