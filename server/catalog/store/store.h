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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "catalog/object.h"

namespace sdb::catalog {

// Catalog persistence: definitions and sequence counters stored as rows in
// tables of the engine's single-file database, attached as "__sdb_store".
// Writes are delete+insert by key; the tables carry no indexes, every read
// is a filtered scan (the catalog is tiny, the per-commit WAL fsync
// dominates any scan cost).
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

   private:
    friend class CatalogStore;

    enum class Op : uint8_t {
      kPutDefinition,
      kDropDefinition,
      kPutSequence,
      kDropSequence,
    };

    struct Entry {
      Op op;
      Key key;
      uint64_t sequence_value = 0;
      std::string def;
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

 private:
  struct BootDef {
    ObjectId id;
    std::string def;
  };

  Result ExecuteEntries(std::vector<WriteContext::Entry>& entries);
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
