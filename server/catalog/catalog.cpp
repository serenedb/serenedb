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

#include "catalog/catalog.h"

#include <absl/cleanup/cleanup.h>
#include <rocksdb/slice.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/serializer.h>
#include <vpack/slice.h>

#include <expected>
#include <iostream>
#include <magic_enum/magic_enum.hpp>
#include <memory>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/local_catalog.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "folly/Function.h"
#include "general_server/scheduler.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/secondary_index_shard.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

namespace sdb::catalog {
namespace {

Result ErrorMeta(ErrorCode code, std::string_view object_type,
                 std::string_view error, vpack::Slice meta) {
  return {code,  "Failed to read ", object_type,  " metadata ', error: ",
          error, " metadata: ",     meta.toJson()};
}

// In case of recovery the ColumnExpr shouldn't be parsed
struct DropTableOptions {
  uint64_t columns;
};

ResultOr<DropTableOptions> GetTableOptionsForDrop(vpack::Slice slice) {
  struct {
    vpack::Slice columns;
  } opts;
  if (auto r = vpack::ReadObjectNothrow(slice, opts, {.skip_unknown = true});
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (!opts.columns.isArray()) {
    return std::unexpected<Result>{
      std::in_place, ERROR_SERVER_ILLEGAL_STATE,
      "\"columns\" variable should be an array in the table definition vpack"};
  }
  return DropTableOptions{.columns = opts.columns.length()};
}

ResultOr<std::shared_ptr<IndexDrop>> CreateIndexDrop(
  RocksDBEngineCatalog& engine, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, ObjectId index_id, ObjectType index_type,
  bool is_root = false) {
  auto shard_type = IndexShardType(index_type);
  ObjectId shard_id;
  auto r = engine.VisitDefinitions(index_id, shard_type,
                                   [&](DefinitionKey key, vpack::Slice) {
                                     SDB_ASSERT(!shard_id.isSet());
                                     shard_id = key.GetObjectId();
                                     return Result{};
                                   });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  return std::make_shared<IndexDrop>(index_id, index_type, db_id, schema_id,
                                     table_id, is_root);
}

ResultOr<std::shared_ptr<TableDrop>> CreateTableDrop(
  RocksDBEngineCatalog& engine, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, uint64_t cols, bool is_root = false) {
  ObjectId shard_id;
  uint64_t table_size = std::numeric_limits<uint64_t>::max();

  auto r = engine.VisitDefinitions(
    table_id, ObjectType::TableShard,
    [&](DefinitionKey key, vpack::Slice slice) {
      SDB_ASSERT(!shard_id.isSet());
      shard_id = key.GetObjectId();
      TableStats stats;
      if (auto r = vpack::ReadTupleNothrow(slice, stats); !r.ok()) {
        return r;
      }
      table_size = stats.num_rows * cols;
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  std::vector<std::shared_ptr<IndexDrop>> indexes;
  auto collect_indexes = [&](ObjectType type) {
    return engine.VisitDefinitions(
      table_id, type, [&](DefinitionKey key, vpack::Slice) {
        auto index_drop = CreateIndexDrop(engine, db_id, schema_id, table_id,
                                          key.GetObjectId(), type);
        if (!index_drop) {
          return std::move(index_drop.error());
        }
        indexes.push_back(std::move(*index_drop));
        return Result{};
      });
  };
  r = collect_indexes(ObjectType::SecondaryIndex);
  if (r.ok()) {
    r = collect_indexes(ObjectType::InvertedIndex);
  }
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }

  std::vector<ObjectId> owned_sequences;
  r = engine.VisitDefinitions(
    schema_id, ObjectType::Sequence,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto seq = Sequence::ReadInternal(slice, {.id = key.GetObjectId(),
                                                .database_id = db_id,
                                                .schema_id = schema_id});
      if (seq && seq->GetOwnerTableId() == table_id) {
        owned_sequences.push_back(key.GetObjectId());
      }
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  return std::make_shared<TableDrop>(
    table_id, shard_id, table_size, std::move(indexes),
    std::move(owned_sequences), schema_id, is_root);
}

ResultOr<std::shared_ptr<SchemaDrop>> CreateSchemaDrop(
  RocksDBEngineCatalog& engine, ObjectId db_id, ObjectId schema_id,
  bool is_root = false) {
  std::vector<std::shared_ptr<TableDrop>> tables;
  auto r = engine.VisitDefinitions(
    schema_id, ObjectType::Table,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto options = GetTableOptionsForDrop(slice);
      if (!options) {
        return std::move(options.error());
      }
      auto table_drop = CreateTableDrop(engine, db_id, schema_id,
                                        key.GetObjectId(), options->columns);
      if (!table_drop) {
        return std::move(table_drop.error());
      }
      tables.push_back(std::move(*table_drop));
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }

  return std::make_shared<SchemaDrop>(schema_id, std::move(tables), db_id,
                                      is_root);
}

ResultOr<std::shared_ptr<DatabaseDrop>> CreateDatabaseDrop(
  RocksDBEngineCatalog& engine, ObjectId db_id) {
  std::vector<std::shared_ptr<SchemaDrop>> schemas;
  auto r = engine.VisitDefinitions(
    db_id, ObjectType::Schema, [&](DefinitionKey key, vpack::Slice) {
      auto schema_drop = CreateSchemaDrop(engine, db_id, key.GetObjectId());
      if (!schema_drop) {
        return std::move(schema_drop.error());
      }
      schemas.push_back(std::move(*schema_drop));
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  return std::make_shared<DatabaseDrop>(db_id, std::move(schemas));
}

class OpenDatabase {
 public:
  enum class DeletedScope : uint8_t {
    Root = 0,  // deleted object with parent = id::kInstance
    Database,  // parent is daatabase
    Schema,    // parent is schema
    Relation,  // parent is relation
  };

  OpenDatabase(LogicalCatalog& catalog) : _catalog{catalog} {}

  Result operator()() {
    CollectDeletedDefinitions(id::kInstance, DeletedScope::Root);
    auto r = RegisterDatabases();
    ClearDeletedDefinitions(DeletedScope::Root);
    return r;
  }

  Result AddRoles();

 private:
  void Resolve();

  Result RegisterDatabases();
  Result RegisterSchemas(ObjectId database_id);
  Result RegisterFunctions(ObjectId database_id, ObjectId schema_id);
  Result RegisterTokenizers(ObjectId database_id, ObjectId schema_id);
  Result RegisterViews(ObjectId database_id, ObjectId schema_id);
  // Sequences load in two passes: standalone before tables/views (so refs
  // to them resolve at the referrer's own registration), owned after
  // tables.
  Result RegisterSequences(ObjectId database_id, ObjectId schema_id,
                           bool owned);
  Result RegisterTypes(ObjectId database_id, ObjectId schema_id);
  Result RegisterTableShard(ObjectId table_id);
  Result RegisterTables(ObjectId database_id, ObjectId schema_id);
  Result RegisterIndexShard(const std::shared_ptr<Index>& index);
  Result RegisterIndexes(ObjectId database_id, ObjectId schema_id,
                         ObjectId table_id);

  Result AddDatabase(ObjectId database_id, vpack::Slice definition);
  Result AddSchema(ObjectId database_id, ObjectId schema_id,
                   vpack::Slice definition);
  Result AddTable(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  std::shared_ptr<Table> table);
  Result AddIndex(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  ObjectId index_id, ObjectType entry_type,
                  vpack::Slice definition);

  Result AddTableShard(ObjectId table_id, ObjectId shard_id,
                       vpack::Slice definition);
  Result AddIndexShard(ObjectId index_id, ObjectId shard_id,
                       vpack::Slice definition);

  bool IsDeleted(ObjectId id, DeletedScope scope) {
    return _deleted[magic_enum::enum_integer(scope)].contains(id);
  }

  void ClearDeletedDefinitions(DeletedScope scope) noexcept {
    _deleted[magic_enum::enum_integer(scope)].clear();
  }

  void CollectDeletedDefinitions(ObjectId id, DeletedScope scope) {
    auto& engine = GetServerEngine();
    auto& deleted = _deleted[magic_enum::enum_integer(scope)];
    SDB_ASSERT(deleted.empty());
    auto r = engine.VisitDefinitions(id, ObjectType::Tombstone,
                                     [&](DefinitionKey key, vpack::Slice) {
                                       deleted.insert(key.GetObjectId());
                                       return Result{};
                                     });
    SDB_ASSERT(r.ok());
  }

  LogicalCatalog& _catalog;

  std::array<containers::FlatHashSet<ObjectId>,
             magic_enum::enum_count<DeletedScope>()>
    _deleted;
};

Result OpenDatabase::AddDatabase(ObjectId database_id,
                                 vpack::Slice definition) {
  auto db = catalog::Database::ReadInternal(definition, {.id = database_id});
  if (!db) {
    return Result{ERROR_INTERNAL, "Failed to read database definition"};
  }
  if (auto r = _catalog.RegisterDatabase(db); !r.ok()) {
    return r;
  }
  CollectDeletedDefinitions(database_id, DeletedScope::Database);
  auto r = RegisterSchemas(database_id);
  ClearDeletedDefinitions(DeletedScope::Database);
  return r;
}

Result OpenDatabase::RegisterDatabases() {
  return GetServerEngine().VisitDefinitions(
    id::kInstance, ObjectType::Database,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      if (!IsDeleted(key.GetObjectId(), DeletedScope::Root)) {
        return AddDatabase(key.GetObjectId(), slice);
      }
      auto drop = CreateDatabaseDrop(GetServerEngine(), key.GetObjectId());
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterSchemas(ObjectId database_id) {
  return GetServerEngine().VisitDefinitions(
    database_id, ObjectType::Schema,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto schema_id = key.GetObjectId();
      if (!IsDeleted(key.GetObjectId(), DeletedScope::Database)) {
        return AddSchema(database_id, schema_id, slice);
      }

      auto drop = CreateSchemaDrop(GetServerEngine(), database_id,
                                   key.GetObjectId(), true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::PgSqlFunction,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto function =
        catalog::PgSqlFunction::ReadInternal(slice, {
                                                      .id = key.GetObjectId(),
                                                      .database_id = db_id,
                                                      .schema_id = schema_id,
                                                    });
      if (!function) {
        return ErrorMeta(ERROR_INTERNAL, "function",
                         "Failed to read function definition", slice);
      }
      return _catalog.RegisterFunction(db_id, schema_id, std::move(function));
    });
}

Result OpenDatabase::RegisterTokenizers(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::Tokenizer,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto tokenizer = Tokenizer::ReadInternal(slice, {
                                                        .id = key.GetObjectId(),
                                                        .database_id = db_id,
                                                        .schema_id = schema_id,
                                                      });
      if (!tokenizer) {
        return ErrorMeta(ERROR_INTERNAL, "tokenizer",
                         "Failed to read tokenizer definition", slice);
      }
      return _catalog.RegisterTokenizer(db_id, schema_id, std::move(tokenizer));
    });
}

Result OpenDatabase::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::PgSqlView,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto view_id = key.GetObjectId();
      auto view = PgSqlView::ReadInternal(
        slice, {.id = view_id, .database_id = db_id, .schema_id = schema_id});
      if (!view) {
        return ErrorMeta(ERROR_INTERNAL, "view",
                         "Failed to read view definition", slice);
      }
      if (auto r = _catalog.RegisterView(schema_id, std::move(view)); !r.ok()) {
        return r;
      }
      CollectDeletedDefinitions(view_id, DeletedScope::Relation);
      irs::Finally cleanup = [&] noexcept {
        ClearDeletedDefinitions(DeletedScope::Relation);
      };
      return RegisterIndexes(db_id, schema_id, view_id);
    });
}

Result OpenDatabase::RegisterSequences(ObjectId db_id, ObjectId schema_id,
                                       bool owned) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::Sequence,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto seq = Sequence::ReadInternal(slice, {
                                                 .id = key.GetObjectId(),
                                                 .database_id = db_id,
                                                 .schema_id = schema_id,
                                               });
      if (!seq) {
        return ErrorMeta(ERROR_INTERNAL, "sequence",
                         "Failed to read sequence definition", slice);
      }
      if (seq->GetOwnerTableId().isSet() != owned) {
        return Result{};
      }
      // Owner table is tombstoned -> skip; its DropTask will clean the seq.
      if (auto owner = seq->GetOwnerTableId();
          owner.isSet() && IsDeleted(owner, DeletedScope::Schema)) {
        return {};
      }
      return _catalog.RegisterSequence(db_id, schema_id, std::move(seq));
    });
}

Result OpenDatabase::RegisterTypes(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::PgSqlType,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto type = PgSqlType::ReadInternal(slice, {
                                                   .id = key.GetObjectId(),
                                                   .database_id = db_id,
                                                   .schema_id = schema_id,
                                                 });
      if (!type) {
        return ErrorMeta(ERROR_INTERNAL, "type",
                         "Failed to read type definition", slice);
      }
      return _catalog.RegisterType(db_id, schema_id, std::move(type));
    });
}

Result OpenDatabase::RegisterIndexes(ObjectId db_id, ObjectId schema_id,
                                     ObjectId table_id) {
  auto visit = [&](ObjectType type) {
    return GetServerEngine().VisitDefinitions(
      table_id, type, [&](DefinitionKey key, vpack::Slice slice) -> Result {
        auto index_id = key.GetObjectId();
        if (!IsDeleted(index_id, DeletedScope::Relation)) {
          return AddIndex(db_id, schema_id, table_id, index_id, type, slice);
        }

        auto drop = CreateIndexDrop(GetServerEngine(), db_id, schema_id,
                                    table_id, key.GetObjectId(), type, true);
        if (!drop) {
          return std::move(drop.error());
        }
        DropTask::Schedule(std::move(*drop)).Detach();
        return {};
      });
  };
  if (auto r = visit(ObjectType::SecondaryIndex); !r.ok()) {
    return r;
  }
  if (auto r = visit(ObjectType::InvertedIndex); !r.ok()) {
    return r;
  }
  return {};
}

Result OpenDatabase::RegisterTableShard(ObjectId table_id) {
  return GetServerEngine().VisitDefinitions(
    table_id, ObjectType::TableShard,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      ObjectId shard_id = key.GetObjectId();
      SDB_ASSERT(!IsDeleted(shard_id, DeletedScope::Relation));
      TableStats stats;
      if (auto r = vpack::ReadTupleNothrow(slice, stats); !r.ok()) {
        return r;
      }
      auto shard = std::make_shared<TableShard>(shard_id, table_id, stats);
      return _catalog.RegisterTableShard(std::move(shard));
    });
}

Result OpenDatabase::RegisterIndexShard(const std::shared_ptr<Index>& index) {
  return GetServerEngine().VisitDefinitions(
    index->GetId(), IndexShardType(index->GetType()),
    [&](DefinitionKey key, vpack::Slice /*slice*/) -> Result {
      auto shard = index->CreateIndexShard(false, key.GetObjectId());
      if (!shard) {
        return std::move(shard.error());
      }
      SDB_ASSERT(*shard);
      return _catalog.RegisterIndexShard(std::move(*shard));
    });
}

Result OpenDatabase::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, ObjectType::Table,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto table_id = key.GetObjectId();
      if (!IsDeleted(table_id, DeletedScope::Schema)) {
        auto table = Table::ReadInternal(
          slice,
          {.id = table_id, .database_id = db_id, .schema_id = schema_id});
        if (!table) {
          return Result{ERROR_INTERNAL, "Failed to read table definition"};
        }
        return AddTable(db_id, schema_id, table_id, std::move(table));
      }
      auto options = GetTableOptionsForDrop(slice);
      if (!options) {
        return std::move(options.error());
      }
      auto drop = CreateTableDrop(GetServerEngine(), db_id, schema_id, table_id,
                                  options->columns, true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::AddRoles() {
  auto& engine = GetServerEngine();
  auto r = engine.VisitDefinitions(
    id::kInstance, ObjectType::Role,
    [&](DefinitionKey, vpack::Slice slice) -> Result {
      SDB_ASSERT(!slice.get(StaticStrings::kDataSourceId).isNone());

      auto role = catalog::Role::ReadInternal(slice, {});
      if (!role) {
        return ErrorMeta(ERROR_INTERNAL, "role",
                         "Failed to read role definition", slice);
      }

      return _catalog.RegisterRole(std::move(role));
    });

  if (!r.ok()) {
    return {r.errorNumber(), "Failed to read roles, error: ", r.errorMessage()};
  }

  return {};
}

Result OpenDatabase::AddTable(ObjectId db_id, ObjectId schema_id,
                              ObjectId table_id, std::shared_ptr<Table> table) {
  auto r = _catalog.RegisterTable(db_id, schema_id, std::move(table));
  if (!r.ok()) {
    return r;
  }
  CollectDeletedDefinitions(table_id, DeletedScope::Relation);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Relation);
  };
  r = RegisterTableShard(table_id);
  if (!r.ok()) {
    return r;
  }
  return RegisterIndexes(db_id, schema_id, table_id);
}

Result OpenDatabase::AddIndex(ObjectId database_id, ObjectId schema_id,
                              ObjectId table_id, ObjectId index_id,
                              ObjectType entry_type, vpack::Slice slice) {
  SDB_ASSERT(slice.isObject(), "Index definition is not an object");
  ReadContext ctx{.id = index_id,
                  .database_id = database_id,
                  .schema_id = schema_id,
                  .relation_id = table_id};
  std::shared_ptr<Index> index;
  if (entry_type == ObjectType::SecondaryIndex) {
    index = SecondaryIndex::ReadInternal(slice, ctx);
  } else {
    index = InvertedIndex::ReadInternal(slice, ctx);
  }
  if (!index) {
    return Result{ERROR_INTERNAL, "Failed to read index definition"};
  }
  if (auto r = _catalog.RegisterIndex(database_id, schema_id, index); !r.ok()) {
    return r;
  }
  Result r;

#ifdef SDB_DEV
  // Check there are no tombstones in index scope
  size_t counter = 0;
  r = GetServerEngine().VisitDefinitions(index_id, ObjectType::Tombstone,
                                         [&](DefinitionKey, vpack::Slice) {
                                           counter++;
                                           return Result{};
                                         });
  if (!r.ok()) {
    return r;
  }
  SDB_ASSERT(counter == 0);
#endif

  r = RegisterIndexShard(index);
  return r;
}

Result OpenDatabase::AddSchema(ObjectId db_id, ObjectId schema_id,
                               vpack::Slice slice) {
  auto schema = catalog::Schema::ReadInternal(slice, {.database_id = db_id});
  if (!schema) {
    return ErrorMeta(ERROR_INTERNAL, "schema",
                     "Failed to read schema definition", slice);
  }

  if (auto r = _catalog.RegisterSchema(db_id, std::move(schema)); !r.ok()) {
    return r;
  }

  CollectDeletedDefinitions(schema_id, DeletedScope::Schema);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Schema);
  };

  if (auto r = RegisterTokenizers(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterTypes(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterSequences(db_id, schema_id, false); !r.ok()) {
    return r;
  }
  if (auto r = RegisterFunctions(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterTables(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterViews(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterSequences(db_id, schema_id, true); !r.ok()) {
    return r;
  }
  return {};
}

}  // namespace

template<typename T>
ResultOr<std::shared_ptr<Database>> GetDatabaseImpl(T key) {
  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto database = catalog.GetCatalogSnapshot()->GetDatabase(key);
  if (!database) [[unlikely]] {
    return std::unexpected<Result>(std::in_place,
                                   ERROR_SERVER_DATABASE_NOT_FOUND,
                                   "Cannot find database ", key);
  }
  return database;
}

CatalogFeature::CatalogFeature() { gInstance = this; }

CatalogFeature::~CatalogFeature() { gInstance = nullptr; }

void CatalogFeature::start() {
  // NOTE: stop() is intentionally empty in the header. _local / _global
  // must remain valid past every feature's stop() because
  // EngineFeature::stop() drains the rocksdb background thread, which
  // calls SyncStats -> GetCatalog(). The CatalogFeature dtor releases
  // the shared_ptrs once all features are torn down.
  auto catalog = std::make_shared<LocalCatalog>();
  _global = catalog;
  _local = std::move(catalog);

  auto r = Open();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

Result CatalogFeature::Open() {
  OpenDatabase open_db{Local()};
  if (auto r = open_db.AddRoles(); !r.ok()) {
    return r;
  }

  // Bootstrap the default `postgres` role on first start. AddRoles() has
  // already loaded any persisted roles; if none exist we mint the root user
  // and persist it via Local().CreateRole(), which writes it to RocksDB so it
  // survives across restarts. RBAC isn't implemented yet, so the password is
  // empty and auth checks are skipped.
  if (Local().GetCatalogSnapshot()->GetRoles().empty()) {
    auto root = Role::NewUser(StaticStrings::kDefaultUser, "", id::kRootUser);
    if (auto br = Local().CreateRole(std::move(root)); !br.ok()) {
      return br;
    }
  }

  auto r = open_db();

  if (!r.ok()) {
    SDB_FATAL(GENERAL, "Failed to open database, ", r.errorMessage());
  }

  if (auto fr = Local().FinalizeLoad(); !fr.ok()) {
    SDB_FATAL(GENERAL, "FinalizeLoad failed: ", fr.errorMessage());
  }

  if (!catalog::GetDatabase(StaticStrings::kDefaultDatabase)) {
    SDB_FATAL(GENERAL, "No ", StaticStrings::kDefaultDatabase,
              " database found in database directory");
  }

  // Attach all existing databases into DuckDB
  {
    auto snapshot = GetCatalog().GetCatalogSnapshot();
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    for (auto& db : snapshot->GetDatabases()) {
      auto query = absl::StrCat("ATTACH '", db->GetId().id(), "' AS \"",
                                db->GetName(), "\" (TYPE serenedb)");
      auto result = conn->Query(query);
      if (result->HasError()) {
        SDB_FATAL(GENERAL, "Failed to attach database ", db->GetName(), ": ",
                  result->GetError());
      }
    }
  }

  return r;
}

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id) {
  return GetDatabaseImpl(database_id);
}

ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name) {
  return GetDatabaseImpl(name);
}

LogicalCatalog& GetCatalog() {
  return catalog::CatalogFeature::instance().Local();
}

}  // namespace sdb::catalog
