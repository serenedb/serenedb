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

#include <rocksdb/slice.h>

#include <expected>
#include <magic_enum/magic_enum.hpp>
#include <memory>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/local_catalog.h"
#include "catalog/object.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/view.h"
#include "general_server/scheduler.h"
#include "general_server/state.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "storage_engine/engine_feature.h"
#include "utils/query_cache.h"
#include "vpack/builder.h"
#include "vpack/iterator.h"
#include "vpack/serializer.h"
#include "vpack/slice.h"

#ifdef SDB_CLUSTER
#include "cluster/cluster_feature.h"
#include "cluster/global_catalog.h"
#endif

namespace sdb::catalog {
namespace {

Result ErrorMeta(ErrorCode code, std::string_view object_type,
                 std::string_view error, vpack::Slice meta) {
  return {code,  "Failed to read ", object_type,  " metadata ', error: ",
          error, " metadata: ",     meta.toJson()};
}

Result CreateIndexDrop(RocksDBEngineCatalog& engine, ObjectId db_id,
                       ObjectId schema_id, ObjectId table_id, ObjectId index_id,
                       std::shared_ptr<IndexDrop>& drop,
                       vpack::Slice definition) {
  drop->parent_id = table_id;
  drop->id = index_id;
  drop->db_id = db_id;
  drop->schema_id = schema_id;
  IndexBaseOptions options;
  if (auto r = vpack::ReadTupleNothrow(definition, options); !r.ok()) {
    return r;
  }
  drop->type = options.type;
  return engine.VisitDefinitions(index_id, RocksDBEntryType::IndexShard,
                                 [&](DefinitionKey key, vpack::Slice) {
                                   SDB_ASSERT(!drop->shard_id.isSet());
                                   drop->shard_id = key.GetObjectId();
                                   return Result{};
                                 });
}

Result CreateTableDrop(RocksDBEngineCatalog& engine, ObjectId db_id,
                       ObjectId schema_id, ObjectId table_id,
                       std::shared_ptr<TableDrop>& drop) {
  drop->parent_id = schema_id;
  drop->id = table_id;

  auto r = engine.VisitDefinitions(table_id, RocksDBEntryType::TableShard,
                                   [&](DefinitionKey key, vpack::Slice) {
                                     SDB_ASSERT(!drop->shard_id.isSet());
                                     drop->shard_id = key.GetObjectId();
                                     return Result{};
                                   });
  if (!r.ok()) {
    return r;
  }
  return engine.VisitDefinitions(
    table_id, RocksDBEntryType::Index,
    [&](DefinitionKey key, vpack::Slice slice) {
      drop->indexes.push_back(std::make_shared<IndexDrop>());
      return CreateIndexDrop(engine, db_id, schema_id, table_id,
                             key.GetObjectId(), drop->indexes.back(), slice);
    });
}

Result CreateSchemaDrop(RocksDBEngineCatalog& engine, ObjectId db_id,
                        ObjectId schema_id, std::shared_ptr<SchemaDrop>& drop) {
  drop->parent_id = db_id;
  drop->id = schema_id;
  return engine.VisitDefinitions(
    schema_id, RocksDBEntryType::Table, [&](DefinitionKey key, vpack::Slice) {
      drop->tables.push_back(std::make_shared<TableDrop>());
      return CreateTableDrop(engine, db_id, schema_id, key.GetObjectId(),
                             drop->tables.back());
    });
}

Result CreateDatabaseDrop(RocksDBEngineCatalog& engine, ObjectId db_id,
                          std::shared_ptr<DatabaseDrop>& drop) {
  drop->parent_id = id::kInstance;
  drop->id = db_id;
  return engine.VisitDefinitions(
    db_id, RocksDBEntryType::Schema, [&](DefinitionKey key, vpack::Slice) {
      drop->schemas.push_back(std::make_shared<SchemaDrop>());
      return CreateSchemaDrop(engine, db_id, key.GetObjectId(),
                              drop->schemas.back());
    });
}

class OpenDatabase {
 public:
  OpenDatabase(LogicalCatalog& catalog) : _catalog{catalog} {}

  Result operator()() { return RegisterDatabases(); }
  Result AddRoles();

 private:
  Result RegisterDatabases();
  Result RegisterSchemas(ObjectId database_id);
  Result RegisterFunctions(ObjectId database_id, ObjectId schema_id);
  Result RegisterViews(ObjectId database_id, ObjectId schema_id);
  Result RegisterTableShard(ObjectId table_id);
  Result RegisterTables(ObjectId database_id, ObjectId schema_id);
  Result RegisterIndexShard(const std::shared_ptr<Index>& index);
  Result RegisterIndexes(ObjectId database_id, ObjectId schema_id,
                         ObjectId table_id);

  Result AddDatabase(ObjectId database_id, vpack::Slice definition);
  Result AddSchema(ObjectId database_id, ObjectId schema_id,
                   vpack::Slice definition);
  Result AddTable(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  vpack::Slice definition);
  Result AddIndex(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  ObjectId index_id, vpack::Slice definition);

  Result AddTableShard(ObjectId table_id, ObjectId shard_id,
                       vpack::Slice definition);
  Result AddIndexShard(ObjectId index_id, ObjectId shard_id,
                       vpack::Slice definition);

  void CollectDeletedDefinitions(ObjectId parent_id) {
    auto& engine = GetServerEngine();
    auto r = engine.VisitDefinitions(parent_id, RocksDBEntryType::Tombstone,
                                     [&](DefinitionKey key, vpack::Slice) {
                                       _deleted.insert(key.GetObjectId());
                                       return Result{};
                                     });
    SDB_ASSERT(r.ok());
  }

  LogicalCatalog& _catalog;
  containers::FlatHashSet<ObjectId> _deleted;
};

Result OpenDatabase::AddDatabase(ObjectId database_id,
                                 vpack::Slice definition) {
  catalog::DatabaseOptions database;
  if (auto r = vpack::ReadTupleNothrow(definition, database); !r.ok()) {
    return r;
  }
  auto db = std::make_shared<catalog::Database>(database_id, database);
  if (auto r = _catalog.RegisterDatabase(db); !r.ok()) {
    return r;
  }
  return RegisterSchemas(database_id);
}

Result OpenDatabase::RegisterDatabases() {
  CollectDeletedDefinitions(id::kInstance);
  return GetServerEngine().VisitDefinitions(
    id::kInstance, RocksDBEntryType::Database,
    [&](DefinitionKey key, vpack::Slice slice) {
      if (_deleted.contains(key.GetObjectId())) {
        auto db_drop = std::make_shared<DatabaseDrop>();
        db_drop->is_root = true;
        auto r =
          CreateDatabaseDrop(GetServerEngine(), key.GetObjectId(), db_drop);
        if (!r.ok()) {
          return r;
        }
        db_drop->Schedule().Detach();
        return Result{};
      } else {
        return AddDatabase(key.GetObjectId(), slice);
      }
    });
}

Result OpenDatabase::RegisterSchemas(ObjectId database_id) {
  CollectDeletedDefinitions(database_id);
  return GetServerEngine().VisitDefinitions(
    database_id, RocksDBEntryType::Schema,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto schema_id = key.GetObjectId();
      if (_deleted.contains(schema_id)) {
        auto schema_drop = std::make_shared<SchemaDrop>();
        schema_drop->is_root = true;
        auto r = CreateSchemaDrop(GetServerEngine(), database_id, schema_id,
                                  schema_drop);
        schema_drop->Schedule().Detach();
        return r;
      } else {
        return AddSchema(database_id, schema_id, slice);
      }
    });
}

Result OpenDatabase::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, RocksDBEntryType::Function,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      std::shared_ptr<catalog::Function> function;
      auto r = catalog::Function::Instantiate(function, db_id, slice, true);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "function", r.errorMessage(), slice);
      }

      return _catalog.RegisterFunction(db_id, schema_id, std::move(function));
    });
}

Result OpenDatabase::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitDefinitions(
    schema_id, RocksDBEntryType::View,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      ViewOptions options;
      auto r = ViewOptions::Read(options, slice);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "view", r.errorMessage(), slice);
      }
      std::shared_ptr<View> view;

      r =
        CreateViewInstance(view, db_id, std::move(options), ViewContext::User);
      if (!r.ok()) {
        return r;
      }

      return _catalog.RegisterView(schema_id, view);
    });
}

Result OpenDatabase::RegisterIndexes(ObjectId db_id, ObjectId schema_id,
                                     ObjectId table_id) {
  CollectDeletedDefinitions(table_id);
  return GetServerEngine().VisitDefinitions(
    table_id, RocksDBEntryType::Index,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto index_id = key.GetObjectId();
      if (_deleted.contains(index_id)) {
        auto index_drop = std::make_shared<IndexDrop>();
        index_drop->is_root = true;
        auto r = CreateIndexDrop(GetServerEngine(), db_id, schema_id, table_id,
                                 index_id, index_drop, slice);
        index_drop->Schedule().Detach();
        return r;
      } else {
        return AddIndex(db_id, schema_id, table_id, index_id, slice);
      }
    });
}

Result OpenDatabase::RegisterTableShard(ObjectId table_id) {
  CollectDeletedDefinitions(table_id);
  return GetServerEngine().VisitDefinitions(
    table_id, RocksDBEntryType::TableShard,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      ObjectId shard_id = key.GetObjectId();
      SDB_ASSERT(!_deleted.contains(shard_id));
      TableStats stats;
      if (auto r = vpack::ReadTupleNothrow(slice, stats); !r.ok()) {
        SDB_WARN("xxxxx", Logger::STARTUP,
                 "Failed to read table stats for table shard ", shard_id);
      }
      auto shard = std::make_shared<TableShard>(shard_id, table_id, stats);
      return _catalog.RegisterTableShard(shard);
    });
}

Result OpenDatabase::RegisterIndexShard(const std::shared_ptr<Index>& index) {
  CollectDeletedDefinitions(index->GetId());
  return GetServerEngine().VisitDefinitions(
    index->GetId(), RocksDBEntryType::IndexShard,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      SDB_ASSERT(!_deleted.contains(key.GetObjectId()));
      return index->CreateIndexShard(false, key.GetObjectId(), std::move(slice))
        .transform(
          [&](auto&& shard) { return _catalog.RegisterIndexShard(shard); })
        .error_or(Result{});
    });
}

Result OpenDatabase::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  CollectDeletedDefinitions(schema_id);
  return GetServerEngine().VisitDefinitions(
    schema_id, RocksDBEntryType::Table,
    [&](DefinitionKey key, vpack::Slice slice) -> Result {
      auto table_id = key.GetObjectId();
      if (_deleted.contains(table_id)) {
        auto table_drop = std::make_shared<TableDrop>();
        table_drop->is_root = true;
        auto r = CreateTableDrop(GetServerEngine(), db_id, schema_id, table_id,
                                 table_drop);
        table_drop->Schedule().Detach();
        return r;
      } else {
        return AddTable(db_id, schema_id, table_id, slice);
      }
    });
}

Result OpenDatabase::AddRoles() {
  auto& engine = GetServerEngine();
  auto r = engine.VisitDefinitions(
    id::kSystemDB, RocksDBEntryType::Role,
    [&](DefinitionKey, vpack::Slice slice) -> Result {
      SDB_ASSERT(!slice.get(StaticStrings::kDataSourceId).isNone());

      std::shared_ptr<catalog::Role> role;
      auto r = catalog::Role::Instantiate(role, slice, false);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "role", r.errorMessage(), slice);
      }

      return _catalog.RegisterRole(std::move(role));
    });

  if (!r.ok()) {
    return {r.errorNumber(), "Failed to read roles, error: ", r.errorMessage()};
  }

  return {};
}

Result OpenDatabase::AddTable(ObjectId db_id, ObjectId schema_id,
                              ObjectId table_id, vpack::Slice slice) {
  CreateTableOptions options;

  // TODO(gnusi): .skip_unknown = false, .strict = true
  if (auto r = vpack::ReadObjectNothrow<TableOptions>(
        slice, options, {.skip_unknown = true, .strict = false},
        ObjectInternal{db_id});
      !r.ok()) {
    return ErrorMeta(r.errorNumber(), "table", r.errorMessage(), slice);
  }

  auto r = _catalog.RegisterTable(db_id, schema_id, std::move(options));
  if (!r.ok()) {
    return r;
  }
  r = RegisterTableShard(table_id);
  if (!r.ok()) {
    return r;
  }
  return RegisterIndexes(db_id, schema_id, table_id);
}

Result OpenDatabase::AddIndex(ObjectId database_id, ObjectId schema_id,
                              ObjectId table_id, ObjectId index_id,
                              vpack::Slice slice) {
  IndexBaseOptions options;
  if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
    return r;
  }
  return _catalog
    .RegisterIndex(database_id, schema_id, index_id, table_id,
                   std::move(options))
    .transform([&](auto&& index) { return RegisterIndexShard(index); })
    .error_or(Result{});
}

Result OpenDatabase::AddSchema(ObjectId db_id, ObjectId schema_id,
                               vpack::Slice slice) {
  SchemaOptions options;
  if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
    return ErrorMeta(r.errorNumber(), "schema", r.errorMessage(), slice);
  }

  auto schema = std::make_shared<catalog::Schema>(db_id, std::move(options));

  if (auto r = _catalog.RegisterSchema(db_id, std::move(schema)); !r.ok()) {
    return r;
  }

  if (auto r = RegisterTables(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterViews(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterFunctions(db_id, schema_id); !r.ok()) {
    return r;
  }
  return Result{ERROR_OK};
}

}  // namespace

template<typename T>
ResultOr<std::shared_ptr<Database>> GetDatabaseImpl(T key) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto database = catalog.GetSnapshot()->GetDatabase(key);
  if (!database) [[unlikely]] {
    return std::unexpected<Result>(std::in_place,
                                   ERROR_SERVER_DATABASE_NOT_FOUND,
                                   "Cannot find database ", key);
  }
  return database;
}

CatalogFeature::CatalogFeature(Server& server)
  : SerenedFeature{server, name()} {}

void CatalogFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addOption(
    "--skip-background-errors",
    "Whether to attempt to continue in face of errors caused by background "
    "tasks; may result in inconsistent database state.",
    std::make_unique<options::BooleanParameter>(&_skip_background_errors));
}

void CatalogFeature::prepare() {
  auto catalog = std::make_shared<LocalCatalog>(_skip_background_errors);
  _global = catalog;
  _local = std::move(catalog);
}

void CatalogFeature::start() {
  auto r = Open();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void CatalogFeature::unprepare() {
  // TODO(gnusi): fix
  SDB_ASSERT(_local);
  SDB_ASSERT(_global);
  _local.reset();
  _global.reset();
}

void CatalogFeature::beginShutdown() {}

void CatalogFeature::stop() {
  aql::QueryCacheProperties p{
    .mode = aql::QueryCacheMode::CacheAlwaysOff,
    .max_results_count = 0,
    .max_results_size = 0,
    .max_entry_size = 0,
    .show_bind_vars = false,
  };
  aql::QueryCache::instance()->properties(p);
  aql::QueryCache::instance()->invalidate();
}

Result CatalogFeature::Open() {
  if (ServerState::instance()->IsCoordinator()) {
    return {};
  }

  OpenDatabase open_db{Local()};
  if (ServerState::instance()->IsSingle()) {
    if (auto r = open_db.AddRoles(); !r.ok()) {
      return r;
    }
  }

  auto r = open_db();

  if (!r.ok()) {
    SDB_FATAL("xxxxx", Logger::FIXME, "Failed to open database, ",
              r.errorMessage());
  }

  if (!catalog::GetDatabase(StaticStrings::kSystemDatabase)) {
    SDB_FATAL("xxxxx", Logger::FIXME, "No ", StaticStrings::kSystemDatabase,
              " database found in database directory");
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
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  return ServerState::instance()->IsCoordinator() ? catalogs.Global()
                                                  : catalogs.Local();
}

}  // namespace sdb::catalog
