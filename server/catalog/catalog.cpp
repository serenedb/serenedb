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

containers::FlatHashSet<ObjectId> CollectObjects(ObjectId parent_id,
                                                 RocksDBEntryType type) {
  auto& engine = GetServerEngine();
  containers::FlatHashSet<ObjectId> drops;
  auto r =
    engine.VisitObjects(parent_id, type, [&](rocksdb::Slice key, vpack::Slice) {
      drops.insert(RocksDBKey::GetObjectId(key));
      return Result{};
    });
  SDB_ASSERT(r.ok());
  return drops;
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

void CatalogFeature::start() {}

void CatalogFeature::unprepare() {
  // TODO(gnusi): fix
  SDB_ASSERT(_local);
  SDB_ASSERT(_global);
  //_local.reset();
  //_global.reset();
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

  auto r = ProcessTombstones();
  if (!r.ok()) {
    return r;
  }

  if (ServerState::instance()->IsSingle()) {
    if (auto r = AddRoles(); !r.ok()) {
      return r;
    }
  }

  r = GetServerEngine().VisitDatabases([&](vpack::Slice slice) -> Result {
    catalog::DatabaseOptions database;
    if (auto r = vpack::ReadTupleNothrow(slice, database); !r.ok()) {
      return r;
    }

    if (auto r = OpenDatabase(std::move(database)); !r.ok()) {
      return r;
    }
    return {};
  });

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

Result CatalogFeature::AddDatabase(const DatabaseOptions& options) {
  return Global().RegisterDatabase(
    std::make_shared<catalog::Database>(options));
}

Result CatalogFeature::RegisterSchemas(ObjectId database_id) {
  auto deleted = CollectObjects(database_id, RocksDBEntryType::ScopeTombstone);
  return GetServerEngine().VisitObjects(
    database_id, RocksDBEntryType::Schema,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      auto schema_id = RocksDBKey::GetObjectId(key);
      if (deleted.contains(schema_id)) {
        auto schema_drop = CreateSchemaDrop(database_id, schema_id);
      } else {
        return AddSchema(database_id, schema_id, slice);
      }
    });
}

Result CatalogFeature::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitObjects(
    schema_id, RocksDBEntryType::Function,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      ViewOptions options;
      auto r = ViewOptions::Read(options, slice);

      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "view", r.errorMessage(), slice);
      }

      std::shared_ptr<catalog::View> view;
      r = CreateViewInstance(view, db_id, std::move(options),
                             ViewContext::Internal);

      return Global().RegisterView(db_id, schema_id, std::move(view));
    });
}

Result CatalogFeature::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  return GetServerEngine().VisitObjects(
    schema_id, RocksDBEntryType::View,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      ViewOptions options;
      auto r = ViewOptions::Read(options, slice);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "view", r.errorMessage(), slice);
      }
      std::shared_ptr<View> view;

      r = CreateViewInstance(view, db_id, std::move(options),
                             ViewContext::Restore);
      if (!r.ok()) {
        return r;
      }

      return Global().RegisterView(schema_id, view->GetId(), view);
    });
}

Result CatalogFeature::RegisterIndexes(ObjectId db_id, ObjectId schema_id) {
  auto deleted = CollectObjects(schema_id, RocksDBEntryType::IndexTombstone);
  return GetServerEngine().VisitObjects(
    schema_id, RocksDBEntryType::Index,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      auto index_id = RocksDBKey::GetObjectId(key);
      if (deleted.contains(index_id)) {
        auto index_drop = CreateIndexDrop(schema_id, index_id);
      } else {
        return AddIndex(db_id, schema_id, index_id, slice);
      }
    });
}

Result CatalogFeature::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  auto deleted = CollectObjects(db_id, RocksDBEntryType::TableTombstone);
  return GetServerEngine().VisitObjects(
    schema_id, RocksDBEntryType::Collection,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      auto table_id = RocksDBKey::GetObjectId(key);
      if (deleted.contains(table_id)) {
        auto table_drop = CreateTableDrop(schema_id, table_id);
      } else {
        return AddTable(db_id, schema_id, table_id, slice);
      }
    });
}

Result CatalogFeature::AddRoles() {
  auto& engine = GetServerEngine();
  auto r = engine.VisitObjects(
    id::kSystemDB, RocksDBEntryType::Role,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      SDB_ASSERT(!slice.get(StaticStrings::kDataSourceId).isNone());

      std::shared_ptr<catalog::Role> role;
      auto r = catalog::Role::Instantiate(role, slice, false);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "role", r.errorMessage(), slice);
      }

      return Global().RegisterRole(std::move(role));
    });

  if (!r.ok()) {
    return {r.errorNumber(), "Failed to read roles, error: ", r.errorMessage()};
  }

  return {};
}

Result CatalogFeature::AddTable(ObjectId db_id, ObjectId schema_id,
                                ObjectId table_id, vpack::Slice slice) {
  CreateTableOptions options;

  // TODO(gnusi): .skip_unknown = false, .strict = true
  if (auto r = vpack::ReadObjectNothrow<TableOptions>(
        slice, options, {.skip_unknown = true, .strict = false},
        ObjectInternal{db_id});
      !r.ok()) {
    return ErrorMeta(r.errorNumber(), "table", r.errorMessage(), slice);
  }

  return Local().RegisterTable(db_id, schema_id, std::move(options));
}

Result CatalogFeature::AddIndex(ObjectId db_id, ObjectId schema_id,
                                ObjectId index_id, vpack::Slice slice) {
  IndexBaseOptions options;
  if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
    return r;
  }
  return Local().RegisterIndex(db_id, schema_id, std::move(options));
}

Result CatalogFeature::AddSchema(ObjectId db_id, ObjectId schema_id,
                                 vpack::Slice slice) {
  SchemaOptions options;
  if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
    return ErrorMeta(r.errorNumber(), "schema", r.errorMessage(), slice);
  }

  auto schema = std::make_shared<catalog::Schema>(db_id, std::move(options));

  if (auto r = Global().RegisterSchema(db_id, std::move(schema)); !r.ok()) {
    return r;
  }
  if (auto r = RegisterTables(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterIndexes(db_id, schema_id); !r.ok()) {
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

Result CatalogFeature::OpenDatabase(catalog::DatabaseOptions database) {
  SDB_TRACE(
    "xxxxx", Logger::ENGINES,
    "opening views and collections metadata in database: ", database.name);

  return basics::SafeCall(
    [&] -> Result {
      if (auto r = AddDatabase(database); !r.ok()) {
        return r;
      }

      auto make_error = [&](auto& r, std::string_view context) {
        return Result{r.errorNumber(),  "Failed to read ", context,
                      " in database: ", database.name,     " error: ",
                      r.errorMessage()};
      };

      std::vector<std::shared_ptr<Schema>> schemas;
      if (auto r = AddSchemas(database.id, schemas); !r.ok()) {
        return make_error(r, "schemas");
      }

      for (auto& schema : schemas) {
        if (ServerState::instance()->IsSingle()) {
          if (auto r = AddFunctions(database.id, *schema); !r.ok()) {
            return make_error(r, "functions");
          }
        }
        if (auto r = AddTables(database.id, *schema); !r.ok()) {
          return make_error(r, "tables");
        }
        if (auto r = AddIndexes(database.id, *schema); !r.ok()) {
          return make_error(r, "indexes");
        }
        if (ServerState::instance()->IsSingle()) {
          if (auto r = AddViews(database.id, *schema); !r.ok()) {
            return make_error(r, "views");
          }
        }
      }
      return {};
    },
    [&](ErrorCode code, std::string_view message) {
      return Result{code, "error while opening database '", database.name,
                    "': ", message};
    });
}

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id) {
  return GetDatabaseImpl(database_id);
}

ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name) {
  return GetDatabaseImpl(name);
}

std::shared_ptr<TableShard> GetTableShard(ObjectId id) {
  auto& catalog = GetCatalog();
  return catalog.GetSnapshot()->GetTableShard(id);
}

LogicalCatalog& GetCatalog() {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  return ServerState::instance()->IsCoordinator() ? catalogs.Global()
                                                  : catalogs.Local();
}

}  // namespace sdb::catalog
