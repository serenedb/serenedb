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
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/local_catalog.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
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

}  // namespace

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

Result CatalogFeature::AddSchemas(
  ObjectId database_id, std::vector<std::shared_ptr<Schema>>& schemas) {
  return GetServerEngine().VisitObjects(
    database_id, RocksDBEntryType::Schema,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      SchemaOptions options;
      if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
        return ErrorMeta(r.errorNumber(), "schema", r.errorMessage(), slice);
      }

      auto schema =
        std::make_shared<catalog::Schema>(database_id, std::move(options));
      schemas.emplace_back(schema);

      return Global().RegisterSchema(database_id, std::move(schema));
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

Result CatalogFeature::ProcessTombstones() {
  auto& engine = GetServerEngine();
  auto process_schema = [&](ObjectId database_id,
                            ObjectId schema_id) -> Result {
    return engine.VisitSchemaObjects(
      database_id, schema_id, RocksDBEntryType::Collection,
      [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
        CreateTableOptions options;

        // TODO(gnusi): .skip_unknown = false, .strict = true
        if (auto r = vpack::ReadObjectNothrow<TableOptions>(
              slice, options, {.skip_unknown = true, .strict = false},
              ObjectInternal{database_id});
            !r.ok()) {
          return ErrorMeta(r.errorNumber(), "collection", r.errorMessage(),
                           slice);
        }

        TableTombstone tombstone;
        tombstone.table = options.id;

        // TODO(gnusi): this will be gone once we have normal indexes
        struct IndexMeta {
          std::string_view objectId;  // NOLINT
          sdb::IndexType type = sdb::IndexType::kTypeUnknown;
          bool unique = false;
        };
        for (auto index_slice : vpack::ArrayIterator{options.indexes}) {
          IndexMeta index;
          auto r = vpack::ReadObjectNothrow(
            index_slice, index, {.skip_unknown = true, .strict = false});
          if (!r.ok()) {
            return ErrorMeta(r.errorNumber(), "index", r.errorMessage(),
                             index_slice);
          }
          ObjectId index_id{basics::string_utils::Uint64(index.objectId)};
          if (!index_id.isSet()) {
            continue;
          }
          tombstone.indexes.emplace_back(index_id, index.type,
                                         std::numeric_limits<uint64_t>::max(),
                                         index.unique);
        }

        Local().RegisterTableDrop(std::move(tombstone));
        return {};
      });
  };

  auto r = engine.VisitObjects(
    id::kTombstoneDatabase, RocksDBEntryType::TableTombstone,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      TableTombstone tombstone;

      if (auto r = vpack::ReadTupleNothrow<TableTombstone>(slice, tombstone);
          !r.ok()) {
        return ErrorMeta(r.errorNumber(), "collection tombstone",
                         r.errorMessage(), slice);
      }

      Local().RegisterTableDrop(std::move(tombstone));
      return {};
    });

  if (!r.ok()) {
    return {r.errorNumber(), "Failed to process collection tombstones, error: ",
            r.errorMessage()};
  }

  r = engine.VisitObjects(
    id::kTombstoneDatabase, RocksDBEntryType::ScopeTombstone,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      ObjectId database_id{RocksDBKey::SchemaId(key)};

      if (!database_id.isSet()) {
        return ErrorMeta(ERROR_BAD_PARAMETER, "scope tombstone",
                         "invalid database id", slice);
      }

      ObjectId schema_id{RocksDBKey::objectId(key)};

      auto r = [&] {
        if (schema_id.isSet()) {
          return process_schema(database_id, schema_id);
        } else {
          return engine.VisitObjects(
            database_id, RocksDBEntryType::Schema,
            [&](rocksdb::Slice key, vpack::Slice slice) {
              ObjectId schema_id{RocksDBKey::objectId(key)};
              if (!schema_id.isSet()) {
                return ErrorMeta(ERROR_BAD_PARAMETER, "schema",
                                 "invalid schema id", slice);
              }

              return process_schema(database_id, schema_id);
            });
        }
      }();

      if (!r.ok()) {
        return r;
      }

      Local().RegisterScopeDrop(database_id, schema_id);
      return {};
    });

  return {};
}

Result CatalogFeature::AddTables(ObjectId database_id, const Schema& schema) {
  return GetServerEngine().VisitSchemaObjects(
    database_id, schema.GetId(), RocksDBEntryType::Collection,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      CreateTableOptions options;

      // TODO(gnusi): .skip_unknown = false, .strict = true
      if (auto r = vpack::ReadObjectNothrow<TableOptions>(
            slice, options, {.skip_unknown = true, .strict = false},
            ObjectInternal{database_id});
          !r.ok()) {
        return ErrorMeta(r.errorNumber(), "table", r.errorMessage(), slice);
      }

      return Local().RegisterTable(database_id, schema.GetName(),
                                   std::move(options));
    });
}

Result CatalogFeature::AddIndexes(ObjectId database_id, const Schema& schema) {
  return GetServerEngine().VisitSchemaObjects(
    database_id, schema.GetId(), RocksDBEntryType::Index,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      IndexOptions<vpack::Slice> options;

      if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
        return ErrorMeta(r.errorNumber(), "index", r.errorMessage(), slice);
      }

      return Local().RegisterIndex(
        database_id, schema.GetName(), [&](const SchemaObject*) {
          return CreateIndex(database_id, std::move(options));
        });
    });
}

Result CatalogFeature::AddFunctions(ObjectId database_id,
                                    const Schema& schema) {
  return GetServerEngine().VisitSchemaObjects(
    database_id, schema.GetId(), RocksDBEntryType::Function,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      std::shared_ptr<catalog::Function> function;
      auto r =
        catalog::Function::Instantiate(function, database_id, slice, false);
      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "function", r.errorMessage(), slice);
      }

      return Global().RegisterFunction(database_id, schema.GetName(),
                                       std::move(function));
    });
}

Result CatalogFeature::AddViews(ObjectId database_id, const Schema& schema) {
  return GetServerEngine().VisitSchemaObjects(
    database_id, schema.GetId(), RocksDBEntryType::View,
    [&](rocksdb::Slice key, vpack::Slice slice) -> Result {
      ViewOptions options;
      auto r = ViewOptions::Read(options, slice);

      if (!r.ok()) {
        return ErrorMeta(r.errorNumber(), "view", r.errorMessage(), slice);
      }

      std::shared_ptr<catalog::View> view;
      r = CreateViewInstance(view, database_id, std::move(options),
                             ViewContext::Internal);

      return Global().RegisterView(database_id, schema.GetName(),
                                   std::move(view));
    });
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
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = ServerState::instance()->IsCoordinator() ? catalogs.Global()
                                                           : catalogs.Local();
  return catalog.GetSnapshot()->GetTableShard(id);
}

}  // namespace sdb::catalog
