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

#include "catalog/store/store.h"

#include <absl/cleanup/cleanup.h>

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
#include "basics/log.h"
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
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::catalog {
namespace {

// In case of recovery the ColumnExpr shouldn't be parsed
struct DropTableOptions {
  uint64_t columns;
};

ResultOr<DropTableOptions> GetTableOptionsForDrop(std::string_view bytes,
                                                  ReadContext ctx) {
  auto table = catalog::DeserializeObject<Table>(bytes, ctx);
  if (!table) {
    return std::unexpected<Result>{
      std::in_place, ERROR_SERVER_ILLEGAL_STATE,
      "failed to deserialize table definition during drop recovery"};
  }
  return DropTableOptions{.columns = table->Columns().size()};
}

ResultOr<std::shared_ptr<IndexDrop>> CreateIndexDrop(
  CatalogStore& store, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, ObjectId index_id, ObjectType index_type,
  bool is_root = false) {
  auto shard_type = IndexShardType(index_type);
  ObjectId shard_id;
  auto r = store.VisitBoot(index_id, shard_type,
                                   [&](CatalogStore::Key key, std::string_view) {
                                     SDB_ASSERT(!shard_id.isSet());
                                     shard_id = key.id;
                                     return Result{};
                                   });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  return std::make_shared<IndexDrop>(index_id, index_type, db_id, schema_id,
                                     table_id, is_root);
}

ResultOr<std::shared_ptr<TableDrop>> CreateTableDrop(
  CatalogStore& store, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, uint64_t cols, bool is_root = false) {
  ObjectId shard_id;
  uint64_t table_size = std::numeric_limits<uint64_t>::max();

  auto r =
    store.VisitBoot(table_id, ObjectType::TableShard,
                            [&](CatalogStore::Key key, std::string_view bytes) {
                              SDB_ASSERT(!shard_id.isSet());
                              shard_id = key.id;
                              auto stats = TableShard::DeserializeStats(bytes);
                              table_size = stats.num_rows * cols;
                              return Result{};
                            });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  std::vector<std::shared_ptr<IndexDrop>> indexes;
  auto collect_indexes = [&](ObjectType type) {
    return store.VisitBoot(
      table_id, type, [&](CatalogStore::Key key, std::string_view) {
        auto index_drop = CreateIndexDrop(store, db_id, schema_id, table_id,
                                          key.id, type);
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
  r = store.VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto seq =
        catalog::DeserializeObject<Sequence>(bytes, {.id = key.id,
                                                     .database_id = db_id,
                                                     .schema_id = schema_id});
      if (seq && seq->GetOwnerTableId() == table_id) {
        owned_sequences.push_back(key.id);
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
  CatalogStore& store, ObjectId db_id, ObjectId schema_id,
  bool is_root = false) {
  std::vector<std::shared_ptr<TableDrop>> tables;
  auto r = store.VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto options = GetTableOptionsForDrop(bytes, {.id = key.id,
                                                    .database_id = db_id,
                                                    .schema_id = schema_id});
      if (!options) {
        return std::move(options.error());
      }
      auto table_drop = CreateTableDrop(store, db_id, schema_id,
                                        key.id, options->columns);
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
  CatalogStore& store, ObjectId db_id) {
  std::vector<std::shared_ptr<SchemaDrop>> schemas;
  auto r = store.VisitBoot(
    db_id, ObjectType::Schema, [&](CatalogStore::Key key, std::string_view) {
      auto schema_drop = CreateSchemaDrop(store, db_id, key.id);
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

  Result AddDatabase(ObjectId database_id, std::string_view bytes);
  Result AddSchema(ObjectId database_id, ObjectId schema_id,
                   std::string_view bytes);
  Result AddTable(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  std::shared_ptr<Table> table);
  Result AddIndex(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  ObjectId index_id, ObjectType entry_type,
                  std::string_view bytes);

  bool IsDeleted(ObjectId id, DeletedScope scope) {
    return _deleted[magic_enum::enum_integer(scope)].contains(id);
  }

  void ClearDeletedDefinitions(DeletedScope scope) noexcept {
    _deleted[magic_enum::enum_integer(scope)].clear();
  }

  void CollectDeletedDefinitions(ObjectId id, DeletedScope scope) {
    auto& store = GetCatalogStore();
    auto& deleted = _deleted[magic_enum::enum_integer(scope)];
    SDB_ASSERT(deleted.empty());
    auto r = store.VisitBoot(id, ObjectType::Tombstone,
                                     [&](CatalogStore::Key key, std::string_view) {
                                       deleted.insert(key.id);
                                       return Result{};
                                     });
    SDB_ASSERT(r.ok());
  }

  LogicalCatalog& _catalog;

  std::array<containers::FlatHashSet<ObjectId>,
             magic_enum::enum_count<DeletedScope>()>
    _deleted;
};

Result OpenDatabase::AddDatabase(ObjectId database_id, std::string_view bytes) {
  auto db =
    catalog::DeserializeObject<catalog::Database>(bytes, {.id = database_id});
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
  return GetCatalogStore().VisitBoot(
    id::kInstance, ObjectType::Database,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      if (!IsDeleted(key.id, DeletedScope::Root)) {
        return AddDatabase(key.id, bytes);
      }
      auto drop = CreateDatabaseDrop(GetCatalogStore(), key.id);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterSchemas(ObjectId database_id) {
  return GetCatalogStore().VisitBoot(
    database_id, ObjectType::Schema,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto schema_id = key.id;
      if (!IsDeleted(key.id, DeletedScope::Database)) {
        return AddSchema(database_id, schema_id, bytes);
      }

      auto drop = CreateSchemaDrop(GetCatalogStore(), database_id,
                                   key.id, true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlFunction,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto function = catalog::DeserializeObject<catalog::PgSqlFunction>(
        bytes, {
                 .id = key.id,
                 .database_id = db_id,
                 .schema_id = schema_id,
               });
      if (!function) {
        return Result{ERROR_INTERNAL, "Failed to read function definition"};
      }
      return _catalog.RegisterFunction(db_id, schema_id, std::move(function));
    });
}

Result OpenDatabase::RegisterTokenizers(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Tokenizer,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto tokenizer =
        catalog::DeserializeObject<Tokenizer>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!tokenizer) {
        return Result{ERROR_INTERNAL, "Failed to read tokenizer definition"};
      }
      return _catalog.RegisterTokenizer(db_id, schema_id, std::move(tokenizer));
    });
}

Result OpenDatabase::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlView,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto view_id = key.id;
      auto view = catalog::DeserializeObject<PgSqlView>(
        bytes, {.id = view_id, .database_id = db_id, .schema_id = schema_id});
      if (!view) {
        return Result{ERROR_INTERNAL, "Failed to read view definition"};
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
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto seq =
        catalog::DeserializeObject<Sequence>(bytes, {
                                                      .id = key.id,
                                                      .database_id = db_id,
                                                      .schema_id = schema_id,
                                                    });
      if (!seq) {
        return Result{ERROR_INTERNAL, "Failed to read sequence definition"};
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
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlType,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto type =
        catalog::DeserializeObject<PgSqlType>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!type) {
        return Result{ERROR_INTERNAL, "Failed to read type definition"};
      }
      return _catalog.RegisterType(db_id, schema_id, std::move(type));
    });
}

Result OpenDatabase::RegisterIndexes(ObjectId db_id, ObjectId schema_id,
                                     ObjectId table_id) {
  auto visit = [&](ObjectType type) {
    return GetCatalogStore().VisitBoot(
      table_id, type, [&](CatalogStore::Key key, std::string_view bytes) -> Result {
        auto index_id = key.id;
        if (!IsDeleted(index_id, DeletedScope::Relation)) {
          return AddIndex(db_id, schema_id, table_id, index_id, type, bytes);
        }

        auto drop = CreateIndexDrop(GetCatalogStore(), db_id, schema_id,
                                    table_id, key.id, type, true);
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
  return GetCatalogStore().VisitBoot(
    table_id, ObjectType::TableShard,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      ObjectId shard_id = key.id;
      SDB_ASSERT(!IsDeleted(shard_id, DeletedScope::Relation));
      auto stats = TableShard::DeserializeStats(bytes);
      auto shard = std::make_shared<TableShard>(shard_id, table_id, stats);
      return _catalog.RegisterTableShard(std::move(shard));
    });
}

Result OpenDatabase::RegisterIndexShard(const std::shared_ptr<Index>& index) {
  return GetCatalogStore().VisitBoot(
    index->GetId(), IndexShardType(index->GetType()),
    [&](CatalogStore::Key key, std::string_view /*bytes*/) -> Result {
      auto shard = index->CreateIndexShard(false, key.id);
      if (!shard) {
        return std::move(shard.error());
      }
      SDB_ASSERT(*shard);
      return _catalog.RegisterIndexShard(std::move(*shard));
    });
}

Result OpenDatabase::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto table_id = key.id;
      if (!IsDeleted(table_id, DeletedScope::Schema)) {
        auto table = catalog::DeserializeObject<Table>(
          bytes,
          {.id = table_id, .database_id = db_id, .schema_id = schema_id});
        if (!table) {
          return Result{ERROR_INTERNAL, "Failed to read table definition"};
        }
        return AddTable(db_id, schema_id, table_id, std::move(table));
      }
      auto options = GetTableOptionsForDrop(
        bytes, {.id = table_id, .database_id = db_id, .schema_id = schema_id});
      if (!options) {
        return std::move(options.error());
      }
      auto drop = CreateTableDrop(GetCatalogStore(), db_id, schema_id, table_id,
                                  options->columns, true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::AddRoles() {
  auto& store = GetCatalogStore();
  auto r = store.VisitBoot(
    id::kInstance, ObjectType::Role,
    [&](CatalogStore::Key, std::string_view bytes) -> Result {
      auto role = catalog::DeserializeObject<catalog::Role>(bytes, {});
      if (!role) {
        return Result{ERROR_INTERNAL, "Failed to read role definition"};
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
                              ObjectType entry_type, std::string_view bytes) {
  ReadContext ctx{.id = index_id,
                  .database_id = database_id,
                  .schema_id = schema_id,
                  .relation_id = table_id};
  std::shared_ptr<Index> index;
  if (entry_type == ObjectType::SecondaryIndex) {
    index = catalog::DeserializeObject<SecondaryIndex>(bytes, ctx);
  } else {
    index = catalog::DeserializeObject<InvertedIndex>(bytes, ctx);
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
  r = GetCatalogStore().VisitBoot(index_id, ObjectType::Tombstone,
                                         [&](CatalogStore::Key, std::string_view) {
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
                               std::string_view bytes) {
  auto schema =
    catalog::DeserializeObject<catalog::Schema>(bytes, {.database_id = db_id});
  if (!schema) {
    return Result{ERROR_INTERNAL, "Failed to read schema definition"};
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
  auto catalog = std::make_shared<LocalCatalog>();
  _global = catalog;
  _local = std::move(catalog);

  auto r = Open();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void CatalogFeature::stop() {
  _local.reset();
  _global.reset();
}

Result CatalogFeature::Open() {
  if (auto r = GetCatalogStore().LoadBootState(); !r.ok()) {
    return r;
  }
  irs::Finally release_boot = [] noexcept {
    GetCatalogStore().ReleaseBootState();
  };

  OpenDatabase open_db{Local()};
  if (auto r = open_db.AddRoles(); !r.ok()) {
    return r;
  }

  // Bootstrap the default `postgres` role on first start. AddRoles() has
  // already loaded any persisted roles; if none exist we mint the root user
  // and persist it via Local().CreateRole(), which persists it so it
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
