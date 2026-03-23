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

#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "connector/key_utils.hpp"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"
#include "utils/exec_context.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

void TruncateTables(const std::vector<std::shared_ptr<catalog::Table>>& tables,
                    const std::shared_ptr<const catalog::Snapshot>& snapshot,
                    RocksDBEngineCatalog& engine) {
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);

  std::vector<std::unique_ptr<absl::WriterMutexLock>> table_locks;
  table_locks.reserve(tables.size());
  for (auto& table : tables) {
    auto table_shard = snapshot->GetTableShard(table->GetId());
    SDB_ASSERT(table_shard);
    auto lock =
      std::make_unique<absl::WriterMutexLock>(&table_shard->GetTableLock());
    table_locks.emplace_back(std::move(lock));
  }

  for (auto& table : tables) {
    auto table_id = table->GetId();

    auto [start, end] = connector::key_utils::CreateTableRange(table_id);
    auto r = rocksutils::RemoveLargeRange(engine.db(), rocksdb::Slice{start},
                                          rocksdb::Slice{end}, cf, true, true);
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }

    for (auto index_shard : snapshot->GetIndexShardsByTable(table_id)) {
      SDB_ASSERT(index_shard);
      index_shard->Clear();
    }
  }
}

}  // namespace

yaclib::Future<> TruncateTable(ExecContext& context, const TruncateStmt& stmt) {
  if (stmt.behavior == DROP_CASCADE) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("TRUNCATE CASCADE is not supported yet"));
  }

  auto& catalog = catalog::GetCatalog();
  auto snapshot = catalog.GetSnapshot();
  auto current_database = context.GetDatabaseId();
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();

  // Resolve and validate all tables first
  std::vector<std::shared_ptr<catalog::Table>> tables;
  for (const auto* rel : PgListWrapper<RangeVar>{stmt.relations}) {
    ObjectId db_id = current_database;
    std::string_view schema_name = current_schema;
    std::string_view rel_name;

    if (rel->catalogname) {
      db_id = snapshot->GetDatabase(rel->catalogname)->GetId();
    }
    if (rel->schemaname) {
      schema_name = rel->schemaname;
    }
    SDB_ASSERT(rel->relname);
    rel_name = rel->relname;

    auto relation = snapshot->GetRelation(db_id, schema_name, rel_name);
    if (!relation) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG("relation \"", rel_name, "\" does not exist"));
    }

    if (relation->GetType() != catalog::ObjectType::Table) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", rel_name, "\" is not a table"));
    }

    auto table = basics::downCast<catalog::Table>(std::move(relation));

    if (table->GetTableType() == TableType::File) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("cannot truncate file table \"", rel_name, "\""));
    }

    tables.push_back(std::move(table));
  }

  TruncateTables(tables, snapshot, GetServerEngine());

  return {};
}

}  // namespace sdb::pg
