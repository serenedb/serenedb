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
#include <yaclib/async/join.hpp>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/errors.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "rest_server/serened_single.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "utils/exec_context.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

yaclib::Future<> UpdateIndexes(ExecContext& context,
                               const PgListWrapper<VacuumRelation>& rels) {
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();
  auto current_database = context.GetDatabase();
  const auto db = context.GetDatabaseId();
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto snapshot = catalog.GetSnapshot();
  std::vector<yaclib::Future<>> index_futures;
  for (const auto& rel : rels) {
    std::string_view schema_name = current_schema;
    std::string_view rel_name;
    if (rel->relation->catalogname) {
      return yaclib::MakeFuture<>();
    }
    if (rel->relation->schemaname) {
      schema_name = {rel->relation->schemaname};
    }
    SDB_ASSERT(rel->relation->relname);
    rel_name = {rel->relation->relname};
    auto table = snapshot->GetTable(db, schema_name, rel_name);
    SDB_ASSERT(table);
    for (auto index_shard : snapshot->GetIndexShardsByTable(table->GetId())) {
      SDB_ASSERT(index_shard);
      switch (index_shard->GetType()) {
        case IndexType::Inverted: {
          auto& inverted_index =
            basics::downCast<search::InvertedIndexShard>(*index_shard);
          index_futures.push_back(inverted_index.CommitWait());
          break;
        }
        case IndexType::Secondary:
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_CASE_NOT_FOUND),
                          ERR_MSG("Secondary index is not supported"));
          break;
        case IndexType::Unknown:
          SDB_UNREACHABLE();
      }
    }
  }
  if (index_futures.empty()) {
    return yaclib::MakeFuture<>();
  }
  return yaclib::WhenAll(index_futures.begin(), index_futures.size());
}

Result SyncStats(ExecContext& context,
                 const PgListWrapper<VacuumRelation>& rels) {
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();
  auto current_database = context.GetDatabase();
  const auto db = context.GetDatabaseId();
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto snapshot = catalog.GetSnapshot();
  for (const auto& rel : rels) {
    std::string_view schema_name = current_schema;
    std::string_view rel_name;
    if (rel->relation->catalogname) {
      return {};
    }
    if (rel->relation->schemaname) {
      schema_name = {rel->relation->schemaname};
    }
    SDB_ASSERT(rel->relation->relname);
    rel_name = {rel->relation->relname};
    auto table = snapshot->GetTable(db, schema_name, rel_name);
    SDB_ASSERT(table);
    auto shard = snapshot->GetTableShard(table->GetId());
    if (auto r = GetServerEngine().SyncTableShard(*shard); !r.ok()) {
      return r;
    }
  }
  return {};
}

}  // namespace

// TODO: use ErrorPosition in ThrowSqlError
yaclib::Future<Result> Vacuum(ExecContext& context, const VacuumStmt& stmt) {
  if (!stmt.is_vacuumcmd) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ANALYZE is not implemented yet"));
  }
  PgListWrapper<DefElem> options(stmt.options);
  if (options.size() > 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("VACUUM does not support multiple options"));
  }
  containers::FlatHashSet<std::string_view> names;
  for (auto elem : options) {
    names.insert(elem->defname);
  }
  std::vector<yaclib::Future<Result>> res;
  if (names.contains("update_indexes")) {
    auto f = UpdateIndexes(context, PgListWrapper<VacuumRelation>{stmt.rels})
               .ThenInline([] { return Result{}; });
    res.push_back(std::move(f));
  }
  if (names.contains("sync_stats")) {
    auto f = yaclib::MakeFuture(
      SyncStats(context, PgListWrapper<VacuumRelation>{stmt.rels}));
    res.push_back(std::move(f));
  }

  if (names.contains("compact")) {
    if (list_length(stmt.options) > 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("VACUUM options are not implemented yet"));
    }
    if (list_length(stmt.rels) > 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("VACUUM for specific tables is not implemented yet"));
    }

    auto f = GetServerEngine().compactAll(true, true);
    res.push_back(std::move(f));
  }
  if (res.size() == 0) {
    return yaclib::MakeFuture<Result>();
  }
  return yaclib::Join(res.begin(), res.size()).ThenInline([] {
    return Result{};
  });
}

}  // namespace sdb::pg
