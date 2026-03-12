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

#include <absl/functional/overload.h>

#include <memory>
#include <string_view>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "connector/serenedb_connector.hpp"
#include "magic_enum/magic_enum.hpp"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/options_parser.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/query.h"
#include "rest_server/serened_single.h"
#include "search/inverted_index_shard.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

IndexType GetIndexType(char* method) {
  SDB_ASSERT(method);
  return magic_enum::enum_cast<IndexType>(method, magic_enum::case_insensitive)
    .value_or(IndexType::Unknown);
}

Result ParseIndexOptions(const IndexStmt& index,
                         std::vector<std::string>& column_names,
                         catalog::IndexBaseOptions& options) {
  if (!index.accessMethod) {
    return Result{ERROR_BAD_PARAMETER, "access method is not provided"};
  }
  auto index_type = GetIndexType(index.accessMethod);
  if (index_type == IndexType::Unknown) {
    return Result{ERROR_BAD_PARAMETER, "access method \"", index.accessMethod,
                  "\" does not exist"};
  }

  pg::PgListWrapper<IndexElem> index_columns{index.indexParams};
  options.column_ids.reserve(index_columns.size());

  for (auto* index_elem : index_columns) {
    column_names.push_back(index_elem->name);
  }

  options.name = index.idxname;
  options.type = index_type;
  return {};
}

constexpr OptionInfo kCommitInterval{"commit_interval", 1000,
                                     "Commit interval in milliseconds"};
constexpr OptionInfo kConsolidationInterval{
  "consolidation_interval", 1000, "Consolidation interval in milliseconds"};
constexpr OptionInfo kCleanupIntervalStep{"cleanup_interval_step", 1,
                                          "Cleanup interval step"};
constexpr OptionInfo kIndexOptions[] = {kCommitInterval, kConsolidationInterval,
                                        kCleanupIntervalStep};
constexpr OptionGroup kIndexGroup{"Index", kIndexOptions, {}};
constexpr OptionGroup kIndexOptionGroups[] = {kIndexGroup};

class CreateIndexOptionsParser : public OptionsParser {
 public:
  CreateIndexOptionsParser(const List* options)
    : OptionsParser{MakeOptions(options, {}),
                    kIndexOptionGroups,
                    {.operation = "CREATE INDEX"}} {
    ParseOptions([&] { Parse(); });
  }

  search::InvertedIndexShardOptions GetOptions() && {
    return std::move(_shard_options);
  }

 private:
  void Parse() {
    _shard_options.base.commit_interval_ms =
      EraseOptionOrDefault<kCommitInterval>();
    _shard_options.base.consolidation_interval_ms =
      EraseOptionOrDefault<kConsolidationInterval>();
    _shard_options.base.cleanup_interval_step =
      EraseOptionOrDefault<kCleanupIntervalStep>();
  }

  search::InvertedIndexShardOptions _shard_options;
};

}  // namespace

// TODO: use ErrorPosition in ThrowSqlError
yaclib::Future<> CreateIndex(ExecContext& context, query::Query& query,
                             const IndexStmt& stmt) {
  const auto db = context.GetDatabaseId();
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);

  const std::string_view relation_name = stmt.relation->relname;
  const std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    stmt.relation->schemaname ? std::string_view{stmt.relation->schemaname}
                              : current_schema;
  if (schema.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("no schema has been selected to create in"));
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  if (stmt.concurrent) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("CONCURRENTLY is not implemented"));
  }
  std::vector<std::string> column_names;
  catalog::IndexBaseOptions options;

  if (auto r = ParseIndexOptions(stmt, column_names, options); !r.ok()) {
    SDB_THROW(std::move(r));
  }

  if (options.type == IndexType::Inverted) {
    CreateIndexOptionsParser parser{stmt.options};
    auto shard_options = std::move(parser).GetOptions();
    auto r = catalog.CreateIndex(
      db, schema, relation_name, std::move(column_names), std::move(options),
      shard_options, {.create_with_tombstone = true});

    if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
      return {};
    } else if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("relation \"", stmt.idxname, "\" already exists"));
    }
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("index type is not supported"));
  }

  auto snapshot = catalog.GetSnapshot();
  auto catalog_table = snapshot->GetTable(db, schema, relation_name);
  SDB_ASSERT(catalog_table);
  auto catalog_index = snapshot->GetRelation(db, schema, stmt.idxname);
  SDB_ASSERT(catalog_index);
  auto& transaction = static_cast<query::Transaction&>(conn_ctx);

  auto& write_node = const_cast<axiom::logical_plan::TableWriteNode&>(
    basics::downCast<const axiom::logical_plan::TableWriteNode>(
      *query.GetLogicalPlan()));

  auto axiom_table =
    std::make_shared<connector::RocksDBTable>(*catalog_table, transaction);
  axiom_table->BackfillIndexId() = catalog_index->GetId();
  write_node.setTable(std::move(axiom_table));
  query.CompileQuery();
  query.MakeRunner();

  return {};
}

}  // namespace sdb::pg
