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

#include "pg/sql_statement.h"

#include <absl/strings/string_view.h>
#include <velox/core/QueryConfig.h>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/logger/logger.h"
#include "catalog/catalog.h"
#include "general_server/state.h"
#include "pg/command_executor.h"
#include "pg/pg_feature.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"
#include "pg/sql_statement.h"
#include "query/velox_executor.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

std::unique_ptr<query::Query> CreateCTASPipeline(
  const VeloxQuery& query_desc, query::QueryContext& query_ctx,
  const std::shared_ptr<ConnectionContext>& connection_ctx) {
  SDB_ASSERT(query_desc.pgsql_node);
  SDB_ASSERT(query_desc.root);
  SDB_ASSERT(query_desc.root->is(axiom::logical_plan::NodeKind::kTableWrite));

  const IntoClause* into = nullptr;
  bool if_not_exists = false;
  if (nodeTag(query_desc.pgsql_node) == T_CreateTableAsStmt) {
    const auto& ctas_stmt = *castNode(CreateTableAsStmt, query_desc.pgsql_node);
    into = ctas_stmt.into;
    if_not_exists = ctas_stmt.if_not_exists;
  } else {
    SDB_ASSERT(nodeTag(query_desc.pgsql_node) == T_SelectStmt);
    const auto& select_stmt = *castNode(SelectStmt, query_desc.pgsql_node);
    into = select_stmt.intoClause;
  }
  SDB_ASSERT(into);

  auto create_table = std::make_unique<CTASCreateTableExecutor>(
    connection_ctx, *into, if_not_exists);
  auto rollback = [connection_ctx, into] noexcept {
    auto db = connection_ctx->GetDatabaseId();
    const auto& rel = *into->rel;
    std::string current_schema = connection_ctx->GetCurrentSchema();
    const std::string_view schema =
      rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
    SDB_ASSERT(!schema.empty());
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    std::ignore = catalog.DropTable(db, schema, into->rel->relname);
  };
  auto velox_exec = std::make_unique<query::VeloxExecutor>();
  auto remove_tombstone = std::make_unique<RemoveTombstoneExecutor>(
    connection_ctx, absl::NullSafeStringView(into->rel->schemaname),
    into->rel->relname);

  std::vector<std::unique_ptr<query::Executor>> executors;
  executors.reserve(3);
  executors.emplace_back(std::move(create_table));
  executors.emplace_back(std::move(velox_exec));
  executors.emplace_back(std::move(remove_tombstone));

  query_ctx.command_type.Add(query::CommandType::Query);

  return query::Query::CreateWithExecutor(
    query_desc.root, query_ctx, std::move(executors), std::move(rollback));
}

std::unique_ptr<query::Query> CreateIndexPipeline(
  const VeloxQuery& query_desc, query::QueryContext& query_ctx,
  const std::shared_ptr<ConnectionContext>& connection_ctx) {
  SDB_ASSERT(query_desc.pgsql_node);
  SDB_ASSERT(query_desc.root);
  SDB_ASSERT(query_desc.root->is(axiom::logical_plan::NodeKind::kTableWrite));

  const auto& index_stmt = *castNode(IndexStmt, query_desc.pgsql_node);

  auto create_index =
    std::make_unique<CreateIndexExecutor>(connection_ctx, index_stmt);

  auto rollback = [connection_ctx, &index_stmt] noexcept {
    auto db = connection_ctx->GetDatabaseId();
    const auto& rel = *index_stmt.relation;
    std::string current_schema = connection_ctx->GetCurrentSchema();
    const std::string_view schema =
      rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
    SDB_ASSERT(!schema.empty());
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    std::ignore = catalog.DropIndex(db, schema, index_stmt.idxname);
  };
  auto velox_exec = std::make_unique<query::VeloxExecutor>();
  auto finish_creation = std::make_unique<FinishCreateIndexExecutor>(
    connection_ctx, absl::NullSafeStringView(index_stmt.relation->schemaname),
    index_stmt.idxname);
  auto remove_tombstone = std::make_unique<RemoveTombstoneExecutor>(
    connection_ctx, absl::NullSafeStringView(index_stmt.relation->schemaname),
    index_stmt.idxname);

  std::vector<std::unique_ptr<query::Executor>> executors;
  executors.reserve(4);
  executors.emplace_back(std::move(create_index));
  executors.emplace_back(std::move(velox_exec));
  executors.emplace_back(std::move(finish_creation));
  executors.emplace_back(std::move(remove_tombstone));

  query_ctx.command_type.Add(query::CommandType::Query);

  return query::Query::CreateWithExecutor(
    query_desc.root, query_ctx, std::move(executors), std::move(rollback));
}

}  // namespace

void* SqlTree::GetRoot() const { return list_nth(list, root_idx - 1); }

void* SqlTree::GetNextRoot() {
  if (static_cast<int>(root_idx) >= list->length) {
    return nullptr;
  }
  return list_nth(list, root_idx++);
}

void SqlStatement::Reset() noexcept {
  memory_context.reset();
  objects.clear();
  query_string.reset();
  params.Reset();
  tree = {.list = nullptr, .root_idx = 0};
}

bool SqlStatement::ProcessNextRoot(
  const std::shared_ptr<ConnectionContext>& connection_ctx) {
  auto* raw_stmt = castNode(RawStmt, tree.GetNextRoot());
  if (!raw_stmt) {
    return false;
  }

  objects.clear();

  // TODO : split to Parse and Bind steps
  ParamIndex max_bind_param_idx = 0;
  pg::Collect(connection_ctx->GetDatabase(), *raw_stmt, objects,
              max_bind_param_idx);
  params.types.resize(max_bind_param_idx);
  if (!params.types.empty()) {
    // cannot have multiple bind stmts, already checked in pg_commit_task
    SDB_ASSERT(RootCount() == 1);
  }

  pg::Resolve(connection_ctx->GetDatabaseId(), objects, *connection_ctx);
  SDB_ASSERT(memory_context);

  query::QueryContext query_ctx{connection_ctx, objects};

  auto query_desc = pg::AnalyzeVelox(
    *raw_stmt, *query_string, objects, id_generator, query_ctx, params,
    connection_ctx->GetSendBuffer(), connection_ctx->GetCopyQueue());

  if (query_desc.type == pg::SqlCommandType::Show) {
    SDB_ASSERT(query_desc.pgsql_node);
    const auto* show_stmt = castNode(VariableShowStmt, query_desc.pgsql_node);
    if (!strcmp(show_stmt->name, "all")) {
      query = query::Query::CreateShowAll(query_ctx);
    } else {
      query = query::Query::CreateShow(show_stmt->name, query_ctx);
    }
    return true;
  }

  if (query_desc.type == pg::SqlCommandType::CTAS) {
    query = CreateCTASPipeline(query_desc, query_ctx, connection_ctx);
    return true;
  }

  if (query_desc.type == pg::SqlCommandType::CreateIndex) {
    query = CreateIndexPipeline(query_desc, query_ctx, connection_ctx);
    return true;
  }

  if (query_desc.pgsql_node) {
    SDB_ASSERT(query_desc.pgsql_node);
    auto executor =
      std::make_unique<DDLExecutor>(connection_ctx, *query_desc.pgsql_node);
    query = query::Query::CreateDDL(std::move(executor), query_ctx);
    return true;
  }

  if (query_desc.type == pg::SqlCommandType::Explain) {
    query_ctx.command_type.Add(query::CommandType::Explain);
    if (query_desc.options.contains("analyze")) {
      query_ctx.explain_params.Add(query::ExplainWith::Execution);
      query_ctx.explain_params.Add(query::ExplainWith::Stats);
      query_ctx.command_type.Add(query::CommandType::Query);
    }

    if (query_desc.options.contains("all_plans")) {
      query_ctx.explain_params.Add(query::ExplainWith::Logical);
      query_ctx.explain_params.Add(query::ExplainWith::InitialQueryGraph);
      query_ctx.explain_params.Add(query::ExplainWith::FinalQueryGraph);
      query_ctx.explain_params.Add(query::ExplainWith::Physical);
      query_ctx.explain_params.Add(query::ExplainWith::Execution);
    }
    if (query_desc.options.contains("logical")) {
      query_ctx.explain_params.Add(query::ExplainWith::Logical);
    }
    if (query_desc.options.contains("initial_query_graph")) {
      query_ctx.explain_params.Add(query::ExplainWith::InitialQueryGraph);
    }
    if (query_desc.options.contains("final_query_graph")) {
      query_ctx.explain_params.Add(query::ExplainWith::FinalQueryGraph);
    }
    if (query_desc.options.contains("physical")) {
      query_ctx.explain_params.Add(query::ExplainWith::Physical);
    }
    if (query_desc.options.contains("execution")) {
      query_ctx.explain_params.Add(query::ExplainWith::Execution);
    }

    if (query_desc.options.contains("registers")) {
      query_ctx.explain_params.Add(query::ExplainWith::Registers);
    }
    if (query_desc.options.contains("oneline")) {
      query_ctx.explain_params.Add(query::ExplainWith::Oneline);
    }
    if (query_desc.options.contains("cost")) {
      query_ctx.explain_params.Add(query::ExplainWith::Cost);
    }
    if (query_desc.options.contains("stats")) {
      query_ctx.explain_params.Add(query::ExplainWith::Stats);
    }
  } else {
    query_ctx.command_type.Add(query::CommandType::Query);
  }
  query = query::Query::CreateQuery(query_desc.root, query_ctx);
  return true;
}

bool SqlStatement::NextRoot(
  const std::shared_ptr<ConnectionContext>& connection_ctx) {
  // After completing previous stmt
  // query could be non-nullptr
  query.reset();
  connection_ctx->OnNewStatement();
  while (!query) {
    if (!ProcessNextRoot(connection_ctx)) {
      return false;
    }
  }
  return true;
}

size_t SqlStatement::RootCount() const { return list_length(tree.list); }

}  // namespace sdb::pg
