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

#include <velox/core/QueryConfig.h>

#include "app/app_server.h"
#include "basics/logger/logger.h"
#include "general_server/state.h"
#include "pg/executor.h"
#include "pg/pg_feature.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"
#include "pg/sql_statement.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

void* SqlTree::GetRoot() const { return list_nth(list, root_idx - 1); }

void* SqlTree::GetNextRoot() {
  if (static_cast<int>(root_idx) >= list->length) {
    return nullptr;
  }
  return list_nth(list, root_idx++);
}

void SqlStatement::Reset() noexcept {
  memory_context.reset();
  objects.getObjects().clear();
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

  objects.getObjects().clear();

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

  auto& cpu_executor = GetScheduler()->GetCPUExecutor();

  query::QueryContext query_ctx{
    velox::core::QueryCtx::create(
      &cpu_executor,
      velox::core::QueryConfig{velox::core::QueryConfig::ConfigTag{},
                               connection_ctx}),
    objects};

  auto query_desc = pg::AnalyzeVelox(*raw_stmt, *query_string, objects,
                                     id_generator, query_ctx, params);

  if (query_desc.type == pg::SqlCommandType::Show) {
    SDB_ASSERT(query_desc.pgsql_node);
    const auto* show_stmt = castNode(VariableShowStmt, query_desc.pgsql_node);
    query_ctx.command_type.Add(query::CommandType::Show);
    if (!strcmp(show_stmt->name, "all")) {
      query = query::Query::CreateShowAll(query_ctx);
    } else {
      query = query::Query::CreateShow(show_stmt->name, query_ctx);
    }
    return true;
  }

  if (query_desc.pgsql_node) {
    SDB_ASSERT(query_desc.pgsql_node);
    auto executor =
      std::make_unique<Executor>(connection_ctx, *query_desc.pgsql_node);
    query_ctx.command_type.Add(query::CommandType::External);
    query = query::Query::CreateExternal(std::move(executor), query_ctx);
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
  while (!query) {
    if (!ProcessNextRoot(connection_ctx)) {
      return false;
    }
  }
  return true;
}

size_t SqlStatement::RootCount() const { return list_length(tree.list); }

}  // namespace sdb::pg
