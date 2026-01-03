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

#include "catalog/sql_function_impl.h"

#include "catalog/catalog.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "utils/query_string.h"
#include "vpack/vpack_helper.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

Result Parse(ObjectId database_id, std::string_view query, pg::Objects& objects,
             pg::MemoryContextPtr& memory_context, const RawStmt*& stmt,
             bool is_procedure) {
  return basics::SafeCall([&] -> Result {
    const QueryString query_string{query};
    memory_context = pg::CreateMemoryContext();
    auto* tree = pg::Parse(*memory_context, query_string);
    if (list_length(tree) != 1) {
      // TODO: SQL function bodies (not the procedure ones) are atomic and can't
      // be partially executed. so not to forget this fact when we'll implement
      // multi-statement functions.
      return {ERROR_BAD_PARAMETER,
              "sql function should contain a single statement"};
    }
    stmt = list_nth_node(RawStmt, tree, 0);
    SDB_ASSERT(stmt);
    SDB_ASSERT(objects.empty());

    if (!is_procedure && stmt->stmt->type != T_SelectStmt) {
      return {ERROR_BAD_PARAMETER,
              "only SELECT statements are allowed in SQL function body"};
    }

    // TODO(gnusi): currently collector checks cross-database references and
    // need a name of the current database for this purpose. It looks like it'd
    // be better to do it in resolver.
    auto database = catalog::GetDatabase(database_id);
    if (!database) {
      return std::move(database).error();
    }

    pg::Collect((*database)->GetName(), *stmt, objects);

    return {};
  });
}

}  // namespace

Result FunctionImpl::Init(ObjectId database, std::string_view name,
                          std::string query, bool is_procedure) {
  auto r =
    Parse(database, query, _objects, _memory_context, _stmt, is_procedure);
  if (!r.ok()) {
    return r;
  }
  auto search_path = Config().Get<VariableType::PgSearchPath>("search_path");
  r = basics::SafeCall([&] {
    pg::Objects objects;
    pg::Disallowed disallowed{pg::Objects::ObjectName{{}, name}};
    pg::ResolveSqlFunction(database, search_path, objects, disallowed,
                           _objects);
  });
  if (!r.ok()) {
    return r;
  }
  _query = std::move(query);
  return r;
}

Result FunctionImpl::FromVPack(ObjectId database, vpack::Slice slice,
                               std::unique_ptr<FunctionImpl>& implementation,
                               bool is_procedure) {
  auto query = basics::VPackHelper::getString(slice, "query", {});
  if (query.empty()) {
    return {ERROR_BAD_PARAMETER,
            "function implementation query must be a non-empty string"};
  }
  auto impl = std::make_unique<FunctionImpl>();
  auto r = Parse(database, query, impl->_objects, impl->_memory_context,
                 impl->_stmt, is_procedure);
  if (!r.ok()) {
    return r;
  }
  implementation = std::move(impl);
  return {};
}

void FunctionImpl::ToVPack(vpack::Builder& builder) const {
  builder.openObject();
  builder.add("query", _query);
  builder.close();
}

}  // namespace sdb::pg
