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

#include "sql_query_view.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <vpack/slice.h>

#include <algorithm>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

#include "auth/common.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/global_resource_monitor.h"
#include "basics/resource_usage.h"
#include "basics/result.h"
#include "basics/std.hpp"
#include "catalog/catalog.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "utils/exec_context.h"
#include "utils/query_string.h"
#include "vpack/serializer.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb {

std::shared_ptr<SqlQueryViewImpl::State> SqlQueryViewImpl::Create() {
  return std::make_shared<State>();
}

Result SqlQueryViewImpl::Parse(State& state, ObjectId database_id,
                               std::string_view query) {
  return basics::SafeCall([&] -> Result {
    const QueryString query_string{query};
    state.memory_context = pg::CreateMemoryContext();
    auto* tree = pg::Parse(*state.memory_context, query_string);
    if (list_length(tree) != 1) {
      return {ERROR_BAD_PARAMETER,
              "sql query view should contains single statement"};
    }
    state.stmt = list_nth_node(RawStmt, tree, 0);
    SDB_ASSERT(state.stmt);
    SDB_ASSERT(state.objects.getObjects().empty());

    // TODO(gnusi): currently collector checks cross-database references and
    // need a name of the current database for this purpose. It looks like it'd
    // be better to do it in resolver.
    auto database = catalog::GetDatabase(database_id);
    if (!database) {
      return std::move(database).error();
    }

    pg::Collect((*database)->GetName(), *state.stmt, state.objects);

    return {};
  });
}

Result SqlQueryViewImpl::Check(ObjectId database, std::string_view name,
                               const State& state) {
  SDB_ASSERT(state.stmt);
  if (state.stmt->stmt->type != T_SelectStmt) {
    return {ERROR_BAD_PARAMETER,
            "sql query view should contains select statement"};
  }

  auto search_path = Config().Get<VariableType::PgSearchPath>("search_path");

  return basics::SafeCall([&] {
    pg::Objects objects;
    pg::Disallowed disallowed{pg::Objects::ObjectName{{}, name}};
    pg::ResolveQueryView(database, search_path, objects, disallowed,
                         state.objects);
  });
}

}  // namespace sdb
