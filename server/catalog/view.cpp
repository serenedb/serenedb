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

#include "catalog/view.h"

#include <vpack/serializer.h>
#include <vpack/slice.h>
#include <vpack/vpack_helper.h>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/identifier.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "utils/query_string.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::catalog {

View::View(ObjectId database_id, ObjectId id, std::string_view name,
           std::string query, std::shared_ptr<const State> state)
  : SchemaObject{{}, database_id, {}, id, name, ObjectType::PgView},
    _query{std::move(query)},
    _state{std::move(state)} {}

void View::WriteInternal(vpack::Builder& b) const {
  WriteObject(b, [&](vpack::Builder& b) {
    b.add("id", GetId().id());
    b.add("query", _query);
  });
}

namespace {

std::shared_ptr<View::State> CreateState() {
  return std::make_shared<View::State>();
}

Result ParseQuery(View::State& state, ObjectId database_id,
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
    SDB_ASSERT(state.objects.empty());

    auto database = catalog::GetDatabase(database_id);
    if (!database) {
      return std::move(database).error();
    }

    pg::Collect((*database)->GetName(), *state.stmt, state.objects);

    return {};
  });
}

Result CheckView(ObjectId database, std::string_view name,
                 const View::State& state, const Config& config) {
  SDB_ASSERT(state.stmt);
  if (state.stmt->stmt->type != T_SelectStmt) {
    return {ERROR_BAD_PARAMETER,
            "sql query view should contains select statement"};
  }

  auto search_path = config.Get<VariableType::PgSearchPath>("search_path");

  return basics::SafeCall([&] {
    pg::Objects objects;
    pg::Disallowed disallowed;
    disallowed.relations.emplace(pg::Objects::ObjectName{{}, name});
    pg::ResolveQueryView(database, search_path, objects, disallowed,
                         state.objects, config);
  });
}

}  // namespace

std::shared_ptr<View> View::ReadInternal(vpack::Slice slice, ReadContext ctx) {
  auto name =
    basics::VPackHelper::getString(slice, StaticStrings::kDataSourceName, {});
  auto query_slice = slice.get("query");
  if (!query_slice.isString()) {
    return nullptr;
  }
  auto query = std::string{query_slice.stringView()};
  if (query.empty()) {
    return nullptr;
  }

  auto state = CreateState();
  if (auto r = ParseQuery(*state, ctx.database_id, query); !r.ok()) {
    return nullptr;
  }

  auto id = ObjectId{basics::VPackHelper::extractIdValue(slice)};
  return std::make_shared<View>(ctx.database_id, id, name, std::move(query),
                                std::move(state));
}

ResultOr<std::shared_ptr<View>> View::Create(ObjectId database_id,
                                             std::string_view name,
                                             std::string query,
                                             const Config* config) {
  if (query.empty()) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   "Query can't be empty"};
  }

  auto state = CreateState();
  if (auto r = ParseQuery(*state, database_id, query); !r.ok()) {
    return std::unexpected(std::move(r));
  }

  if (config) {
    if (auto r = CheckView(database_id, name, *state, *config); !r.ok()) {
      return std::unexpected(std::move(r));
    }
  }

  return std::make_shared<View>(database_id, ObjectId{}, name, std::move(query),
                                std::move(state));
}

}  // namespace sdb::catalog
