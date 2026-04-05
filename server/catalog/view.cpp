////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
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
#include "utils/query_string.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::catalog {

PgSqlView::PgSqlView(ObjectId database_id, ObjectId id, std::string_view name,
                     std::string query)
  : SchemaObject{{}, database_id, {}, id, name, ObjectType::PgSqlView},
    _query{std::move(query)} {
  SDB_ASSERT(!_query.empty());

  const QueryString query_string{_query};
  _memory_context = pg::CreateMemoryContext();
  auto* tree = pg::Parse(*_memory_context, query_string);
  SDB_ASSERT(list_length(tree) == 1);
  _stmt = list_nth_node(RawStmt, tree, 0);
  SDB_ASSERT(_stmt);

  // Currently collector checks cross-database references and
  // needs the name of the current database for this purpose.
  // It looks like it'd be better to do it in resolver.
  auto database = catalog::GetDatabase(database_id);
  SDB_ASSERT(database);
  pg::Collect((*database)->GetName(), *_stmt, _objects);
}

void PgSqlView::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [&](vpack::Builder& b) {
    b.add("id", GetId().id());
    b.add("query", _query);
  });
  b.close();
}

std::shared_ptr<PgSqlView> PgSqlView::ReadInternal(vpack::Slice slice,
                                                   ReadContext ctx) {
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

  auto id = ObjectId{basics::VPackHelper::extractIdValue(slice)};
  return std::make_shared<PgSqlView>(ctx.database_id, id, name,
                                     std::move(query));
}

std::shared_ptr<Object> PgSqlView::Clone() const {
  return std::make_shared<PgSqlView>(GetDatabaseId(), GetId(), GetName(),
                                     std::string{_query});
}

}  // namespace sdb::catalog
