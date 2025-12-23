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

#include "column_expr.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <utility>

#include "basics/errors.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "pg/sql_collector.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "pg/sql_utils.h"
#include "utils/query_string.h"
#include "vpack/vpack_helper.h"

namespace sdb {
namespace {

Result ParseColumnExpr(ObjectId database_id, std::string_view query,
                       pg::Objects& objects,
                       pg::MemoryContextPtr& memory_context,
                       const Node*& expr) {
  return basics::SafeCall([&] -> Result {
    const QueryString query_string{query};
    memory_context = pg::CreateMemoryContext();
    expr = pg::ParseSingleExpression(*memory_context, query_string);
    SDB_ASSERT(expr);
    SDB_ASSERT(objects.getObjects().empty());

    auto database = catalog::GetDatabase(database_id);
    if (!database) {
      return std::move(database).error();
    }

    pg::CollectExpr((*database)->GetName(), *expr, objects);

    return {};
  });
}

}  // namespace

Result ColumnExpr::Init(ObjectId database, Node* expr) {
  SDB_ASSERT(expr);
  auto query = pg::DeparseExpr(expr);
  return Init(database, std::move(query));
}

Result ColumnExpr::Init(ObjectId database, std::string query) {
  auto r = ParseColumnExpr(database, query, _objects, _memory_context, _expr);
  if (!r.ok()) {
    return r;
  }
  SDB_ASSERT(!query.empty());
  _query = std::move(query);
  return r;
}

Result ColumnExpr::FromVPack(ObjectId database, vpack::Slice slice,
                             ColumnExpr& column_expr) {
  auto query_slice = slice.get("query");
  if (query_slice.isNone()) {
    return {};
  }
  auto query = basics::VPackHelper::getString(slice, "query", {});
  SDB_ASSERT(!query.empty());
  auto r = ParseColumnExpr(database, query, column_expr._objects,
                           column_expr._memory_context, column_expr._expr);
  if (!r.ok()) {
    return r;
  }
  column_expr._query = std::move(query);
  return {};
}

void ColumnExpr::ToVPack(vpack::Builder& builder) const {
  vpack::ObjectBuilder o{&builder};
  builder.add("query", _query);
}

}  // namespace sdb
