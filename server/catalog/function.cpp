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

#include "catalog/function.h"

#include <velox/type/Type.h>
#include <vpack/serializer.h>
#include <vpack/vpack_helper.h>

#include <string_view>

#include "basics/exceptions.h"
#include "basics/fwd.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/object.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "query/types.h"
#include "utils/query_string.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::catalog {

bool FunctionParameter::IsCollection() const { return aql::IsCollection(type); }
void FunctionParameter::MarkAsCollection() { type = aql::COLLECTION(); }

bool FunctionSignature::Matches(
  const std::vector<velox::TypePtr>& arg_types) const {
  return std::ranges::equal(
    arg_types, parameters,
    [](const velox::TypePtr& arg, const FunctionParameter& param) {
      SDB_ASSERT(param.type);
      SDB_ASSERT(arg);
      return *arg == *param.type ||
             (arg == pg::PGUNKNOWN() && param.type == velox::VARCHAR());
    });
}

bool FunctionSignature::ReturnsTable() const {
  return return_type && return_type->kind() == velox::TypeKind::ROW;
}

bool FunctionSignature::ReturnsVoid() const { return pg::IsVoid(return_type); }

bool FunctionSignature::IsProcedure() const {
  return pg::IsProcedure(return_type);
}
void FunctionSignature::MarkAsProcedure() { return_type = pg::PROCEDURE(); }

Result FunctionProperties::Read(FunctionProperties& properties,
                                vpack::Slice slice) {
  if (!slice.isObject()) {
    return {ERROR_BAD_PARAMETER, "Function definition must be an object"};
  }

  properties.name = basics::VPackHelper::getString(
    slice, sdb::StaticStrings::kDataSourceName, {});

  if (properties.name.empty()) {
    return {ERROR_BAD_PARAMETER, "Function name must be a non-empty string"};
  }

  properties.id = Identifier{basics::VPackHelper::extractIdValue(slice)};
  properties.implementation = slice.get("implementation");

  if (auto r =
        vpack::ReadTupleNothrow(slice.get("signature"), properties.signature);
      !r.ok()) {
    return r;
  }

  if (auto r =
        vpack::ReadTupleNothrow(slice.get("options"), properties.options);
      !r.ok()) {
    return r;
  }

  return {};
}

std::shared_ptr<PgSqlFunction> PgSqlFunction::ReadInternal(vpack::Slice slice,
                                                           ReadContext ctx) {
  FunctionProperties properties;
  if (auto r = FunctionProperties::Read(properties, slice); !r.ok()) {
    return nullptr;
  }

  auto impl_slice = properties.implementation;
  auto query = basics::VPackHelper::getString(impl_slice, "query", {});
  if (query.empty()) {
    return nullptr;
  }

  return std::make_shared<PgSqlFunction>(
    ctx.database_id, properties.id, properties.name, std::string{query},
    std::move(properties.signature), std::move(properties.options));
}

PgSqlFunction::PgSqlFunction(ObjectId database_id, ObjectId id,
                             std::string_view name, std::string query,
                             FunctionSignature signature,
                             FunctionOptions options)
  : SchemaObject{{}, database_id, {}, id, name, ObjectType::PgSqlFunction},
    _signature{std::move(signature)},
    _options{std::move(options)},
    _query{std::move(query)} {
  SDB_ASSERT(!this->GetName().empty());

  if (!_query.empty()) {
    const QueryString query_string{_query};
    _memory_context = pg::CreateMemoryContext();
    auto* tree = pg::Parse(*_memory_context, query_string);
    SDB_ASSERT(list_length(tree) == 1);
    _stmt = list_nth_node(RawStmt, tree, 0);
    SDB_ASSERT(_stmt);

    auto database = catalog::GetDatabase(database_id);
    SDB_ASSERT(database);
    pg::Collect((*database)->GetName(), *_stmt, _objects);
  }
}

void PgSqlFunction::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [&](vpack::Builder& b) {
    b.add("id", GetId().id());
    b.add("signature");
    vpack::WriteTuple(b, _signature);
    b.add("options");
    vpack::WriteTuple(b, _options);
    b.add("implementation");
    b.openObject();
    b.add("query", _query);
    b.close();
  });
  b.close();
}

std::shared_ptr<Object> PgSqlFunction::Clone() const {
  vpack::Builder b;
  WriteInternal(b);
  return ReadInternal(b.slice(), {.database_id = GetDatabaseId()});
}

}  // namespace sdb::catalog
