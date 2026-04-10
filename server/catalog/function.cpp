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

#include <vpack/vpack_helper.h>

#include "basics/static_strings.h"
#include "catalog/identifiers/identifier.h"

namespace sdb::catalog {

PgSqlFunction::PgSqlFunction(ObjectId database_id, ObjectId id,
                             std::string_view name, std::string sql)
  : SchemaObject{{}, database_id,       {},
                 id, std::string{name}, ObjectType::PgSqlFunction},
    _sql{std::move(sql)} {}

std::shared_ptr<PgSqlFunction> PgSqlFunction::ReadInternal(vpack::Slice slice,
                                                           ReadContext ctx) {
  auto id = ObjectId{basics::VPackHelper::extractIdValue(slice)};
  auto name =
    basics::VPackHelper::getString(slice, StaticStrings::kDataSourceName, {});
  auto sql = basics::VPackHelper::getString(slice, "sql", {});
  return std::make_shared<PgSqlFunction>(ctx.database_id, id, name,
                                         std::string{sql});
}

void PgSqlFunction::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add("_key", Identifier{GetId().id()});
  builder.add(StaticStrings::kDataSourceName, GetName());
  builder.add("sql", _sql);
  builder.close();
}

std::shared_ptr<Object> PgSqlFunction::Clone() const {
  return std::make_shared<PgSqlFunction>(GetDatabaseId(), GetId(), GetName(),
                                         _sql);
}

}  // namespace sdb::catalog
