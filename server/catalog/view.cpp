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

#include <vpack/vpack_helper.h>

#include "basics/static_strings.h"
#include "catalog/identifiers/identifier.h"

namespace sdb::catalog {

PgSqlView::PgSqlView(ObjectId database_id, ObjectId id, std::string_view name,
                     std::string query)
  : SchemaObject{{},
                 database_id,
                 {},
                 id,
                 std::string{name},
                 ObjectType::PgSqlView},
    _query{std::move(query)} {}

std::shared_ptr<PgSqlView> PgSqlView::ReadInternal(vpack::Slice slice,
                                                    ReadContext ctx) {
  auto id = ObjectId{basics::VPackHelper::extractIdValue(slice)};
  auto name = basics::VPackHelper::getString(
    slice, StaticStrings::kDataSourceName, {});
  auto query = basics::VPackHelper::getString(slice, "query", {});
  return std::make_shared<PgSqlView>(ctx.database_id, id, name,
                                     std::string{query});
}

void PgSqlView::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add("_key", Identifier{GetId().id()});
  builder.add(StaticStrings::kDataSourceName, GetName());
  builder.add("query", _query);
  builder.close();
}

std::shared_ptr<Object> PgSqlView::Clone() const {
  return std::make_shared<PgSqlView>(GetDatabaseId(), GetId(), GetName(),
                                     _query);
}

}  // namespace sdb::catalog
