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

#include "basics/serializer.h"
#include "catalog/create_info_serde.h"

namespace sdb::catalog {

PgSqlView::PgSqlView(Permissions perm, ObjectId schema_id, ObjectId id,
                     std::string_view name,
                     duckdb::unique_ptr<duckdb::CreateViewInfo> info)
  : Object{std::move(perm), schema_id, id, std::string{name},
           ObjectType::PgSqlView},
    _info{std::move(info)} {}

std::shared_ptr<PgSqlView> PgSqlView::Deserialize(duckdb::Deserializer& src,
                                                  ReadContext ctx) {
  CreateInfoReadData<duckdb::CreateViewInfo> data;
  basics::ReadTuple(src, data);
  return std::make_shared<PgSqlView>(std::move(data.perm), ctx.schema_id,
                                     ctx.id, data.name,
                                     std::move(data.info.info));
}

void PgSqlView::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, CreateInfoWriteData<duckdb::CreateViewInfo>{
                             GetName(), {_info.get()}, GetPermissions()});
}

std::shared_ptr<Object> PgSqlView::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      _info->Copy());
  return std::make_shared<PgSqlView>(GetPermissions(), GetParentId(), GetId(),
                                     GetName(), std::move(cloned_info));
}

Refs PgSqlView::GetRefs(RefKinds kinds) const {
  if (!_info->query) {
    return {};
  }
  return ExtractRefs(*_info->query, kinds);
}

}  // namespace sdb::catalog
