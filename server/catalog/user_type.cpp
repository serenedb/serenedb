////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "catalog/user_type.h"

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <ranges>

#include "basics/serializer.h"
#include "catalog/create_info_serde.h"
#include "database/ticks.h"

namespace sdb::catalog {

PgSqlType::PgSqlType(Permissions perm, ObjectId schema_id, ObjectId id,
                     std::string_view name,
                     duckdb::unique_ptr<duckdb::CreateTypeInfo> info)
  : Object{std::move(perm), schema_id,
           id == id::kInvalid ? ObjectId{NewTickServer(2) + 1} : id, name,
           ObjectType::PgSqlType},
    _info{std::move(info)} {
  auto type_info = _info->type.AuxInfo()
                     ? _info->type.AuxInfo()->DeepCopy()
                     : duckdb::make_shared_ptr<duckdb::ExtraTypeInfo>(
                         duckdb::ExtraTypeInfoType::GENERIC_TYPE_INFO);
  type_info->alias = GetName();
  auto ext = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
  ext->properties[kPgSqlTypeOidProp] = duckdb::Value::UBIGINT(GetId().id());
  type_info->extension_info = std::move(ext);
  _info->type = {_info->type.id(), std::move(type_info)};
}

std::shared_ptr<PgSqlType> PgSqlType::Deserialize(duckdb::Deserializer& src,
                                                  ReadContext ctx) {
  CreateInfoReadData<duckdb::CreateTypeInfo> data;
  basics::ReadTuple(src, data);
  return std::make_shared<PgSqlType>(std::move(data.perm), ctx.schema_id,
                                     ctx.id, data.name,
                                     std::move(data.info.info));
}

void PgSqlType::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, CreateInfoWriteData<duckdb::CreateTypeInfo>{
                             GetName(), {_info.get()}, GetPermissions()});
}

std::shared_ptr<Object> PgSqlType::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      _info->Copy());
  return std::make_shared<PgSqlType>(GetPermissions(), GetParentId(), GetId(),
                                     GetName(), std::move(cloned_info));
}

}  // namespace sdb::catalog
