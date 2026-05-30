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

#include <vpack/vpack_helper.h>

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include "basics/static_strings.h"
#include "database/ticks.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {

PgSqlType::PgSqlType(ObjectId schema_id, ObjectId id, std::string_view name,
                     duckdb::unique_ptr<duckdb::CreateTypeInfo> info)
  : Object{schema_id, id == id::kInvalid ? ObjectId{NewTickServer(2) + 1} : id,
           name, ObjectType::PgSqlType},
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

std::shared_ptr<PgSqlType> PgSqlType::ReadInternal(vpack::Slice slice,
                                                   ReadContext ctx) {
  auto name =
    basics::VPackHelper::getString(slice, StaticStrings::kDataSourceName, {});

  auto info_slice = slice.get("info");
  SDB_ASSERT(info_slice.isString());
  auto str = info_slice.stringViewUnchecked();
  duckdb::MemoryStream stream(
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(str.data())),
    str.size());
  duckdb::BinaryDeserializer deserializer(stream);
  auto create_info = duckdb::CreateInfo::Deserialize(deserializer);
  auto type_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      std::move(create_info));
  return std::make_shared<PgSqlType>(ctx.schema_id, ctx.id, name,
                                     std::move(type_info));
}

void PgSqlType::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add(StaticStrings::kDataSourceName, GetName());

  // Serialize CreateTypeInfo via DuckDB BinarySerializer
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(*_info, stream,
                                      duckdb::VersionStorageOptions());
  auto data = stream.GetData();
  auto size = stream.GetPosition();
  builder.add("info",
              std::string_view{reinterpret_cast<const char*>(data), size});

  builder.close();
}

std::shared_ptr<Object> PgSqlType::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      _info->Copy());
  return std::make_shared<PgSqlType>(GetParentId(), GetId(), GetName(),
                                     std::move(cloned_info));
}

}  // namespace sdb::catalog
