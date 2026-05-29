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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include "basics/static_strings.h"

namespace sdb::catalog {

PgSqlView::PgSqlView(ObjectId schema_id, ObjectId id, std::string_view name,
                     duckdb::unique_ptr<duckdb::CreateViewInfo> info)
  : Object{schema_id, id, std::string{name}, ObjectType::PgSqlView},
    _info{std::move(info)} {}

std::shared_ptr<PgSqlView> PgSqlView::ReadInternal(vpack::Slice slice,
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
  auto view_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      std::move(create_info));
  return std::make_shared<PgSqlView>(ctx.schema_id, ctx.id, name,
                                     std::move(view_info));
}

void PgSqlView::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add(StaticStrings::kDataSourceName, GetName());

  // Serialize CreateViewInfo via DuckDB BinarySerializer
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(*_info, stream);
  auto data = stream.GetData();
  auto size = stream.GetPosition();
  builder.add("info",
              std::string_view{reinterpret_cast<const char*>(data), size});

  builder.close();
}

std::shared_ptr<Object> PgSqlView::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      _info->Copy());
  return std::make_shared<PgSqlView>(GetParentId(), GetId(), GetName(),
                                     std::move(cloned_info));
}

Refs PgSqlView::ExtractRefs(RefKinds kinds) const {
  if (!_info->query) {
    return {};
  }
  return ::sdb::ExtractRefs(*_info->query, kinds);
}

}  // namespace sdb::catalog
