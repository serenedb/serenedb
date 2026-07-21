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

#pragma once

#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <string>

#include "catalog/object.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

inline constexpr std::string kPgSqlTypeOidProp = "sdb_oid";

class PgSqlType final : public Object {
 public:
  PgSqlType(Permissions perm, ObjectId schema_id, ObjectId id,
            std::string_view name,
            duckdb::unique_ptr<duckdb::CreateTypeInfo> info);

  static std::shared_ptr<PgSqlType> Deserialize(duckdb::Deserializer& src,
                                                ReadContext ctx);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const duckdb::CreateTypeInfo& GetInfo() const noexcept { return *_info; }

  static constexpr ObjectId ToArrayOid(ObjectId scalar) noexcept {
    return ObjectId{scalar.id() - 1};
  }
  ObjectId GetArrayOid() const noexcept { return ToArrayOid(GetId()); }

  const duckdb::LogicalType& GetLogicalType() const noexcept {
    SDB_ASSERT(_info);
    return _info->type;
  }

 private:
  duckdb::unique_ptr<duckdb::CreateTypeInfo> _info;
};

}  // namespace sdb::catalog
