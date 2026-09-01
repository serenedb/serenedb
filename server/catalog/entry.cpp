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

#include "entry.h"

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database_manager.hpp>

#include "basics/duckdb_engine.h"
#include "catalog/database.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/role.h"
#include "catalog/tokenizer.h"

namespace sdb::catalog {
namespace {

duckdb::unique_ptr<duckdb::CreateInfo> DeserializeForeignCreateInfo(
  duckdb::Deserializer& deserializer, duckdb::CatalogType type) {
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
      return Role::Deserialize(deserializer);
    case DATABASE_ENTRY:
      return CreateDatabaseInfo::Deserialize(deserializer);
    case TOKENIZER_ENTRY:
      return CreateTokenizerInfo::Deserialize(deserializer);
    case FOREIGN_SERVER_ENTRY:
      return CreateForeignServerInfo::Deserialize(deserializer);
    case INDEX_ENTRY:
      return DeserializeIndexInfo(deserializer);
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("Unsupported type for deserialization of CreateInfo!"));
  }
}

}  // namespace

void RegisterForeignCreateInfoDeserializer() {
  duckdb::foreign_create_info_deserializer = &DeserializeForeignCreateInfo;
}

duckdb::DatabaseManager& IdAllocator() {
  return duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
}

ObjectId NextId() { return ObjectId{IdAllocator().NextOid()}; }

ObjectId NextNIds(uint64_t n) { return ObjectId{IdAllocator().NextOids(n)}; }

void RestoreId(uint64_t id) { IdAllocator().RestoreOid(id); }

void CollectTypeIds(const duckdb::LogicalType& type,
                    std::vector<ObjectId>& out) {
  if (auto ext = type.GetExtensionInfo()) {
    if (auto it = ext->properties.find(kPgSqlTypeOidProp);
        it != ext->properties.end()) {
      out.push_back(ObjectId{it->second.GetValue<uint64_t>()});
      return;
    }
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
      CollectTypeIds(duckdb::ListType::GetChildType(type), out);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      CollectTypeIds(duckdb::ArrayType::GetChildType(type), out);
      break;
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::VARIANT:
      for (const auto& child : duckdb::StructType::GetChildTypes(type)) {
        CollectTypeIds(child.second, out);
      }
      break;
    case duckdb::LogicalTypeId::MAP:
      CollectTypeIds(duckdb::MapType::KeyType(type), out);
      CollectTypeIds(duckdb::MapType::ValueType(type), out);
      break;
    case duckdb::LogicalTypeId::UNION:
      for (idx_t i = 0; i < duckdb::UnionType::GetMemberCount(type); ++i) {
        CollectTypeIds(duckdb::UnionType::GetMemberType(type, i), out);
      }
      break;
    default:
      break;
  }
}

}  // namespace sdb::catalog
