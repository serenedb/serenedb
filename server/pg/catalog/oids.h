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

#pragma once

#include <array>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <span>
#include <string_view>

namespace sdb::pg {

inline constexpr uint64_t kInvalidOid = 0;

constexpr uint32_t WireOid(uint64_t oid) { return static_cast<uint32_t>(oid); }

// Postgres' PUBLIC pseudo-role. It is not a role id at all: 0 is the oid no
// pg_authid row can carry, which is what lets an acl item name "everybody".
inline constexpr duckdb::idx_t kPublicGrantee = 0;

inline constexpr duckdb::idx_t kMinSystem = 16384;
inline constexpr duckdb::idx_t kMaxSystem = 65536;

#include "pg/catalog/generated/catalog_oids.gen.inc"

inline constexpr duckdb::idx_t kPgMainSchema = kMinSystem + 4;

inline constexpr duckdb::idx_t kRootUser = kMinSystem;

inline constexpr duckdb::idx_t kPgAmInverted = kMinSystem + 300;
inline constexpr duckdb::idx_t kPgAmIResearch = kMinSystem + 301;
inline constexpr duckdb::idx_t kPgAmSecondary = kMinSystem + 303;

inline constexpr duckdb::idx_t kPgOpclassIvf = kMinSystem + 200;
inline constexpr duckdb::idx_t kPgOpclassIncluded = kMinSystem + 201;
inline constexpr duckdb::idx_t kPgOpclassHnsw = kMinSystem + 202;

struct SystemObject {
  duckdb::idx_t oid;
  std::string_view name;
};

inline constexpr std::array kForeignDataWrappers{
  SystemObject{kMinSystem + 500, "clickhouse_fdw"},
  SystemObject{kMinSystem + 501, "iceberg_fdw"},
  SystemObject{kMinSystem + 502, "postgres_fdw"},
};

inline constexpr std::array kSystemNamespaces{
  SystemObject{kPgCatalogSchema, irs::StaticStrings::kPgCatalogSchema},
  SystemObject{kPgInformationSchema, irs::StaticStrings::kInformationSchema},
};

inline constexpr std::array kSystemTablespaces{
  SystemObject{kPgDefaultTablespace, "pg_default"},
  SystemObject{kPgGlobalTablespace, "pg_global"},
};

inline constexpr const SystemObject* FindSystemObject(
  std::span<const SystemObject> objects, duckdb::idx_t oid) {
  for (const auto& object : objects) {
    if (object.oid == oid) {
      return &object;
    }
  }
  return nullptr;
}

inline constexpr const SystemObject* FindSystemObject(
  std::span<const SystemObject> objects, std::string_view name) {
  for (const auto& object : objects) {
    if (object.name == name) {
      return &object;
    }
  }
  return nullptr;
}

inline constexpr duckdb::idx_t kFirstCollation = kMinSystem + 1'000;

inline constexpr duckdb::idx_t kFirstBuiltinFunction = kMinSystem + 10'000;

inline constexpr uint64_t kRowTypeOidBit = uint64_t{1} << 30;
inline constexpr uint64_t kRowArrayTypeOidBit = uint64_t{1} << 31;

inline constexpr uint64_t TypeArrayOid(uint64_t element_oid) {
  return element_oid & kRowTypeOidBit ? element_oid | kRowArrayTypeOidBit
                                      : element_oid - 1;
}

inline constexpr uint64_t TypeElementOid(uint64_t array_oid) {
  return array_oid & kRowTypeOidBit ? array_oid & ~kRowArrayTypeOidBit
                                    : array_oid + 1;
}

inline constexpr uint64_t RowTypeOid(uint64_t relation_oid) {
  return relation_oid | kRowTypeOidBit;
}

inline constexpr uint64_t RowTypeRelation(uint64_t oid) {
  return oid & ~(kRowTypeOidBit | kRowArrayTypeOidBit);
}

}  // namespace sdb::pg
