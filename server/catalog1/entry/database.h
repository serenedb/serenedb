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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <string>

namespace sdb::catalog {

// The durable definition of a database. duckdb keeps owning the live
// attachment (AttachedDatabase, in DatabaseManager's map); this entry is the
// definition it is derived from, and it is the one that is transactional.
class CreateDatabaseInfo final : public duckdb::CreateInfo {
 public:
  CreateDatabaseInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::DATABASE_ENTRY} {}

  duckdb::idx_t public_schema_id{0};

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::string ToString() const final;
};

class DatabaseCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::DATABASE_ENTRY;
  static constexpr const char* Name = "database";

  DatabaseCatalogEntry(duckdb::Catalog& catalog, CreateDatabaseInfo& info);

  duckdb::idx_t PublicSchemaId() const noexcept { return _public_schema_id; }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

 private:
  duckdb::idx_t _public_schema_id;
};

}  // namespace sdb::catalog
