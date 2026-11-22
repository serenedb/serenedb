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
#include <duckdb/parser/parsed_data/create_db_role_setting_info.hpp>
#include <string>

namespace sdb::catalog {

class DbRoleSettingCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::DB_ROLE_SETTING_ENTRY;

  DbRoleSettingCatalogEntry(duckdb::Catalog& catalog,
                            duckdb::CreateDbRoleSettingInfo& info);

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;

  duckdb::idx_t Database() const noexcept { return _database; }
  duckdb::idx_t Role() const noexcept { return _role; }
  const duckdb::vector<std::string>& Config() const noexcept { return _config; }

 private:
  duckdb::idx_t _database;
  duckdb::idx_t _role;
  duckdb::vector<std::string> _config;
};

}  // namespace sdb::catalog
