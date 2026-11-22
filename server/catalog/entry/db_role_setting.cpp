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

#include "catalog/entry/db_role_setting.h"

#include <duckdb/parser/parsed_data/alter_table_info.hpp>

#include "catalog/entry/role.h"

namespace sdb::catalog {

DbRoleSettingCatalogEntry::DbRoleSettingCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateDbRoleSettingInfo& info)
  : duckdb::InCatalogEntry{Type, catalog,
                           duckdb::CreateDbRoleSettingInfo::NameOf(
                             info.database, info.role),
                           info.oid},
    _database{info.database},
    _role{info.role},
    _config{info.config} {}

duckdb::unique_ptr<duckdb::CreateInfo> DbRoleSettingCatalogEntry::GetInfo()
  const {
  auto info = duckdb::make_uniq<duckdb::CreateDbRoleSettingInfo>();
  info->database = _database;
  info->role = _role;
  info->config = _config;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> DbRoleSettingCatalogEntry::Copy(
  duckdb::ClientContext&) const {
  auto info = GetInfo();
  info->oid = oid;
  return duckdb::make_uniq<DbRoleSettingCatalogEntry>(
    catalog, info->Cast<duckdb::CreateDbRoleSettingInfo>());
}

duckdb::unique_ptr<duckdb::CatalogEntry> DbRoleSettingCatalogEntry::AlterEntry(
  duckdb::ClientContext&, duckdb::AlterInfo& info) {
  const auto& alter = info.Cast<duckdb::AlterDatabaseSettingsInfo>();
  auto next = GetInfo();
  next->oid = oid;
  auto& setting = next->Cast<duckdb::CreateDbRoleSettingInfo>();
  ApplyConfigChange(setting.config, alter.set_config, alter.reset_config,
                    alter.reset_all_config);
  return duckdb::make_uniq<DbRoleSettingCatalogEntry>(catalog, setting);
}

}  // namespace sdb::catalog
