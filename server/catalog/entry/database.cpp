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

#include "catalog/entry/database.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/boot.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"

namespace sdb::catalog {

DatabaseCatalogEntry::DatabaseCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateDatabaseInfo& info,
  std::shared_ptr<DatabaseDirectory> directory)
  : duckdb::InCatalogEntry{duckdb::CatalogType::DATABASE_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid},
    _options{info.options},
    _settings{info.settings},
    _conn_limit{info.connection_limit},
    _allow_connections{info.allow_connections},
    _directory{directory
                 ? std::move(directory)
                 : std::make_shared<DatabaseDirectory>(
                     ClusterLayout(catalog.GetAttached()).DatabaseDir(oid))} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> DatabaseCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateDatabaseInfo>();
  info->SetName(name);
  info->comment = comment;
  info->tags = tags;
  info->options = _options;
  info->settings = _settings;
  info->connection_limit = _conn_limit;
  info->allow_connections = _allow_connections;
  return std::move(info);
}

const duckdb::vector<std::string>* DatabaseCatalogEntry::SettingsOf(
  duckdb::idx_t role) const {
  const auto row =
    absl::c_find_if(_settings, [&](const duckdb::DatabaseSetting& setting) {
      return setting.role == role;
    });
  return row == _settings.end() ? nullptr : &row->config;
}

duckdb::unique_ptr<duckdb::CatalogEntry> DatabaseCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_DATABASE_SETTINGS) {
    return duckdb::InCatalogEntry::AlterEntry(context, info);
  }
  auto& alter = info.Cast<duckdb::AlterDatabaseSettingsInfo>();
  if (alter.set_allow_connections && !alter.allow_connections) {
    if (const auto* connection = connector::GetSereneDBContextPtr(context);
        connection && connection->GetDatabaseId() == oid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot disallow connections for current database"));
    }
  }
  auto copy = GetInfo();
  auto& next = copy->Cast<duckdb::CreateDatabaseInfo>();
  next.oid = oid;
  next.permissions = permissions;
  if (alter.set_connection_limit) {
    next.connection_limit = alter.connection_limit;
  }
  if (alter.set_allow_connections) {
    next.allow_connections = alter.allow_connections;
  }
  auto row =
    absl::c_find_if(next.settings, [&](const duckdb::DatabaseSetting& setting) {
      return setting.role == alter.role_id;
    });
  if (row == next.settings.end()) {
    row = next.settings.insert(
      row, duckdb::DatabaseSetting{.role = alter.role_id, .config = {}});
  }
  ApplyConfigChange(row->config, alter.set_config, alter.reset_config,
                    alter.reset_all_config);
  if (row->config.empty()) {
    next.settings.erase(row);
  }
  return duckdb::make_uniq<DatabaseCatalogEntry>(catalog, next, _directory);
}

duckdb::unique_ptr<duckdb::CatalogEntry> DatabaseCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<DatabaseCatalogEntry>(
    catalog, info->Cast<duckdb::CreateDatabaseInfo>(), _directory);
}

void DatabaseCatalogEntry::OnDrop() {
  _directory->MarkDropped();
  pg::ProgressRegistry::Instance().ForgetDatabase(oid);
}

void DatabaseCatalogEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (CreatedObject(prev_entry)) {
    _directory->MarkDropped();
  }
}

}  // namespace sdb::catalog
