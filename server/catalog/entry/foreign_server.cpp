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

#include "catalog/entry/foreign_server.h"

#include <utility>

namespace sdb::catalog {

ForeignServerCatalogEntry::ForeignServerCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateForeignServerInfo& info)
  : duckdb::InCatalogEntry{duckdb::CatalogType::FOREIGN_SERVER_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid},
    _server_type{info.server_type},
    _version{info.version},
    _fdw_name{info.fdw_name},
    _options{info.options} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> ForeignServerCatalogEntry::GetInfo()
  const {
  auto info = duckdb::make_uniq<duckdb::CreateForeignServerInfo>();
  info->SetName(name);
  info->server_type = _server_type;
  info->version = _version;
  info->fdw_name = _fdw_name;
  info->options = _options;
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> ForeignServerCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<ForeignServerCatalogEntry>(
    catalog, info->Cast<duckdb::CreateForeignServerInfo>());
}

}  // namespace sdb::catalog
