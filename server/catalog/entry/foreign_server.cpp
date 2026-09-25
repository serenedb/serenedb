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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "catalog/boot.h"

namespace sdb::catalog {
namespace {

std::string ConnectionString(const ServerOptions& options) {
  return absl::StrJoin(options, " ", [](std::string* out, const auto& option) {
    absl::StrAppend(
      out, option.first, "='",
      absl::StrReplaceAll(option.second, {{"\\", "\\\\"}, {"'", "\\'"}}), "'");
  });
}

}  // namespace

void ForeignServerCatalogEntry::Attach(duckdb::ClientContext& context) const {
  const std::string_view type = absl::StripSuffix(_fdw_name, "_fdw");
  duckdb::AttachInfo info;
  info.name = name;
  if (type == "clickhouse") {
    info.path = ConnectionString(_options);
  } else {
    for (const auto& [key, value] : _options) {
      info.options.emplace(key, duckdb::Value{value});
    }
    if (type == "iceberg") {
      auto warehouse = info.options.extract("warehouse");
      info.path = warehouse ? warehouse.mapped().ToString() : "";
    }
  }
  catalog::Attach(context, info, type, duckdb::AttachVisibility::SHOWN);
}

void ForeignServerCatalogEntry::OnDrop() {
  duckdb::DatabaseManager::Get(catalog.GetDatabase()).DetachInternal(name);
}

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
