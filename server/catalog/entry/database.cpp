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

#include "catalog1/entry/database.h"

#include <duckdb/catalog/catalog.hpp>
#include <utility>

#include "catalog1/boot.h"

namespace sdb::catalog {

DatabaseCatalogEntry::DatabaseCatalogEntry(duckdb::Catalog& catalog,
                                           duckdb::CreateDatabaseInfo& info)
  : duckdb::InCatalogEntry{duckdb::CatalogType::DATABASE_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> DatabaseCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateDatabaseInfo>();
  info->SetName(name);
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> DatabaseCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<DatabaseCatalogEntry>(
    catalog, info->Cast<duckdb::CreateDatabaseInfo>());
}

std::string DatabaseCatalogEntry::ToSQL() const {
  return GetInfo()->ToString();
}

void DatabaseCatalogEntry::OnDrop() {
  RemoveDatabaseFiles(catalog.GetAttached(), oid);
}

}  // namespace sdb::catalog
