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

#include "catalog/entry/foreign_data_wrapper.h"

namespace sdb::catalog {

ForeignDataWrapperCatalogEntry::ForeignDataWrapperCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateForeignDataWrapperInfo& info)
  : duckdb::InCatalogEntry{Type, catalog, info.GetQualifiedName().Name(),
                           info.oid} {
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> ForeignDataWrapperCatalogEntry::GetInfo()
  const {
  auto info = duckdb::make_uniq<duckdb::CreateForeignDataWrapperInfo>();
  info->SetName(name);
  info->oid = oid;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> ForeignDataWrapperCatalogEntry::Copy(
  duckdb::ClientContext&) const {
  duckdb::CreateForeignDataWrapperInfo info;
  info.SetName(name);
  info.oid = oid;
  return duckdb::make_uniq<ForeignDataWrapperCatalogEntry>(catalog, info);
}

}  // namespace sdb::catalog
