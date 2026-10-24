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

#include "catalog/entry/tablespace.h"

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

namespace sdb::catalog {

void RequireUnreservedTablespaceName(std::string_view name) {
  if (name.starts_with("pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("unacceptable tablespace name \"", name, "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system tablespaces."));
  }
}

TablespaceCatalogEntry::TablespaceCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateTablespaceInfo& info,
  std::shared_ptr<TablespaceRoot> root)
  : duckdb::InCatalogEntry{duckdb::CatalogType::TABLESPACE_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid},
    _location{info.location},
    _root{std::move(root)} {
  internal = info.internal;
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> TablespaceCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateTablespaceInfo>();
  info->SetName(name);
  info->location = _location;
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> TablespaceCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  auto& tablespace = info->Cast<duckdb::CreateTablespaceInfo>();
  tablespace.oid = oid;
  tablespace.internal = internal;
  tablespace.permissions = permissions;
  return duckdb::make_uniq<TablespaceCatalogEntry>(catalog, tablespace, _root);
}

void TablespaceCatalogEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  if (prev_entry.type == duckdb::CatalogType::INVALID) {
    SDB_ASSERT(_root);
    _root->Abandon();
  }
}

void TablespaceCatalogEntry::OnDrop() {
  SDB_ASSERT(_root);
  _root->MarkDropped();
}

}  // namespace sdb::catalog
