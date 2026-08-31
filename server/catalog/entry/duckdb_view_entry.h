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

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>

#include "catalog/entry.h"

namespace sdb::catalog {

using SereneDBViewEntry = duckdb::ViewCatalogEntry;

// One version of one view as PlaceEntry (and the static pg_catalog schema)
// builds it. The stored CREATE VIEW text keeps the name the view was defined
// under, so a renamed view would otherwise key its CatalogSet chain under the
// old one.
inline duckdb::unique_ptr<duckdb::CatalogEntry> MakeViewEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, const duckdb::CreateViewInfo& view,
  catalog::Permissions perm) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      view.Copy());
  info->SetSchema(schema.name);
  info->SetViewName(duckdb::Identifier{entry_name});
  info->temporary = false;
  info->internal = false;
  auto entry =
    duckdb::make_uniq_base<duckdb::CatalogEntry, duckdb::ViewCatalogEntry>(
      catalog, schema, *info);
  catalog::AdoptEntryIdentity(*entry, ObjectId{info->oid}, std::move(perm));
  return entry;
}

}  // namespace sdb::catalog
