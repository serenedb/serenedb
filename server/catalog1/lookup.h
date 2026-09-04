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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/common/constants.hpp>

#include "pg/pg_types.h"

namespace sdb::catalog {

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchemaById(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, duckdb::idx_t id);

duckdb::optional_ptr<duckdb::CatalogEntry> FindEntryById(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  duckdb::CatalogType type, duckdb::idx_t id);

template<typename T>
duckdb::optional_ptr<T> FindIn(duckdb::ClientContext* context,
                               duckdb::Catalog& catalog, duckdb::idx_t id) {
  auto entry = FindEntryById(context, catalog, T::Type, id);
  return entry ? &entry->template Cast<T>() : nullptr;
}

}  // namespace sdb::catalog
