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

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/helper.hpp>
#include <vector>

namespace sdb::catalog {

template<typename T>
std::vector<duckdb::reference<T>> EntriesOf(duckdb::DuckCatalog& catalog,
                                            const auto& accept) {
  std::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
  catalog.ScanSchemas(
    [&](duckdb::SchemaCatalogEntry& schema) { schemas.emplace_back(schema); });
  std::vector<duckdb::reference<T>> entries;
  for (auto& schema : schemas) {
    schema.get().Scan(T::Type, [&](duckdb::CatalogEntry& entry) {
      if (entry.type == T::Type && accept(entry)) {
        entries.emplace_back(entry.Cast<T>());
      }
    });
  }
  return entries;
}

}  // namespace sdb::catalog
