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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>

namespace sdb::connector {

// Index entry for SereneDB indexes (secondary indexes
// and inverted iresearch indexes).  Exists primarily so DuckDB recognises
// the index name during DROP INDEX and routes to our DropObject.
class SereneDBIndexEntry final : public duckdb::IndexCatalogEntry {
 public:
  SereneDBIndexEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateIndexInfo& info, std::string table_name);

  duckdb::Identifier GetSchemaName() const final { return schema.name; }
  duckdb::Identifier GetTableName() const final {
    return duckdb::Identifier{_table_name};
  }

 private:
  std::string _table_name;
};

}  // namespace sdb::connector
