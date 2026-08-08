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
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "catalog/duckdb_entry.h"
#include "catalog/index.h"

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::catalog {

// Index entry for SereneDB indexes (secondary indexes
// and inverted iresearch indexes).  Exists primarily so DuckDB recognises
// the index name during DROP INDEX and routes to our DropObject.
class SereneDBIndexEntry final : public duckdb::IndexCatalogEntry {
 public:
  SereneDBIndexEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateIndexInfo& info, catalog::IndexInfoRef index,
                     std::string table_name);

  duckdb::Identifier GetSchemaName() const final { return schema.name; }
  duckdb::Identifier GetTableName() const final {
    return duckdb::Identifier{_table_name};
  }

  // The relation this index is built on. An index is the one kind with two
  // ancestors -- its name lives in the schema, its rows belong to a relation --
  // so its schema does not answer this.
  ObjectId GetRelationId() const noexcept { return _relation_id; }

  bool IsInverted() const noexcept { return _sdb_index->IsInverted(); }

  // The iresearch directory behind an inverted index, shared with every other
  // version of the same index. Null for a secondary index, whose rows are an
  // ART on the store table.
  //
  // Read through the definition rather than captured: the holder is bound
  // after the entry exists at boot, where the catalog log builds every entry
  // before anything opens a directory.
  const std::shared_ptr<search::InvertedIndexStorage>& GetInvertedData()
    const noexcept {
    return _sdb_index->GetData();
  }

  // The definition this entry is, which is the only place an index lives.
  const catalog::IndexInfoRef& Definition() const noexcept {
    return _sdb_index;
  }

 private:
  catalog::IndexInfoRef _sdb_index;
  std::string _table_name;
  ObjectId _relation_id;
};

}  // namespace sdb::catalog
