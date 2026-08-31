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
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "catalog/entry.h"
#include "catalog/index.h"
#include "catalog/table.h"

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::catalog {

class SereneDBSchemaEntry;

// Index entry for SereneDB indexes: a plain ART, which is duckdb's own, and an
// inverted iresearch one, which carries a serenedb object.
class SereneDBIndexEntry final : public duckdb::DuckIndexEntry {
 public:
  // `relation_is_table` says whether the covered positions are published into
  // the catalog's indexed-columns registry -- they are the relation's live
  // state, and a view carries no such state. duckdb keeps its own index list
  // on DataTableInfo for the same reason.
  SereneDBIndexEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    const catalog::CreateIndexInfo& info,
    duckdb::shared_ptr<duckdb::IndexDataTableInfo> storage_info,
    std::string table_name, bool relation_is_table,
    std::vector<size_t> relation_col_positions,
    std::shared_ptr<search::InvertedIndexStorage> inverted_data);

  // One version of one index as PlaceEntry builds it: the definition already
  // carries the name, the index type, the UNIQUE constraint and the comment;
  // the relation it hangs off is resolved here -- the info names it by id,
  // the entry by name.
  static duckdb::unique_ptr<duckdb::CatalogEntry> Make(
    duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
    const catalog::CreateIndexInfo& index, duckdb::ClientContext* context);

  duckdb::Identifier GetSchemaName() const final { return ParentSchema().name; }
  duckdb::Identifier GetTableName() const final {
    return duckdb::Identifier{_table_name};
  }

  // The relation this index is built on. An index is the one kind with two
  // ancestors -- its name lives in the schema, its rows belong to a relation --
  // so its schema does not answer this.
  ObjectId GetRelationId() const noexcept { return _relation_id; }

  bool IsInverted() const noexcept { return _sdb_index != nullptr; }

  // Runtime state, not definition: the iresearch directory an inverted index's
  // postings live in, shared across versions -- a second directory over one
  // path is a second writer -- so it is handed over the way duckdb hands
  // `storage`, never copied. Null for a secondary index, and bound after the
  // entry exists at boot and on the create that opens it.
  const std::shared_ptr<search::InvertedIndexStorage>& GetInvertedData()
    const noexcept {
    return _inverted_data;
  }
  void SetInvertedData(
    std::shared_ptr<search::InvertedIndexStorage> data) const {
    _inverted_data = std::move(data);
  }

  // The inverted index this entry is, null when it is a plain ART -- that one
  // is duckdb's, and the fields the base holds are the whole of it. Every
  // version that leaves it unchanged shares it, and what a background feed
  // keeps hold of is the encoding config the runtime publishes, not this.
  const catalog::Index& Definition() const noexcept {
    SDB_ASSERT(_sdb_index);
    return *_sdb_index;
  }

  // For a reader that outlives this version: the same index, shared, not a copy
  // of it.
  const std::shared_ptr<const catalog::Index>& DefinitionPtr() const noexcept {
    return _sdb_index;
  }

  // The durable shape of this version: serenedb's own index record, not the
  // plain CreateIndexInfo duckdb would build -- the record has to name the
  // relation the rows belong to, and for an inverted index the object itself.
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;

  // The version a RENAME produces, for CatalogSet::AlterEntry to chain -- the
  // one alter an index takes this way (options mutate the live object, a
  // comment rebuilds through its own road). Also files the store's rename op:
  // the store mirrors the display name.
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;

  // The rollback of the CREATE this version was: the base detaches the built
  // index from the live list, and the iresearch directory -- which no
  // committed version owns -- goes with it. A rolled-back alter or reindex
  // keeps both; the previous version still answers for them.
  void Rollback(duckdb::CatalogEntry& prev_entry) final;

 private:
  const std::shared_ptr<const catalog::Index> _sdb_index;
  // A plain ART's own state: duckdb keys it by expression, the catalog files it
  // by the column ids those name, which outlive a rename.
  std::vector<ColumnId> _key_columns;
  std::vector<ColumnId> _referenced_columns;
  mutable std::shared_ptr<search::InvertedIndexStorage> _inverted_data;
  std::string _table_name;
  ObjectId _relation_id;
};

}  // namespace sdb::catalog
