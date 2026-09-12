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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"

namespace sdb::catalog {

// Catalog entry for `SELECT * FROM idx_name WHERE ...`. Its own identity is the
// index -- that is the name being scanned -- while what governs access is the
// relation the index hangs off: postgres gives an index no ACL of its own, so
// every privilege decision resolves the relation by the id held here.
class SereneDBIndexScanEntry : public duckdb::TableCatalogEntry {
 public:
  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  std::string ScanName() const override { return name.GetIdentifierName(); }

  ObjectId GetIndexedRelationId() const noexcept { return _relation_id; }

  // The rows an index scan reports are the relation's own; a view-backed index
  // has none of its own and says so by overriding this.
  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

 protected:
  SereneDBIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info, ObjectId index_id);

  ObjectId _relation_id;
};

class InvertedIndexScanEntry : public SereneDBIndexScanEntry {
 public:
  ObjectId IndexId() const noexcept { return _index_id; }

  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) final;

 protected:
  InvertedIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info,
                         const catalog::Index& inverted_index);

  virtual std::vector<IResearchColumnBinding> SegmentInfoBindings() const = 0;
  virtual duckdb::column_t RowIdentityColumnId() const = 0;

  std::vector<IResearchColumnBinding> IndexSegmentInfoBindings() const;

  // The index this wrapper projects, by id. The definition is resolved at bind
  // time and the strong reference goes into the bind data, which is what has to
  // outlive the lookup -- the entry itself holds no definition.
  ObjectId _index_id;
};

class TableInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  TableInvertedIndexScanEntry(duckdb::Catalog& catalog,
                              duckdb::SchemaCatalogEntry& schema,
                              duckdb::CreateTableInfo& info,
                              ObjectId relation_id,
                              const catalog::Index& inverted_index,
                              bool search_engine);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

 protected:
  std::vector<IResearchColumnBinding> SegmentInfoBindings() const final;
  duckdb::column_t RowIdentityColumnId() const final;

 private:
  // The indexed relation is Search-backed, so rows are identified by the
  // synthetic rowid. Captured at construction: the shape accessors take no
  // context and so cannot look the relation up themselves.
  bool _search_engine;
};

class ViewInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  ViewInvertedIndexScanEntry(duckdb::Catalog& catalog,
                             duckdb::SchemaCatalogEntry& schema,
                             duckdb::CreateTableInfo& info,
                             const duckdb::ViewCatalogEntry& view,
                             ObjectId relation_id,
                             const catalog::Index& inverted_index);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

 protected:
  std::vector<IResearchColumnBinding> SegmentInfoBindings() const final;
  duckdb::column_t RowIdentityColumnId() const final;

 private:
  // The view version this wrapper projects. The wrapper is that version, so it
  // owns the definition rather than sharing one: every scan it binds reads the
  // shape out of here and takes what it needs with it.
  duckdb::unique_ptr<duckdb::CreateViewInfo> _sdb_view;
  // Captured at construction (the entry is rebuilt per index version):
  // GetVirtualColumns has no context to resolve the definition through, and
  // the pk kind decides whether generated_pk exists at all.
  catalog::PkColumnKind _pk_column{catalog::PkColumnKind::None};
};

class TableSecondaryIndexScanEntry final : public SereneDBIndexScanEntry {
 public:
  TableSecondaryIndexScanEntry(duckdb::Catalog& catalog,
                               duckdb::SchemaCatalogEntry& schema,
                               duckdb::CreateTableInfo& info,
                               ObjectId relation_id,
                               const catalog::CreateIndexInfo& index);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

 private:
  duckdb::TableCatalogEntry& ResolveRelationEntry(
    duckdb::ClientContext& context) const;
};

// The index-as-table wrapper for one index, as PlaceEntry builds it into the
// relation namespace: the entry the scan of the index's name binds to. Null
// when the relation is gone, or for the shape CREATE INDEX already rejects (a
// secondary index over a view).
duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexScanEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, const catalog::CreateIndexInfo& record,
  duckdb::ClientContext* context);

}  // namespace sdb::catalog
