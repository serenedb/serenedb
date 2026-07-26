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
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "catalog/view.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::connector {

// The relation an index wrapper stands in front of, as the facts the wrapper
// answers with. An index has no owner, no ACL and no columns of its own --
// postgres gives it none -- so all of them are the relation's, copied in at
// build time rather than reached for through a pointer into a version the
// wrapper does not own.
struct IndexedRelation {
  ObjectId id;
  duckdb::CatalogType type = duckdb::CatalogType::INVALID;
  std::string name;
  catalog::Permissions perm;
  // Empty for a view, which has no columns of its own to carry grants for.
  catalog::CreateTableInfo::ColumnAcls column_acls;
};

// Catalog entry for `SELECT * FROM idx_name WHERE ...`. Its own identity is the
// index -- that is the name being scanned -- while what governs access is the
// relation the index hangs off: postgres gives an index no ACL of its own, so
// the relation's owner and grants travel here beside it.
class SereneDBIndexScanEntry : public duckdb::TableCatalogEntry {
 public:
  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  // A plan names this wrapper the way the user wrote it -- the index's own
  // name, unqualified, as every serenedb relation is named.
  std::string ScanName() const override { return name.GetIdentifierName(); }

  const catalog::Permissions& GetIndexedRelationPermissions() const {
    return _relation_perm;
  }
  // What an error about this index names: the relation it is built on.
  duckdb::CatalogType GetIndexedRelationType() const noexcept {
    return _relation_type;
  }
  std::string_view GetIndexedRelationName() const noexcept {
    return _relation_name;
  }
  ObjectId GetIndexedRelationId() const noexcept { return _relation_id; }

  // The relation's per-column grants, which are what a column check reads:
  // an index has no columns of its own.
  const catalog::CreateTableInfo::ColumnAcls& GetColumnAcls() const noexcept {
    return _relation_column_acls;
  }

 protected:
  SereneDBIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info,
                         const catalog::CreateIndexInfoBase& index,
                         std::vector<size_t> indexed_col_indices);

  void SetIndexedRelation(IndexedRelation relation);

  std::vector<size_t> _indexed_col_indices;
  catalog::Permissions _relation_perm;
  catalog::CreateTableInfo::ColumnAcls _relation_column_acls;
  duckdb::CatalogType _relation_type{duckdb::CatalogType::INVALID};
  ObjectId _relation_id;
  std::string _relation_name;
};

class InvertedIndexScanEntry : public SereneDBIndexScanEntry {
 public:
  duckdb::vector<duckdb::ColumnSegmentInfo> GetColumnSegmentInfo(
    const duckdb::QueryContext& context,
    const duckdb::ColumnSegmentInfoScanOptions& options) final;
  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) final;

 protected:
  InvertedIndexScanEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         duckdb::CreateTableInfo& info,
                         std::vector<size_t> indexed_col_indices,
                         catalog::IndexInfoRef inverted_index);

  static const catalog::CreateIndexInfoBase& IndexOf(
    const catalog::IndexInfoRef& index);

  virtual std::vector<IResearchColumnBinding> SegmentInfoBindings() const = 0;
  virtual duckdb::column_t RowIdentityColumnId() const = 0;

  std::vector<IResearchColumnBinding> IndexSegmentInfoBindings() const;

  catalog::IndexInfoRef _inverted_index;
};

class TableInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  TableInvertedIndexScanEntry(duckdb::Catalog& catalog,
                              duckdb::SchemaCatalogEntry& schema,
                              duckdb::CreateTableInfo& info,
                              IndexedRelation relation,
                              std::vector<size_t> indexed_col_indices,
                              catalog::IndexInfoRef inverted_index);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;
  duckdb::virtual_column_map_t GetVirtualColumns() const final;

 protected:
  std::vector<IResearchColumnBinding> SegmentInfoBindings() const final;
  duckdb::column_t RowIdentityColumnId() const final;
};

class ViewInvertedIndexScanEntry final : public InvertedIndexScanEntry {
 public:
  ViewInvertedIndexScanEntry(duckdb::Catalog& catalog,
                             duckdb::SchemaCatalogEntry& schema,
                             duckdb::CreateTableInfo& info,
                             const duckdb::ViewCatalogEntry& view,
                             IndexedRelation relation,
                             std::vector<size_t> indexed_col_indices,
                             catalog::IndexInfoRef inverted_index);

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
  // The view version this wrapper projects, snapshotted at construction: the
  // scan's bind data outlives the catalog lookup that produced it.
  std::shared_ptr<const duckdb::CreateViewInfo> _sdb_view;
};

class SecondaryIndexScanEntry : public SereneDBIndexScanEntry {
 public:
  bool IsUnique() const { return _sk_unique; }

 protected:
  SecondaryIndexScanEntry(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema,
                          duckdb::CreateTableInfo& info,
                          const catalog::CreateIndexInfoBase& index,
                          std::vector<size_t> indexed_col_indices,
                          bool sk_unique);

  ObjectId _secondary_index_id;
  bool _sk_unique;
};

class TableSecondaryIndexScanEntry final : public SecondaryIndexScanEntry {
 public:
  TableSecondaryIndexScanEntry(duckdb::Catalog& catalog,
                               duckdb::SchemaCatalogEntry& schema,
                               duckdb::CreateTableInfo& info,
                               IndexedRelation relation,
                               const catalog::CreateIndexInfoBase& index,
                               std::vector<size_t> indexed_col_indices,
                               bool sk_unique);

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

 private:
  // The relation whose rows this index-as-table wrapper reads.
  duckdb::TableCatalogEntry& ResolveRelationEntry(
    duckdb::ClientContext& context) const;
};

}  // namespace sdb::connector
