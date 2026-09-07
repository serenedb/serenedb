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
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <memory>
#include <span>

#include "catalog/entry.h"
#include "catalog/table.h"

namespace irs {

class DirectoryReader;
class IndexReader;

}  // namespace irs
namespace sdb::catalog {

class SereneDBSchemaEntry;

struct IResearchColumnBinding {
  duckdb::idx_t column_id;
  uint64_t field;
};

bool ScanIResearchColumnSegmentInfo(
  const irs::IndexReader& reader,
  std::span<const IResearchColumnBinding> bindings,
  const duckdb::virtual_column_map_t& virtual_columns,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result);

// Virtual column ID for tableoid (PG system column). Always returns 0.
// Placed in the special-identifier range alongside COLUMN_IDENTIFIER_ROW_*.
// COLUMN_IDENTIFIER_ROW_NUMBER is 2^64-3, this is 2^64-4.
inline constexpr duckdb::column_t kColumnIdentifierTableOid =
  UINT64_C(18446744073709551612);

// Virtual column ID (2^64-5) for the synthetic primary key on no-PK
// tables/views: row identity in UPDATE/DELETE plans, and the key bytes a
// search table's rows are stored under. Distinct from COLUMN_IDENTIFIER_ROW_ID
// so it is advertised only on no-PK relations.
inline constexpr duckdb::column_t kColumnIdentifierGeneratedPk =
  UINT64_C(18446744073709551611);

// 2^64-6. The pk's flat row half needs an id of its own: duckdb's
// COLUMN_IDENTIFIER_ROW_NUMBER is banned from name binding (TableBinding
// skips it), so a virtual column under that id can never be named in SQL.
inline constexpr duckdb::column_t kColumnIdentifierPkRowNumber =
  UINT64_C(18446744073709551610);
inline constexpr duckdb::column_t kColumnIdentifierPkVirtualStart =
  UINT64_C(9223372036854779904);

constexpr duckdb::column_t PKVirtualColumnId(size_t position) noexcept {
  return kColumnIdentifierPkVirtualStart + position;
}

class SereneDBTableEntry : public duckdb::DuckTableEntry {
 public:
  // storage: the rows this version owns -- inherited from its predecessor or
  // handed over by a reshaping statement; null for a search table (whose rows
  // are an iresearch index) and on a create, where the entry builds its own.
  // inherited_triggers: the trigger set is shared, not versioned, so every
  // ALTER-driven re-creation has to carry it or the table's triggers are
  // silently dropped.
  SereneDBTableEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateTableInfo& info, ObjectId id, catalog::Permissions perm,
    duckdb::shared_ptr<duckdb::DataTable> storage,
    std::shared_ptr<search::SearchTable> search_data,
    duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers = nullptr);

  // One version of one table as PlaceEntry builds it: the entry-facing info
  // (BuildInfo below) plus what the superseded version hands over -- the
  // shared trigger set, the search shard, and the rows, except across a
  // zero-column boundary, where ALTER cannot carry them.
  static duckdb::unique_ptr<duckdb::CatalogEntry> Make(
    duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
    std::string_view entry_name, const duckdb::CreateTableInfo& table,
    catalog::Permissions perm, duckdb::ClientContext* context,
    duckdb::optional_ptr<duckdb::CatalogEntry> superseded);

  // The definition in the form an entry is built from: named as the set files
  // it, schema-qualified, and with every foreign key re-pointed at the table
  // at its other end -- the durable identities are what the entry carries and
  // the names are derived here.
  static duckdb::unique_ptr<duckdb::CreateTableInfo> BuildInfo(
    std::string_view name, SereneDBSchemaEntry& schema,
    duckdb::Catalog& catalog, const duckdb::CreateTableInfo& table,
    duckdb::ClientContext* context);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;

  // A search table's rows are an iresearch index rather than a DataTable, so
  // every caller asking "may I reach past the entry into the storage" is told
  // no for one, and yes for a table whose rows really are duckdb's.
  bool IsDuckTable() const override { return !IsSearchTable(); }

  // A plan names a relation the way the user wrote it. Every serenedb relation
  // lives in the database the session is connected to, so there is nothing for
  // a catalog or schema prefix to disambiguate -- and postgres does not put one
  // in a plan either.
  std::string ScanName() const override { return name.GetIdentifierName(); }

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  void InitializeColumnSegmentInfoScan(
    duckdb::ColumnSegmentInfoScanState& state) final;
  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) final;

  bool ForceUpdateDelAndInsert() const override;

  // A reshape replaces this entry with one of its own kind, carrying the
  // definition forward untouched: what the alter changed is the rows, and the
  // definition this entry projects is the catalog's, which the write that
  // follows replaces with the version the statement decided.
  duckdb::unique_ptr<duckdb::CatalogEntry> AlteredEntry(
    duckdb::BoundCreateTableInfo& info,
    duckdb::shared_ptr<duckdb::DataTable> new_storage) const override;

  // Every write states the whole definition, so the only thing an abandoned
  // one leaves behind is the name it wrote onto the rows it shares with the
  // version that stays. The base takes an AlterTableInfo, which a serenedb
  // rewrite never carries.
  void UndoAlter(duckdb::ClientContext& context,
                 duckdb::AlterInfo& info) override;

  // Convert a virtual column ID (PKVirtualColumnId(i)) back to a real column
  // index. Returns DConstants::INVALID_INDEX if not a PK virtual col.
  static duckdb::column_t VirtualToPKColumnIndex(duckdb::column_t virtual_id);

  // This version as a definition again -- the durable shape, which the log
  // writes and boot reads back. Built from the entry's own duckdb state rather
  // than kept beside it: a second copy could only drift from what the entry
  // says, and everything it states is already here.
  duckdb::unique_ptr<duckdb::CreateTableInfo> Definition() const;

  // Column-level grants, keyed by the column's ObjectId -- the id
  // ColumnDefinition::CatalogOid() carries. Only columns some GRANT has named
  // are present; postgres gives a column no owner of its own, so the entry's
  // own owner answers for all of them.
  const catalog::ColumnAcls& GetColumnAcls() const noexcept {
    return permissions.column_acl;
  }
  // A view into this version, never a copy: the pg_catalog projections build an
  // AclView over what they get back and read it after the walk.
  catalog::AclView GetColumnAcl(ObjectId column_id) const noexcept {
    return catalog::ColumnAclOf(permissions.column_acl, column_id);
  }

  catalog::TableEngine GetEngine() const noexcept { return _engine; }
  catalog::persistence::SearchTableOptions SearchOptions() const noexcept {
    return catalog::ReadSearchOptionTags(tags);
  }
  bool IsSearchTable() const noexcept {
    return _engine == catalog::TableEngine::Search;
  }
  ObjectId GetGeneratedPkSeqId() const noexcept { return _generated_pk_seq_id; }

  // The declared primary key's columns, in key order, as positions in this
  // entry's column list. Empty on a table with no primary key -- there the row
  // identity is the generated PK instead. Decoded once from the constraint
  // list, which is the definition's own record of the key.
  std::span<const duckdb::LogicalIndex> GetPKColumnIndexes() const noexcept {
    return _pk_columns;
  }

  // Runtime state, not definition: the iresearch shard a search table's rows
  // live in, shared across versions -- a second shard over one directory is a
  // second writer -- so it is handed over the way duckdb hands `storage`,
  // never copied. Bound after the entry exists at boot.
  const std::shared_ptr<search::SearchTable>& GetSearchData() const noexcept {
    return _search_data;
  }
  void SetSearchData(std::shared_ptr<search::SearchTable> data) const {
    _search_data = std::move(data);
  }
  // The counter feeding the synthetic primary key: owned by the sequence
  // entry this table's tag names, resolved by id, never by name.
  std::shared_ptr<catalog::SequenceCounter> GetGeneratedPkSequence(
    duckdb::ClientContext& context) const;

 private:
  // The base takes a BoundCreateTableInfo by reference, so the one built for it
  // has to outlive the member-initializer list.
  SereneDBTableEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                     ObjectId id, catalog::Permissions perm,
                     duckdb::shared_ptr<duckdb::DataTable> storage,
                     std::shared_ptr<search::SearchTable> search_data,
                     duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers);

  std::vector<IResearchColumnBinding> SearchSegmentInfoBindings() const;
  std::shared_ptr<irs::DirectoryReader> SearchSegmentInfoReader(
    duckdb::ClientContext& context);

  mutable std::shared_ptr<search::SearchTable> _search_data;
  duckdb::vector<duckdb::LogicalIndex> _pk_columns;
  ObjectId _generated_pk_seq_id;
  catalog::TableEngine _engine;
};

// Virtual columns / rowid columns / storage info of a relation, computed from
// the entry's own column list and constraints. Shared between a table's entry
// and the index-name-as-table wrappers, which advertise the same shape.
duckdb::vector<duckdb::column_t> BuildRowIdColumns(
  const duckdb::TableCatalogEntry& table,
  const std::vector<size_t>& indexed_col_indices);
duckdb::virtual_column_map_t BuildVirtualColumns(
  const duckdb::TableCatalogEntry& table,
  const std::vector<size_t>& indexed_col_indices);
duckdb::TableStorageInfo BuildStorageInfo(
  const duckdb::TableCatalogEntry& table);

// The virtual column a row of `table` is identified by: its first primary key
// column, or the synthetic generated PK when it declares none.
duckdb::column_t RowIdentityColumnId(const duckdb::TableCatalogEntry& table);

// pg_attribute.attnum of the column `column_id` names -- its 1-based position
// in the entry's own column list. Zero when the entry lists no such column,
// which is what postgres writes for an index key that is not a plain column.
int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         ObjectId column_id);

// The column `column_id` names, or null when the entry lists no such column.
const duckdb::ColumnDefinition* TableEntryColumn(
  const duckdb::TableCatalogEntry& table, ObjectId column_id);

// Whether the entry constrains `column_id` to be NOT NULL. False for a column
// it does not list, including the synthetic ones a scan produces.
bool TableEntryColumnNotNull(const duckdb::TableCatalogEntry& table,
                             ObjectId column_id);

// The relation a scan of `entry` reads: a table entry is its own relation,
// while an index-as-table wrapper answers with the id of the relation its
// index hangs off.
ObjectId ScanRelationId(const duckdb::TableCatalogEntry& entry);

// The key columns of a PRIMARY KEY or UNIQUE constraint as attnums, in key
// order. The constraint spells its key either as one logical index or as a
// list of names; both resolve against this entry's column list.
std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint);

// Casts `table` to SereneDBTableEntry or throws an ERRCODE_WRONG_OBJECT_TYPE
// error. Use for DML / CREATE INDEX paths that only support base tables so
// that running them on an index entry produces a friendly error instead of a
// reinterpret_cast assertion.
SereneDBTableEntry& RequireBaseTable(duckdb::TableCatalogEntry& table);

}  // namespace sdb::catalog
