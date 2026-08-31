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

#include "catalog/entry/duckdb_table_entry.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>

#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::catalog {

const duckdb::ColumnDefinition* TableEntryColumn(
  const duckdb::TableCatalogEntry& table, ObjectId column_id) {
  return ColumnById(table.GetColumns(), column_id);
}

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         ObjectId column_id) {
  const auto* column = TableEntryColumn(table, column_id);
  return column ? static_cast<int16_t>(column->Logical().index + 1) : 0;
}

bool TableEntryColumnNotNull(const duckdb::TableCatalogEntry& table,
                             ObjectId column_id) {
  const auto* column = TableEntryColumn(table, column_id);
  return column && table.IsNotNull(column->Logical());
}

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint) {
  if (constraint.HasIndex()) {
    return {static_cast<int16_t>(constraint.GetIndex().index + 1)};
  }
  const auto& columns = table.GetColumns();
  std::vector<int16_t> out;
  out.reserve(constraint.GetColumnNames().size());
  for (const auto& name : constraint.GetColumnNames()) {
    const auto column = columns.TryGetColumn(name);
    out.push_back(column ? static_cast<int16_t>(column->Logical().index + 1)
                         : 0);
  }
  return out;
}

SereneDBTableEntry& RequireBaseTable(duckdb::TableCatalogEntry& table) {
  // RTTI is unavoidable here: the caller hands us a generic TableCatalogEntry
  // that may be a SereneDBTableEntry or an entry from another attached
  // catalog -- duckdb::TableCatalogEntry doesn't expose a tag we can extend.
  auto* base = dynamic_cast<SereneDBTableEntry*>(&table);
  if (!base) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("cannot open relation \"", table.name.GetIdentifierName(), "\""));
  }
  return *base;
}

namespace {

// The declared primary key of `table` as positions in its own column list, in
// key order; empty when it declares none. Read off the constraint list, which
// is the entry's own record of the key.
duckdb::vector<duckdb::LogicalIndex> TableEntryPKColumns(
  const duckdb::TableCatalogEntry& table) {
  const auto& columns = table.GetColumns();
  for (const auto& constraint : table.GetConstraints()) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    if (!unique.IsPrimaryKey()) {
      continue;
    }
    if (!unique.HasIndex() &&
        !absl::c_all_of(unique.GetColumnNames(), [&](const auto& name) {
          return columns.ColumnExists(name);
        })) {
      return {};
    }
    return unique.GetLogicalIndexes(columns);
  }
  return {};
}

}  // namespace

duckdb::unique_ptr<duckdb::CreateTableInfo> SereneDBTableEntry::Definition()
  const {
  return duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
    GetInfo());
}

namespace {

// Re-points every foreign key at the table at its other end -- the referenced
// table for the key this one states, the referencing table for the reciprocal
// entry. A rename or a column drop over there has moved the stored names, so
// they are re-derived here from the durable identities.
void RetargetForeignKeys(duckdb::CreateTableInfo& info,
                         const duckdb::CreateTableInfo& table,
                         duckdb::Catalog& catalog,
                         duckdb::ClientContext* context) {
  for (auto& constraint : info.constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    const ObjectId other_id{fk.host_referenced_id};
    const duckdb::CreateTableInfo* other = &table;
    duckdb::unique_ptr<duckdb::CreateTableInfo> held;
    duckdb::Identifier other_schema = info.GetQualifiedName().Schema();
    duckdb::Identifier other_name = info.GetTableName();
    if (other_id.isSet() && other_id != catalog::IdOf(table)) {
      const auto* found =
        FindIn<SereneDBTableEntry>(context, catalog, other_id);
      if (found == nullptr) {
        continue;
      }
      held = found->Definition();
      other = held.get();
      const auto* schema = FindSchema(context, catalog::ParentIdOf(*held));
      other_schema =
        duckdb::Identifier{schema != nullptr ? schema->name.GetIdentifierName()
                                             : std::string_view{}};
      other_name = duckdb::Identifier{held->GetTableName().GetIdentifierName()};
    }
    fk.info.schema = other_schema;
    fk.info.table = other_name;
    const auto& referenced = catalog::StatesForeignKey(fk) ? *other : table;
    fk.pk_columns = catalog::ReferencedKeyNames(fk, &referenced);
    fk.info.pk_keys.clear();
    for (const auto& name : fk.pk_columns) {
      const auto* column =
        catalog::ColumnByName(referenced, name.GetIdentifierName());
      fk.info.pk_keys.emplace_back(column == nullptr ? 0
                                                     : column->Logical().index);
    }
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::CreateTableInfo> SereneDBTableEntry::BuildInfo(
  std::string_view name, SereneDBSchemaEntry& schema, duckdb::Catalog& catalog,
  const duckdb::CreateTableInfo& table, duckdb::ClientContext* context) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
      table.Copy());
  info->SetTableName(duckdb::Identifier{name});
  info->SetSchema(schema.name);
  RetargetForeignKeys(*info, table, catalog, context);
  return info;
}

std::shared_ptr<catalog::SequenceCounter>
SereneDBTableEntry::GetGeneratedPkSequence(
  duckdb::ClientContext& context) const {
  const auto* sequence =
    catalog::FindSession<SereneDBSequenceEntry>(context, _generated_pk_seq_id);
  return sequence ? sequence->Counter() : nullptr;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBTableEntry::Make(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, const duckdb::CreateTableInfo& table,
  catalog::Permissions perm, duckdb::ClientContext* context,
  duckdb::optional_ptr<duckdb::CatalogEntry> superseded) {
  duckdb::shared_ptr<duckdb::CatalogSet> triggers;
  duckdb::shared_ptr<duckdb::DataTable> storage;
  std::shared_ptr<search::SearchTable> search_data;
  if (auto* previous = dynamic_cast<SereneDBTableEntry*>(superseded.get())) {
    triggers = previous->GetTriggerSet();
    search_data = previous->GetSearchData();
    if (auto held = previous->TryGetStorage()) {
      storage = held->shared_from_this();
    }
  }
  // A table on either side of having no columns at all is rebuilt rather
  // than inherited: ALTER refuses to remove the last column, so the reshape
  // is a drop and a create, and rows cannot survive a shape with nowhere to
  // put them. Every other transition hands its rows over.
  if (storage && (storage->Columns().empty() !=
                  (table.columns.PhysicalColumnCount() == 0))) {
    storage.reset();
  }
  auto info = BuildInfo(entry_name, schema, catalog, table, context);
  return duckdb::make_uniq<SereneDBTableEntry>(
    catalog, schema, *info, catalog::IdOf(table), std::move(perm),
    std::move(storage), std::move(search_data), std::move(triggers));
}

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, ObjectId id, catalog::Permissions perm,
  duckdb::shared_ptr<duckdb::DataTable> storage,
  std::shared_ptr<search::SearchTable> search_data,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers)
  : SereneDBTableEntry(
      catalog, schema,
      duckdb::Binder::BindCreateTableCheckpoint(info.Copy(), schema), id,
      std::move(perm), std::move(storage), std::move(search_data),
      std::move(inherited_triggers)) {}

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info, ObjectId id,
  catalog::Permissions perm, duckdb::shared_ptr<duckdb::DataTable> storage,
  std::shared_ptr<search::SearchTable> search_data,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers)
  : duckdb::DuckTableEntry(catalog, schema, *info, StorageAsGiven{},
                           std::move(storage), std::move(inherited_triggers)),
    _search_data(std::move(search_data)),
    _pk_columns(TableEntryPKColumns(*this)),
    _generated_pk_seq_id(catalog::ReadGeneratedPkSeqTag(tags)),
    _engine(catalog::ReadTableEngineTag(tags)) {
  // The definition is a record in the serenedb catalog log, so duckdb neither
  // writes nor reclaims it.
  catalog::AdoptEntryIdentity(*this, id, std::move(perm));
  if (IsSearchTable()) {
    return;
  }
  // The rows, though, are this file's: duckdb owns their WAL records and their
  // blocks, which is what makes a reshape and a drop recoverable.
  duck_managed = true;
  // The rows are filed under this entry's own id wherever they are recorded --
  // the WAL, the checkpoint manifest -- so a rename never reaches them.
  info->Base().oid = oid;
  if (!TryGetStorage() && duckdb::StorageManager::Get(catalog).IsLoaded()) {
    // A create. Boot is the exception: the entry is built from the catalog log
    // before the file it belongs to is open, and its rows arrive afterwards
    // from the checkpoint manifest or the data WAL.
    CreateStorage(*info);
  }
  if (auto rows = TryGetStorage()) {
    // Inherited rows carry the name of the version that held them, which a
    // rename has just moved; every message about them names this one.
    rows->SetTableName(name);
  }
}

void SereneDBTableEntry::UndoAlter(duckdb::ClientContext& /*context*/,
                                   duckdb::AlterInfo& /*info*/) {
  if (auto rows = TryGetStorage()) {
    rows->SetTableName(name);
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBTableEntry::AlteredEntry(
  duckdb::BoundCreateTableInfo& info,
  duckdb::shared_ptr<duckdb::DataTable> new_storage) const {
  auto& altered = info.Base().Cast<duckdb::CreateTableInfo>();
  // A dropped column's grants go with it. Ids are never reissued, so a
  // leftover entry would name a column no reader can resolve while still
  // holding its grantee's role dependency open.
  auto perm = permissions;
  std::erase_if(perm.column_acl, [&](const auto& granted) {
    return catalog::ColumnById(altered, ObjectId{granted.catalog_oid}) ==
           nullptr;
  });
  // duckdb reshaped the entry, so what the new one describes is the reshaped
  // definition it was handed; the identity is this table's, which a reshape
  // never moves.
  return duckdb::make_uniq<SereneDBTableEntry>(
    catalog, Schema(), altered, ObjectId{oid}, std::move(perm),
    std::move(new_storage), _search_data, GetTriggerSet());
}

duckdb::unique_ptr<duckdb::BaseStatistics> SereneDBTableEntry::GetStatistics(
  duckdb::ClientContext& context, duckdb::column_t column_id) {
  if (IsSearchTable()) {
    return nullptr;
  }
  return duckdb::DuckTableEntry::GetStatistics(context, column_id);
}

duckdb::TableFunction SereneDBTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  // Search table: scan the iresearch store directly. The bind data
  // carries the user columns (the generated PK is not stored as a value) plus
  // the rowid (PK bytes) virtual that DELETE/UPDATE consume.
  if (IsSearchTable()) {
    auto& conn_ctx = connector::GetSereneDBContext(context);
    auto reader = conn_ctx.SearchTxn().EnsureSearchTableReader(
      catalog::IdOf(*this),
      [&] { return GetSearchData()->GetDirectoryReader(); });
    auto data = duckdb::make_uniq<connector::TableScanBindData>();
    for (const auto& col : GetColumns().Logical()) {
      data->column_ids.emplace_back(col.CatalogOid());
      data->column_types.push_back(col.Type());
    }
    data->table_entry = this;
    data->entry_kind = connector::ScanEntryKind::SearchTable;
    data->lookup_label = "search";
    data->snapshot = std::make_shared<search::InvertedIndexSnapshot>(
      irs::DirectoryReader{*reader}, nullptr);
    bind_data = std::move(data);
    return connector::CreateIResearchScanFunction();
  }

  return duckdb::DuckTableEntry::GetScanFunction(context, bind_data);
}

bool SereneDBTableEntry::ForceUpdateDelAndInsert() const {
  // Search UPDATE is delete+insert at the index level; the base then projects
  // every physical column so the reinserted row is whole.
  return IsSearchTable();
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  // Search tables identify rows by their PK (or synthetic generated PK)
  // virtual columns rather than a physical rowid; advertise the full set so the
  // INSERT/UPDATE/DELETE binders (BindRowIdColumns) and the scan can resolve
  // them. Transactional tables use the store table's native rowid.
  if (IsSearchTable()) {
    return BuildVirtualColumns(
      *this, catalog.Cast<SereneDBCatalog>().IndexedColumns(ObjectId{oid}));
  }
  auto cols = duckdb::TableCatalogEntry::GetVirtualColumns();
  cols.insert({kColumnIdentifierTableOid,
               duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});
  return cols;
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  if (IsSearchTable()) {
    return BuildRowIdColumns(
      *this, catalog.Cast<SereneDBCatalog>().IndexedColumns(ObjectId{oid}));
  }
  return duckdb::TableCatalogEntry::GetRowIdColumns();
}

duckdb::vector<duckdb::column_t> BuildRowIdColumns(
  const duckdb::TableCatalogEntry& table,
  const std::vector<size_t>& indexed_col_indices) {
  duckdb::vector<duckdb::column_t> result;
  const auto pk_columns = TableEntryPKColumns(table);

  // PK positions in key order, then indexed positions the key does not cover.
  containers::FlatHashSet<size_t> pk_positions;
  pk_positions.reserve(pk_columns.size());
  for (const auto key : pk_columns) {
    if (pk_positions.insert(key.index).second) {
      result.push_back(duckdb::VIRTUAL_COLUMN_START + key.index);
    }
  }
  for (auto idx : indexed_col_indices) {
    if (!pk_positions.contains(idx)) {
      result.push_back(duckdb::VIRTUAL_COLUMN_START + idx);
    }
  }

  if (pk_columns.empty()) {
    result.push_back(kColumnIdentifierGeneratedPk);
  }
  return result;
}

duckdb::virtual_column_map_t BuildVirtualColumns(
  const duckdb::TableCatalogEntry& table,
  const std::vector<size_t>& indexed_col_indices) {
  duckdb::virtual_column_map_t result;
  const auto pk_columns = TableEntryPKColumns(table);
  const auto& columns = table.GetColumns();

  const auto add = [&](size_t position) {
    const auto& column = columns.GetColumn(duckdb::LogicalIndex{position});
    result.insert({duckdb::VIRTUAL_COLUMN_START + position,
                   duckdb::TableColumn(column.Name(), column.Type())});
  };
  for (const auto key : pk_columns) {
    add(key.index);
  }

  for (auto idx : indexed_col_indices) {
    if (!result.contains(duckdb::VIRTUAL_COLUMN_START + idx)) {
      add(idx);
    }
  }

  // tableoid -- always 0, emitted only when referenced
  result.insert({kColumnIdentifierTableOid,
                 duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});

  // COLUMN_IDENTIFIER_EMPTY: the "no data needed" placeholder DuckDB's
  // LogicalGet::GetAnyColumn picks for queries like COUNT(*) that have
  // no real column dependency.
  result.insert({duckdb::COLUMN_IDENTIFIER_EMPTY,
                 duckdb::TableColumn("", duckdb::LogicalType::BOOLEAN)});

  if (pk_columns.empty()) {
    result.insert(
      {kColumnIdentifierGeneratedPk,
       duckdb::TableColumn("rowid", duckdb::LogicalType::ROW_TYPE)});
  }
  return result;
}

duckdb::TableStorageInfo BuildStorageInfo(
  const duckdb::TableCatalogEntry& table) {
  duckdb::TableStorageInfo info;

  // Every key constraint is reported as a unique index so the binder can
  // resolve an ON CONFLICT target against it -- the primary key and every
  // UNIQUE alike; enforcement itself happens elsewhere.
  for (const auto& constraint : table.GetConstraints()) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    duckdb::IndexInfo idx_info;
    idx_info.is_unique = true;
    idx_info.is_primary = unique.IsPrimaryKey();
    idx_info.is_foreign = false;
    for (const auto attnum : KeyConstraintAttnums(table, unique)) {
      if (attnum > 0) {
        idx_info.column_set.insert(static_cast<size_t>(attnum - 1));
      }
    }
    info.index_info.push_back(std::move(idx_info));
  }

  return info;
}

duckdb::column_t SereneDBTableEntry::VirtualToPKColumnIndex(
  duckdb::column_t virtual_id) {
  if (virtual_id >= duckdb::VIRTUAL_COLUMN_START &&
      virtual_id < kColumnIdentifierGeneratedPk) {
    return virtual_id - duckdb::VIRTUAL_COLUMN_START;
  }
  return duckdb::DConstants::INVALID_INDEX;
}

duckdb::TableStorageInfo SereneDBTableEntry::GetStorageInfo(
  duckdb::ClientContext& context) {
  if (IsSearchTable()) {
    return BuildStorageInfo(*this);
  }
  return duckdb::DuckTableEntry::GetStorageInfo(context);
}

namespace {

void AppendIResearchBlockRows(
  const irs::ColumnReader& node, duckdb::idx_t column_id,
  std::vector<duckdb::idx_t>& path, std::string_view type_name, size_t segment,
  uint64_t row_base, const duckdb::virtual_column_map_t& virtual_columns,
  duckdb::vector<duckdb::ColumnSegmentInfo>& out) {
  const auto blocks = node.DataBlocks();
  std::string path_str = "[";
  for (size_t i = 0; i < path.size(); ++i) {
    if (i > 0) {
      path_str += ", ";
    }
    const auto vc = path[i] >= duckdb::VIRTUAL_COLUMN_START
                      ? virtual_columns.find(path[i])
                      : virtual_columns.end();
    if (vc != virtual_columns.end()) {
      absl::StrAppend(&path_str, vc->second.name.GetIdentifierName());
    } else {
      absl::StrAppend(&path_str, path[i]);
    }
  }
  path_str += "]";
  for (size_t block = 0; block < blocks.size(); ++block) {
    const auto& meta = blocks[block];
    auto& info = out.emplace_back();
    info.row_group_index = segment;
    info.column_id = column_id;
    info.column_path = path_str;
    info.segment_idx = segment;
    info.segment_type = std::string{type_name};
    info.segment_start = row_base + node.DataBlockFirstRow(block);
    info.segment_count = meta.tuple_count;
    info.compression_type =
      meta.codec ? duckdb::CompressionTypeToString(meta.codec->type)
                 : std::string{"Uncompressed"};
    info.segment_stats = meta.statistics.ToStruct();
    info.has_updates = false;
    info.persistent = true;
    info.block_id = INVALID_BLOCK;
    info.block_offset = meta.file_offset;
  }
}

void WalkIResearchColumn(const irs::ColumnReader& node, duckdb::idx_t column_id,
                         std::vector<duckdb::idx_t>& path, size_t segment,
                         uint64_t row_base,
                         const duckdb::virtual_column_map_t& virtual_columns,
                         duckdb::vector<duckdb::ColumnSegmentInfo>& out) {
  AppendIResearchBlockRows(node, column_id, path, node.Type().ToString(),
                           segment, row_base, virtual_columns, out);
  if (const auto* validity = node.Validity()) {
    path.push_back(0);
    AppendIResearchBlockRows(*validity, column_id, path, "VALIDITY", segment,
                             row_base, virtual_columns, out);
    path.pop_back();
  }
  if (node.Type().id() == duckdb::LogicalTypeId::STRUCT) {
    for (size_t i = 0; i < node.StructFieldCount(); ++i) {
      path.push_back(i + 1);
      WalkIResearchColumn(node.StructField(i), column_id, path, segment,
                          row_base, virtual_columns, out);
      path.pop_back();
    }
  } else if (const auto* child = node.Child()) {
    path.push_back(1);
    WalkIResearchColumn(*child, column_id, path, segment, row_base,
                        virtual_columns, out);
    path.pop_back();
  }
}

}  // namespace

bool ScanIResearchColumnSegmentInfo(
  const irs::IndexReader& reader,
  std::span<const IResearchColumnBinding> bindings,
  const duckdb::virtual_column_map_t& virtual_columns,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  if (state.position >= reader.size()) {
    return false;
  }
  const auto segment = state.position++;
  const auto& sub_reader = reader[segment];
  const auto* col_reader = sub_reader.GetColReader();
  if (!col_reader) {
    return true;
  }
  uint64_t row_base = 0;
  for (size_t s = 0; s < segment; ++s) {
    row_base += reader[s].docs_count();
  }
  for (const auto& binding : bindings) {
    const auto* column =
      col_reader->Column(static_cast<irs::field_id>(binding.field));
    if (!column) {
      continue;
    }
    std::vector<duckdb::idx_t> path{binding.column_id};
    WalkIResearchColumn(*column, binding.column_id, path, segment, row_base,
                        virtual_columns, result);
  }
  return true;
}

std::vector<IResearchColumnBinding>
SereneDBTableEntry::SearchSegmentInfoBindings() const {
  std::vector<IResearchColumnBinding> bindings;
  for (const auto& col : GetColumns().Physical()) {
    bindings.push_back({col.Physical().index, col.CatalogOid()});
  }
  bindings.push_back(
    {RowIdentityColumnId(*this), catalog::kGeneratedPKId.id()});
  return bindings;
}

duckdb::column_t RowIdentityColumnId(const duckdb::TableCatalogEntry& table) {
  const auto pk_columns = TableEntryPKColumns(table);
  if (!pk_columns.empty()) {
    return duckdb::VIRTUAL_COLUMN_START + pk_columns.front().index;
  }
  return kColumnIdentifierGeneratedPk;
}

std::shared_ptr<irs::DirectoryReader>
SereneDBTableEntry::SearchSegmentInfoReader(duckdb::ClientContext& context) {
  auto& conn_ctx = connector::GetSereneDBContext(context);
  return conn_ctx.SearchTxn().EnsureSearchTableReader(
    catalog::IdOf(*this),
    [&] { return GetSearchData()->GetDirectoryReader(); });
}

void SereneDBTableEntry::InitializeColumnSegmentInfoScan(
  duckdb::ColumnSegmentInfoScanState& state) {
  // A search table has no row groups to walk; the scan below reads its
  // iresearch segments instead.
  if (!IsSearchTable()) {
    duckdb::DuckTableEntry::InitializeColumnSegmentInfoScan(state);
  }
}

bool SereneDBTableEntry::ScanColumnSegmentInfo(
  const duckdb::QueryContext& context,
  duckdb::ColumnSegmentInfoScanState& state,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  auto client = context.GetClientContext();
  if (!client) {
    return false;
  }
  if (IsSearchTable()) {
    return ScanIResearchColumnSegmentInfo(*SearchSegmentInfoReader(*client),
                                          SearchSegmentInfoBindings(),
                                          GetVirtualColumns(), state, result);
  }
  if (!state.row_groups) {
    duckdb::DuckTableEntry::InitializeColumnSegmentInfoScan(state);
  }
  return duckdb::DuckTableEntry::ScanColumnSegmentInfo(context, state, result);
}

}  // namespace sdb::catalog
