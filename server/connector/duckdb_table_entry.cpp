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

#include "connector/duckdb_table_entry.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config_variable_names.h"
#include "query/transaction.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

duckdb::virtual_column_map_t StoreScanVirtualColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData> data) {
  auto& bind_data = data->Cast<duckdb::TableScanBindData>();
  auto cols = bind_data.table.GetVirtualColumns();
  cols.insert({kColumnIdentifierTableOid,
               duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});
  return cols;
}

}  // namespace

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         ObjectId column_id) {
  for (const auto& column : table.GetColumns().Logical()) {
    if (column.CatalogOid() == column_id.id()) {
      return static_cast<int16_t>(column.Logical().index + 1);
    }
  }
  return 0;
}

const duckdb::ColumnDefinition* TableEntryColumn(
  const duckdb::TableCatalogEntry& table, ObjectId column_id) {
  for (const auto& column : table.GetColumns().Logical()) {
    if (column.CatalogOid() == column_id.id()) {
      return &column;
    }
  }
  return nullptr;
}

bool TableEntryColumnNotNull(const duckdb::TableCatalogEntry& table,
                             ObjectId column_id) {
  const auto* column = TableEntryColumn(table, column_id);
  if (!column) {
    return false;
  }
  const auto logical = column->Logical();
  for (const auto& constraint : table.GetConstraints()) {
    if (constraint->type == duckdb::ConstraintType::NOT_NULL &&
        constraint->Cast<duckdb::NotNullConstraint>().index == logical) {
      return true;
    }
  }
  return false;
}

const catalog::CreateTableInfo::ColumnAcls* RelationColumnAcls(
  const duckdb::TableCatalogEntry& entry) noexcept {
  if (const auto* table = dynamic_cast<const SereneDBTableEntry*>(&entry)) {
    return &table->GetColumnAcls();
  }
  if (const auto* scan = dynamic_cast<const SereneDBIndexScanEntry*>(&entry)) {
    return &scan->GetColumnAcls();
  }
  return nullptr;
}

ObjectId ScanRelationId(const duckdb::TableCatalogEntry& entry) {
  if (const auto* scan = dynamic_cast<const SereneDBIndexScanEntry*>(&entry)) {
    return scan->GetIndexedRelationId();
  }
  return catalog::IdOf(entry);
}

std::string_view ScanRelationName(const duckdb::TableCatalogEntry& entry) {
  if (const auto* scan = dynamic_cast<const SereneDBIndexScanEntry*>(&entry)) {
    return scan->GetIndexedRelationName();
  }
  return entry.name.GetIdentifierName();
}

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint) {
  const auto& columns = table.GetColumns();
  std::vector<int16_t> out;
  if (constraint.HasIndex()) {
    out.push_back(static_cast<int16_t>(constraint.GetIndex().index + 1));
    return out;
  }
  out.reserve(constraint.GetColumnNames().size());
  for (const auto& name : constraint.GetColumnNames()) {
    out.push_back(
      columns.ColumnExists(name)
        ? static_cast<int16_t>(columns.GetColumn(name).Logical().index + 1)
        : 0);
  }
  return out;
}

SereneDBTableEntry& RequireBaseTable(duckdb::TableCatalogEntry& table) {
  // RTTI is unavoidable here: the caller hands us a generic
  // TableCatalogEntry that may be a SereneDBTableEntry, a
  // SereneDBIndexScanEntry, or an entry from another attached catalog --
  // duckdb::TableCatalogEntry doesn't expose a tag we can extend.
  auto* base = dynamic_cast<SereneDBTableEntry*>(&table);
  if (!base) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("cannot open relation \"", table.name.GetIdentifierName(), "\""),
      ERR_DETAIL("This operation is not supported for indexes."));
  }
  return *base;
}

namespace {

// The entry is built from a definition serenedb already holds, so nothing here
// is bound: the storage comes in ready and the constraints are the catalog's
// record of themselves, not a plan.
duckdb::unique_ptr<duckdb::BoundCreateTableInfo> UnboundInfo(
  duckdb::SchemaCatalogEntry& schema, duckdb::CreateTableInfo& info) {
  return duckdb::make_uniq<duckdb::BoundCreateTableInfo>(schema, info.Copy());
}

}  // namespace

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
    duckdb::vector<duckdb::LogicalIndex> out;
    if (unique.HasIndex()) {
      out.push_back(unique.GetIndex());
      return out;
    }
    out.reserve(unique.GetColumnNames().size());
    for (const auto& name : unique.GetColumnNames()) {
      if (!columns.ColumnExists(name)) {
        return {};
      }
      out.push_back(columns.GetColumn(name).Logical());
    }
    return out;
  }
  return {};
}

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, catalog::TableInfoRef sdb_table,
  catalog::Permissions perm, duckdb::shared_ptr<duckdb::DataTable> storage,
  std::shared_ptr<catalog::TableRuntime> runtime,
  std::vector<size_t> indexed_col_indices,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers)
  : SereneDBTableEntry(
      catalog, schema, UnboundInfo(schema, info), std::move(sdb_table),
      std::move(perm), std::move(storage), std::move(runtime),
      std::move(indexed_col_indices), std::move(inherited_triggers)) {}

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  catalog::TableInfoRef sdb_table, catalog::Permissions perm,
  duckdb::shared_ptr<duckdb::DataTable> storage,
  std::shared_ptr<catalog::TableRuntime> runtime,
  std::vector<size_t> indexed_col_indices,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers)
  : duckdb::DuckTableEntry(catalog, schema, *info, StorageAsGiven{},
                           std::move(storage), std::move(inherited_triggers)),
    _sdb_table(std::move(sdb_table)),
    _runtime(runtime ? std::move(runtime)
                     : std::make_shared<catalog::TableRuntime>()),
    _pk_columns(TableEntryPKColumns(*this)),
    _indexed_col_indices(std::move(indexed_col_indices)),
    _generated_pk_seq_id(catalog::ReadGeneratedPkSeqTag(tags)),
    _engine(catalog::ReadTableEngineTag(tags)) {
  // The definition is a record in the serenedb catalog log, so duckdb neither
  // writes nor reclaims it.
  catalog::AdoptEntryIdentity(*this, _sdb_table->GetId(), std::move(perm));
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
    // from the checkpoint manifest, the data WAL or the store-op gap replay.
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
  return duckdb::make_uniq<SereneDBTableEntry>(
    catalog, schema, info.Base().Cast<duckdb::CreateTableInfo>(), _sdb_table,
    permissions, std::move(new_storage), _runtime, _indexed_col_indices,
    GetTriggerSet());
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
    auto& conn_ctx = GetSereneDBContext(context);
    auto reader = conn_ctx.SearchTxn().EnsureSearchTableReader(
      catalog::IdOf(*this),
      [&] { return GetSearchData()->GetDirectoryReader(); });
    auto data = duckdb::make_uniq<TableScanBindData>();
    for (const auto& col : GetColumns().Logical()) {
      data->column_ids.emplace_back(col.CatalogOid());
      data->column_types.push_back(col.Type());
    }
    data->table_entry = this;
    data->entry_kind = ScanEntryKind::SearchTable;
    data->lookup_label = "search";
    data->snapshot = std::make_shared<search::InvertedIndexSnapshot>(
      irs::DirectoryReader{*reader});
    bind_data = std::move(data);
    return CreateIResearchScanFunction();
  }

  auto function = duckdb::DuckTableEntry::GetScanFunction(context, bind_data);
  // tableoid binds on tables (scoring functions and PG compatibility take
  // it as an argument); scoring rewrites consume the reference before any
  // scan would have to materialize it.
  function.get_virtual_columns = StoreScanVirtualColumns;
  return function;
}

void SereneDBTableEntry::BindUpdateConstraints(duckdb::Binder& binder,
                                               duckdb::LogicalGet& get,
                                               duckdb::LogicalProjection& proj,
                                               duckdb::LogicalUpdate& update,
                                               duckdb::ClientContext& context) {
  // Transactional tables use DuckDB's default update-constraint binding against
  // the store table (partial per-column updates + base index/LIST handling).
  if (!IsSearchTable()) {
    duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                     context);
    return;
  }

  // Search table: deliberately do NOT call the base method -- search UPDATE is
  // delete+insert at the index level, so we project every physical column
  // below. STORED generated columns are recomputed by duckdb's update binder
  // (bind_update.cpp) whenever an assigned column feeds them, so they already
  // ride along in update.columns/expressions; we must not add them again here.

  // CHECK passthroughs -- VerifyUpdateConstraints needs every CHECK input
  // present in the chunk, otherwise CreateMockChunk skips the check.
  for (auto& constraint : update.bound_constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      auto& check = constraint->Cast<duckdb::BoundCheckConstraint>();
      duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                              check.bound_columns);
    }
  }

  // Project every physical column so the deleted-then-reinserted row can be
  // rebuilt in full from the update's input. Columns already in the update set
  // (user assignments + duckdb's generated-column recomputes) are skipped; the
  // rest are added as old-value passthroughs.
  const auto& cols = GetColumns();
  duckdb::physical_index_set_t all_physical;
  for (auto& col : cols.Physical()) {
    all_physical.insert(col.Physical());
  }
  duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                          all_physical);
  update.update_is_del_and_insert = true;
  update.update_column_count = 0;
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  // Search tables identify rows by their PK (or synthetic generated PK)
  // virtual columns rather than a physical rowid; advertise the full set so the
  // INSERT/UPDATE/DELETE binders (BindRowIdColumns) and the scan can resolve
  // them. Transactional tables use the store table's native rowid.
  if (IsSearchTable()) {
    return BuildVirtualColumns(*this, _indexed_col_indices);
  }
  auto cols = duckdb::TableCatalogEntry::GetVirtualColumns();
  cols.insert({kColumnIdentifierTableOid,
               duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});
  return cols;
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  // Search tables have no physical rowid: row identity is the PK (or synthetic
  // generated PK) virtual columns. Transactional tables fall back to DuckDB's
  // default store rowid.
  if (IsSearchTable()) {
    return BuildRowIdColumns(*this, _indexed_col_indices);
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

  // Indexed columns (skip if already added as PK)
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

  // Generated-PK virtual column: only declared on tables without an
  // explicit PK.
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
  // Virtual PK column ids live in
  // [VIRTUAL_COLUMN_START, kColumnIdentifierGeneratedPk):
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
  // The live index list, which is what an UPDATE is checked against: an update
  // that touches a column any index covers has to be rewritten as delete plus
  // insert, and the inverted indexes injected into this list are covered too.
  auto info = duckdb::DuckTableEntry::GetStorageInfo(context);
  const auto covered = [&](const duckdb::IndexInfo& candidate) {
    return std::ranges::any_of(
      info.index_info, [&](const duckdb::IndexInfo& present) {
        return present.column_set == candidate.column_set;
      });
  };
  // Plus the key constraints, which ON CONFLICT resolves its target against
  // even where no index of this file backs them.
  for (auto& constraint_index : BuildStorageInfo(*this).index_info) {
    if (!covered(constraint_index)) {
      info.index_info.push_back(std::move(constraint_index));
    }
  }
  return info;
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

void BuildIResearchColumnSegmentInfo(
  const irs::IndexReader& reader,
  std::span<const IResearchColumnBinding> bindings,
  const duckdb::virtual_column_map_t& virtual_columns,
  duckdb::vector<duckdb::ColumnSegmentInfo>& result) {
  duckdb::ColumnSegmentInfoScanState state;
  while (ScanIResearchColumnSegmentInfo(reader, bindings, virtual_columns,
                                        state, result)) {
  }
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
  auto& conn_ctx = GetSereneDBContext(context);
  return conn_ctx.SearchTxn().EnsureSearchTableReader(
    catalog::IdOf(*this),
    [&] { return GetSearchData()->GetDirectoryReader(); });
}

duckdb::vector<duckdb::ColumnSegmentInfo>
SereneDBTableEntry::GetColumnSegmentInfo(
  const duckdb::QueryContext& context,
  const duckdb::ColumnSegmentInfoScanOptions& options) {
  auto client = context.GetClientContext();
  if (!client) {
    return {};
  }
  if (IsSearchTable()) {
    duckdb::vector<duckdb::ColumnSegmentInfo> result;
    BuildIResearchColumnSegmentInfo(*SearchSegmentInfoReader(*client),
                                    SearchSegmentInfoBindings(),
                                    GetVirtualColumns(), result);
    return result;
  }
  return duckdb::DuckTableEntry::GetColumnSegmentInfo(context, options);
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

}  // namespace sdb::connector
