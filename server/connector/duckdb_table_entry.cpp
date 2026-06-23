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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

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

SereneDBTableEntry& RequireBaseTable(duckdb::TableCatalogEntry& table) {
  // RTTI is unavoidable here: the caller hands us a generic
  // TableCatalogEntry that may be a SereneDBTableEntry, a
  // SereneDBIndexScanEntry, or an entry from another attached catalog --
  // duckdb::TableCatalogEntry doesn't expose a tag we can extend.
  auto* base = dynamic_cast<SereneDBTableEntry*>(&table);
  if (!base) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("cannot open relation \"", table.name, "\""),
                    ERR_DETAIL("This operation is not supported for indexes."));
  }
  return *base;
}

SereneDBTableEntry::SereneDBTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  duckdb::CreateTableInfo& info, std::shared_ptr<catalog::Table> sdb_table,
  std::vector<size_t> indexed_col_indices)
  : duckdb::TableCatalogEntry(catalog, schema, info),
    _sdb_table(std::move(sdb_table)),
    _indexed_col_indices(std::move(indexed_col_indices)) {}

duckdb::unique_ptr<duckdb::BaseStatistics> SereneDBTableEntry::GetStatistics(
  duckdb::ClientContext& context, duckdb::column_t column_id) {
  SDB_ASSERT(false,
             "facade GetStatistics is unreachable: scans read column stats via "
             "the store-delegated TableScanStatistics, not this entry; if this "
             "fires, delegate to ResolveStoreEntry(context).GetStatistics");
  return nullptr;
}

duckdb::TableCatalogEntry& SereneDBTableEntry::ResolveStoreEntry(
  duckdb::ClientContext& context) const {
  auto store_name = catalog::StoreTableName(ParentCatalog().GetName(),
                                            ParentSchema().name, name);
  return duckdb::Catalog::GetEntry(context, duckdb::CatalogType::TABLE_ENTRY,
                                   std::string{catalog::kStoreDatabaseName},
                                   "main", store_name)
    .Cast<duckdb::TableCatalogEntry>();
}

duckdb::TableFunction SereneDBTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  // Search table: scan the iresearch store directly. The bind data
  // carries the user columns (the generated PK is not stored as a value) plus
  // the rowid (PK bytes) virtual that DELETE/UPDATE consume.
  if (_sdb_table->GetEngine() == catalog::TableEngine::Search) {
    auto data = duckdb::make_uniq<TableScanBindData>();
    data->table = _sdb_table;
    for (const auto& col : _sdb_table->Columns()) {
      if (col.GetId() == catalog::Column::kGeneratedPKId) {
        continue;  // Skip generated PK -- not stored as a value
      }
      data->column_ids.push_back(col.GetId());
      data->column_types.push_back(col.type);
    }
    data->table_entry = this;
    data->entry_kind = ScanEntryKind::BaseTable;
    bind_data = std::move(data);
    return CreateSearchTableScanFunction();
  }

  auto function =
    ResolveStoreEntry(context).GetScanFunction(context, bind_data);
  if (bind_data) {
    if (auto* table_bind =
          dynamic_cast<duckdb::TableScanBindData*>(bind_data.get())) {
      table_bind->display_name = name;
    }
  }
  // tableoid binds on tables (scoring functions and PG compatibility take
  // it as an argument); scoring rewrites consume the reference before any
  // scan would have to materialize it.
  function.get_virtual_columns = StoreScanVirtualColumns;
  return function;
}

duckdb::Catalog& SereneDBTableEntry::GetStorageCatalog(
  duckdb::ClientContext& context) {
  // Search tables have no store table; a write against one is tracked
  // (RegisterDBModify, called by the INSERT/UPDATE/DELETE binders) on the
  // SereneDB catalog itself -- its transaction runs the iresearch commit.
  if (_sdb_table->GetEngine() == catalog::TableEngine::Search) {
    return ParentCatalog();
  }
  return ResolveStoreEntry(context).ParentCatalog();
}

void SereneDBTableEntry::BindUpdateConstraints(duckdb::Binder& binder,
                                               duckdb::LogicalGet& get,
                                               duckdb::LogicalProjection& proj,
                                               duckdb::LogicalUpdate& update,
                                               duckdb::ClientContext& context) {
  // Transactional tables use DuckDB's default update-constraint binding against
  // the store table (partial per-column updates + base index/LIST handling).
  if (_sdb_table->GetEngine() != catalog::TableEngine::Search) {
    duckdb::TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update,
                                                     context);
    return;
  }

  // Search table: deliberately do NOT call the base method -- search
  // UPDATE is delete+insert at the index level, so we project every physical
  // column (below) and recompute STORED gen-cols ourselves.
  auto user_set = update.columns;

  // CHECK passthroughs -- VerifyUpdateConstraints needs every CHECK input
  // present in the chunk, otherwise CreateMockChunk skips the check.
  for (auto& constraint : update.bound_constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      auto& check = constraint->Cast<duckdb::BoundCheckConstraint>();
      duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                              check.bound_columns);
    }
  }

  // STORED gen-col recompute. The bound gen expression lives in
  // update.bound_defaults[phys] (CheckBinder pre-inlined transitive gen->gen
  // refs at CREATE TABLE time, so its leaves are non-gen physical cols).
  // We append a BoundConstantExpression(NULL) sentinel here -- the logical
  // optimizer rejects BoundReferenceExpression on the logical side, so we
  // can't put the real expression in. PlanUpdate keys off the sentinel and
  // substitutes the real expression at physical-plan time.
  const auto& cols = GetColumns();
  for (auto& gen_col : cols.Physical()) {
    if (gen_col.Category() != duckdb::TableColumnType::GENERATED_STORED) {
      continue;
    }
    SDB_ASSERT(gen_col.Physical().index < update.bound_defaults.size());
    auto& bound_gen = *update.bound_defaults[gen_col.Physical().index];

    duckdb::physical_index_set_t deps;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundReferenceExpression>(
      bound_gen, [&](const duckdb::BoundReferenceExpression& r) {
        deps.insert(duckdb::PhysicalIndex(r.index));
      });

    const bool needs_recompute = absl::c_any_of(
      deps, [&](auto d) { return absl::c_contains(user_set, d); });
    if (!needs_recompute) {
      continue;
    }
    duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update, deps);
    update.expressions.push_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value(gen_col.Type())));
    update.columns.push_back(gen_col.Physical());
  }

  // Project every physical column so the deleted-then-reinserted row can be
  // rebuilt in full from the update's input.
  duckdb::physical_index_set_t all_physical;
  for (auto& col : cols.Physical()) {
    all_physical.insert(col.Physical());
  }
  duckdb::LogicalUpdate::BindExtraColumns(*this, get, proj, update,
                                          all_physical);
  update.update_is_del_and_insert = true;
}

duckdb::DuckTableEntry& SereneDBTableEntry::GetStorageTableEntry(
  duckdb::ClientContext& context) {
  return ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  // Search tables identify rows by their PK (or synthetic generated PK)
  // virtual columns rather than a physical rowid; advertise the full set so the
  // INSERT/UPDATE/DELETE binders (BindRowIdColumns) and the scan can resolve
  // them. Transactional tables use the store table's native rowid.
  if (_sdb_table->GetEngine() == catalog::TableEngine::Search) {
    return BuildVirtualColumns(*_sdb_table, _indexed_col_indices);
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
  if (_sdb_table->GetEngine() == catalog::TableEngine::Search) {
    return BuildRowIdColumns(*_sdb_table, _indexed_col_indices);
  }
  return duckdb::TableCatalogEntry::GetRowIdColumns();
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::BuildRowIdColumns(
  const catalog::Table& table, const std::vector<size_t>& indexed_col_indices) {
  duckdb::vector<duckdb::column_t> result;
  const auto& pk_col_ids = table.PKColumns();

  // PK positions (O(1) per id), in PK order; then indexed positions not already
  // covered by the PK set.
  containers::FlatHashSet<size_t> pk_positions;
  pk_positions.reserve(pk_col_ids.size());
  for (auto pk_id : pk_col_ids) {
    const auto pos = table.ColumnPosById(pk_id);
    if (pos < table.Columns().size() && pk_positions.insert(pos).second) {
      result.push_back(duckdb::VIRTUAL_COLUMN_START + pos);
    }
  }
  for (auto idx : indexed_col_indices) {
    if (!pk_positions.contains(idx)) {
      result.push_back(duckdb::VIRTUAL_COLUMN_START + idx);
    }
  }

  if (pk_col_ids.empty()) {
    result.push_back(kColumnIdentifierGeneratedPk);
  }
  return result;
}

duckdb::virtual_column_map_t SereneDBTableEntry::BuildVirtualColumns(
  const catalog::Table& table, const std::vector<size_t>& indexed_col_indices) {
  duckdb::virtual_column_map_t result;
  const auto& pk_col_ids = table.PKColumns();
  const auto& columns = table.Columns();

  // PK columns
  for (auto pk_id : pk_col_ids) {
    const auto pos = table.ColumnPosById(pk_id);
    if (pos < columns.size()) {
      result.insert({duckdb::VIRTUAL_COLUMN_START + pos,
                     duckdb::TableColumn(std::string{columns[pos].GetName()},
                                         columns[pos].type)});
    }
  }

  // Indexed columns (skip if already added as PK)
  for (auto idx : indexed_col_indices) {
    auto virt_id = duckdb::VIRTUAL_COLUMN_START + idx;
    if (!result.contains(virt_id)) {
      result.insert(
        {virt_id, duckdb::TableColumn(std::string{columns[idx].GetName()},
                                      columns[idx].type)});
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
  if (pk_col_ids.empty()) {
    result.insert(
      {kColumnIdentifierGeneratedPk,
       duckdb::TableColumn("rowid", duckdb::LogicalType::ROW_TYPE)});
  }
  return result;
}

duckdb::TableStorageInfo SereneDBTableEntry::BuildStorageInfo(
  const catalog::Table& table) {
  duckdb::TableStorageInfo info;

  // Report PK as a unique index so DuckDB binder can use it for ON CONFLICT
  const auto& pk_col_ids = table.PKColumns();
  if (!pk_col_ids.empty()) {
    duckdb::IndexInfo idx_info;
    idx_info.is_unique = true;
    idx_info.is_primary = true;
    idx_info.is_foreign = false;
    // Map PK column IDs to column indices in the table
    for (auto pk_id : pk_col_ids) {
      const auto pos = table.ColumnPosById(pk_id);
      if (pos < table.Columns().size()) {
        idx_info.column_set.insert(pos);
      }
    }
    info.index_info.push_back(std::move(idx_info));
  }

  // Report UNIQUE constraints as unique indexes too, so the binder can resolve
  // ON CONFLICT (unique_col) against them (enforcement already happens; without
  // this the conflict target binds only to the PK).
  for (const auto& unique_col_ids : table.UniqueConstraints()) {
    duckdb::IndexInfo idx_info;
    idx_info.is_unique = true;
    idx_info.is_primary = false;
    idx_info.is_foreign = false;
    for (auto col_id : unique_col_ids) {
      const auto pos = table.ColumnPosById(col_id);
      if (pos < table.Columns().size()) {
        idx_info.column_set.insert(pos);
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
  return BuildStorageInfo(*_sdb_table);
}

}  // namespace sdb::connector
