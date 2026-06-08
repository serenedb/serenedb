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
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

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
  return nullptr;
}

duckdb::TableFunction SereneDBTableEntry::GetScanFunction(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto shard =
    conn_ctx.EnsureCatalogSnapshot()->GetTableShard(_sdb_table->GetId());
  SDB_ASSERT(shard);
  auto data = duckdb::make_uniq<TableScanBindData>();
  data->table = _sdb_table;
  for (const auto& col : _sdb_table->Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;  // Skip generated PK -- not stored as a value
    }
    data->column_ids.push_back(col.GetId());
    data->column_types.push_back(col.type);
  }
  // Always include rowid (PK bytes) as the last column for DELETE/UPDATE
  data->has_rowid = true;
  data->table_entry = this;
  data->entry_kind = ScanEntryKind::BaseTable;

  bind_data = std::move(data);
  // Storage-kind dispatch. The kSearch path bypasses the rocksdb scan
  // entirely (CreateSearchTableScanFunction reads the iresearch
  // columnstore via ColumnstoreMaterializer); the kRocksDB path is the
  // original PK-iterator-based fullscan. Other dispatch points (UPDATE /
  // DELETE / TRUNCATE / CREATE INDEX) still trip RejectIfSearchTable
  // upstream because they resolve the target through this scan bind --
  // until their respective milestones land, the M2 PR 2.3 stub message
  // surfaces.
  if (shard->GetStorage() == catalog::StorageKind::kSearch) {
    return CreateSearchTableScanFunction();
  }
  return CreateTableFullscanFunction();
}

void SereneDBTableEntry::BindUpdateConstraints(duckdb::Binder& binder,
                                               duckdb::LogicalGet& get,
                                               duckdb::LogicalProjection& proj,
                                               duckdb::LogicalUpdate& update,
                                               duckdb::ClientContext& context) {
  // We deliberately do NOT call
  // duckdb::TableCatalogEntry::BindUpdateConstraints. The base class also flips
  // update_is_del_and_insert + projects every physical column when any SET
  // column is indexed (or is a LIST). For our storage that's pure overhead
  // (RocksDB does partial per-column updates; SereneDBPhysicalUpdate already
  // does delete-and-insert at the secondary index level for every UPDATE) AND
  // it would silently overwrite the gen col recompute below with stale "i=i"
  // passthroughs.
  //
  // TODO: handle update.return_chunk (RETURNING *) when we add support.

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
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::BuildRowIdColumns(
  const catalog::Table& table, const std::vector<size_t>& indexed_col_indices) {
  duckdb::vector<duckdb::column_t> result;
  const auto& pk_col_ids = table.PKColumns();
  const auto& columns = table.Columns();

  // Collect unique column indices: PK columns + indexed columns
  containers::FlatHashSet<size_t> needed;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == pk_id) {
        needed.insert(i);
        break;
      }
    }
  }
  for (auto idx : indexed_col_indices) {
    needed.insert(idx);
  }

  // Register as virtual columns in stable order (PK first, then indexed)
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == pk_id) {
        result.push_back(duckdb::VIRTUAL_COLUMN_START + i);
        break;
      }
    }
  }
  for (auto idx : indexed_col_indices) {
    if (!needed.contains(idx)) {
      continue;  // already added as PK
    }
    // Only add if not already in the PK set
    bool is_pk = false;
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].GetId() == pk_id && i == idx) {
          is_pk = true;
          break;
        }
      }
      if (is_pk) {
        break;
      }
    }
    if (!is_pk) {
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
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == pk_id) {
        result.insert({duckdb::VIRTUAL_COLUMN_START + i,
                       duckdb::TableColumn(std::string{columns[i].GetName()},
                                           columns[i].type)});
        break;
      }
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
    const auto& columns = table.Columns();
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].GetId() == pk_id) {
          idx_info.column_set.insert(i);
          break;
        }
      }
    }
    info.index_info.push_back(std::move(idx_info));
  }

  return info;
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::GetRowIdColumns() const {
  return BuildRowIdColumns(*_sdb_table, _indexed_col_indices);
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  return BuildVirtualColumns(*_sdb_table, _indexed_col_indices);
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
