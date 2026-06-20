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
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
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

// Enforce SELECT on the table/view column the binder is reading. `column_index`
// is the DuckDB logical index, which excludes the internal generated-PK column
// (it is not part of CreateTableInfo) -- so it lines up with the i-th
// user-visible catalog column. Fired per referenced read column at bind time
// (the analogue of PostgreSQL markVarForSelectPriv); internal/bootstrap queries
// have no client state and are skipped.
void EnforceColumnRead(duckdb::ClientContext& context,
                       const catalog::Table& table,
                       duckdb::column_t column_index) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto& conn_ctx = state->GetConnectionContext();
  conn_ctx.EnsureCatalogSnapshot()->RequireColumnAccess(
    conn_ctx.GetRoleId(), table, catalog::AclMode::Select, column_index);
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
  return ResolveStoreEntry(context).ParentCatalog();
}

duckdb::virtual_column_map_t SereneDBTableEntry::GetVirtualColumns() const {
  auto cols = duckdb::TableCatalogEntry::GetVirtualColumns();
  cols.insert({kColumnIdentifierTableOid,
               duckdb::TableColumn("tableoid", duckdb::LogicalType::BIGINT)});
  return cols;
}

duckdb::vector<duckdb::column_t> SereneDBTableEntry::BuildRowIdColumns(
  const catalog::Table& table, const std::vector<size_t>& indexed_col_indices) {
  duckdb::vector<duckdb::column_t> result;
  const auto& pk_col_ids = table.PKColumns();
  const auto& columns = table.Columns();

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
