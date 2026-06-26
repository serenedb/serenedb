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

#include "connector/duckdb_access_check.h"

#include "auth/privilege.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

const catalog::Table& Current(const catalog::Snapshot& snapshot,
                              const catalog::Table& recorded) {
  if (auto current = snapshot.GetObject<catalog::Table>(recorded.GetId())) {
    return *current;
  }
  return recorded;
}

void RequireColumns(const catalog::Snapshot& snapshot, ObjectId role,
                    const catalog::Table& table, catalog::AclMode need,
                    std::span<const catalog::Column* const> columns) {
  if (auth::HasColumnPrivilege(snapshot, role, table, need, columns)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", table.GetName()));
}

// Resolve logical column indices (the position excluding the generated PK, as
// the planner numbers them) to Column pointers in a single pass over the table.
std::vector<const catalog::Column*> ResolveColumns(
  const catalog::Table& table, const absl::flat_hash_set<uint64_t>& logical) {
  std::vector<const catalog::Column*> out;
  out.reserve(logical.size());
  uint64_t visible = 0;
  for (const auto& col : table.Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    if (logical.contains(visible)) {
      out.push_back(&col);
    }
    ++visible;
  }
  return out;
}

}  // namespace

void AccessRecord::MarkWriteTarget(uint64_t table_index,
                                   std::shared_ptr<catalog::Table> table,
                                   catalog::AclMode action,
                                   const std::vector<uint64_t>& write_columns) {
  if (!table) {
    return;
  }
  const auto id = table->GetId();
  uint64_t target_key = table_index;
  for (const auto& [idx, rel] : _by_table) {
    if (rel.table && rel.table->GetId() == id) {
      target_key = idx;
      break;
    }
  }
  auto apply = [&](RelationAccess& rel) {
    rel.table = table;
    rel.action |= action;
    rel.table_read = false;
    if ((action & catalog::AclMode::Insert) != catalog::AclMode::NoRights) {
      rel.inserted.insert(write_columns.begin(), write_columns.end());
    }
    if ((action & catalog::AclMode::Update) != catalog::AclMode::NoRights) {
      rel.updated.insert(write_columns.begin(), write_columns.end());
    }
  };
  {
    auto& dml_entry = _by_table[table_index];
    apply(dml_entry);
    dml_entry.dml_projection = true;
  }
  if (target_key != table_index) {
    apply(_by_table[target_key]);
  }
}

void AccessRecord::Enforce(ConnectionContext& ctx) const {
  if (_by_table.empty()) {
    return;
  }
  const auto role = ctx.GetRoleId();
  const auto snapshot = ctx.EnsureCatalogSnapshot();

  for (const auto& [table_index, rel] : _by_table) {
    if (rel.inside_view || !rel.table) {
      continue;
    }
    const auto& table = Current(*snapshot, *rel.table);

    constexpr auto kTableActions =
      catalog::AclMode::Delete | catalog::AclMode::Truncate;
    const auto table_action = rel.action & kTableActions;
    if (table_action != catalog::AclMode::NoRights) {
      snapshot->RequireAccess(role, table, table_action);
    }

    // SELECT covers both the columns read directly (selected) and those a DML
    // RETURNING clause reads back (returned). An empty set with table_read set
    // is the "read with no specific column" case (e.g. count(*)) ->
    // table-level.
    if (rel.table_read || !rel.selected.empty() || !rel.returned.empty()) {
      absl::flat_hash_set<uint64_t> read = rel.selected;
      read.insert(rel.returned.begin(), rel.returned.end());
      RequireColumns(*snapshot, role, table, catalog::AclMode::Select,
                     ResolveColumns(table, read));
    }

    if ((rel.action & catalog::AclMode::Update) != catalog::AclMode::NoRights) {
      RequireColumns(*snapshot, role, table, catalog::AclMode::Update,
                     ResolveColumns(table, rel.updated));
    }
    if ((rel.action & catalog::AclMode::Insert) != catalog::AclMode::NoRights) {
      RequireColumns(*snapshot, role, table, catalog::AclMode::Insert,
                     ResolveColumns(table, rel.inserted));
    }
  }
}

}  // namespace sdb::connector
