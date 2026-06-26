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

#include "connector/optimizer/rbac_collect.h"

#include <cstdint>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "auth/privilege.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

struct RelationAccess {
  std::shared_ptr<catalog::Table> table;
  catalog::AclMode action = catalog::AclMode::NoRights;
  containers::FlatHashSet<uint64_t> selected;
  containers::FlatHashSet<uint64_t> returned;
  containers::FlatHashSet<uint64_t> updated;
  containers::FlatHashSet<uint64_t> inserted;
  bool table_read = false;
  bool inside_view = false;
  bool dml_projection = false;
};

class AccessRecord {
 public:
  RelationAccess& ForTable(uint64_t table_index) {
    return _by_table[table_index];
  }

  void MarkWriteTarget(uint64_t table_index,
                       std::shared_ptr<catalog::Table> table,
                       catalog::AclMode action,
                       const std::vector<uint64_t>& write_columns = {});

  template<typename Fn>
  void ForEach(Fn&& fn) {
    for (auto& [table_index, rel] : _by_table) {
      fn(table_index, rel);
    }
  }

  void Enforce(ConnectionContext& ctx) const;

 private:
  absl::flat_hash_map<uint64_t, RelationAccess> _by_table;
};

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
  const catalog::Table& table,
  const containers::FlatHashSet<uint64_t>& logical) {
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
      containers::FlatHashSet<uint64_t> read = rel.selected;
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
namespace sdb::optimizer {
namespace {

std::shared_ptr<catalog::Table> WriteTarget(duckdb::TableCatalogEntry& table) {
  if (auto* facade = dynamic_cast<connector::SereneDBTableEntry*>(&table)) {
    return facade->GetSereneDBTable();
  }
  return nullptr;
}

using GetMap = containers::FlatHashMap<uint64_t, const duckdb::LogicalGet*>;

std::optional<uint64_t> ResolveLogicalColumn(
  const duckdb::BoundColumnRefExpression& col, const GetMap& gets) {
  auto it = gets.find(col.binding.table_index.index);
  const auto proj = col.binding.column_index.GetIndex();
  if (it == gets.end()) {
    return proj;
  }
  const auto& column_ids = it->second->GetColumnIds();
  if (proj >= column_ids.size()) {
    return std::nullopt;
  }
  const auto& cid = column_ids[proj];
  if (cid.IsRowIdColumn() || cid.IsVirtualColumn()) {
    return std::nullopt;
  }
  return cid.GetPrimaryIndex();
}

class RbacVisitor final : public duckdb::LogicalOperatorVisitor {
 public:
  RbacVisitor(connector::AccessRecord& access, const GetMap& gets,
              const catalog::Snapshot& snapshot)
    : _access{access}, _gets{gets}, _snapshot{snapshot} {}

  void VisitOperator(duckdb::LogicalOperator& op) override {
    using duckdb::LogicalOperatorType;
    bool is_dml = true;
    switch (op.type) {
      case LogicalOperatorType::LOGICAL_DELETE: {
        auto& del = op.Cast<duckdb::LogicalDelete>();
        _access.MarkWriteTarget(del.table_index.index, WriteTarget(del.table),
                                del.is_truncate ? catalog::AclMode::Truncate
                                                : catalog::AclMode::Delete);
        break;
      }
      case LogicalOperatorType::LOGICAL_UPDATE: {
        auto& upd = op.Cast<duckdb::LogicalUpdate>();
        std::vector<uint64_t> set_cols;
        if (!upd.update_is_del_and_insert) {
          for (const auto& phys : upd.columns) {
            set_cols.push_back(phys.index);
          }
        }
        _access.MarkWriteTarget(upd.table_index.index, WriteTarget(upd.table),
                                catalog::AclMode::Update, set_cols);
        break;
      }
      case LogicalOperatorType::LOGICAL_INSERT: {
        auto& ins = op.Cast<duckdb::LogicalInsert>();
        std::vector<uint64_t> ins_cols;
        if (ins.column_index_map.empty()) {
          for (uint64_t i = 0; i < ins.expected_types.size(); ++i) {
            ins_cols.push_back(i);
          }
        } else {
          for (uint64_t i = 0; i < ins.column_index_map.size(); ++i) {
            if (ins.column_index_map[duckdb::PhysicalIndex(i)] !=
                duckdb::DConstants::INVALID_INDEX) {
              ins_cols.push_back(i);
            }
          }
        }
        _access.MarkWriteTarget(ins.table_index.index, WriteTarget(ins.table),
                                catalog::AclMode::Insert, ins_cols);
        break;
      }
      case LogicalOperatorType::LOGICAL_MERGE_INTO: {
        auto& merge = op.Cast<duckdb::LogicalMergeInto>();
        _access.MarkWriteTarget(
          merge.table_index.index, WriteTarget(merge.table),
          catalog::AclMode::Insert | catalog::AclMode::Update |
            catalog::AclMode::Delete);
        break;
      }
      case LogicalOperatorType::LOGICAL_GET: {
        auto& get = op.Cast<duckdb::LogicalGet>();
        // A SereneDB base-table scan is a native seq_scan over the store table,
        // so get.GetTable() is the store entry, not the facade -- resolve the
        // catalog table from the scan bind_data instead.
        auto* bind =
          dynamic_cast<connector::SereneDBScanBindData*>(get.bind_data.get());
        auto table = bind
                       ? _snapshot.GetObject<catalog::Table>(bind->RelationId())
                       : nullptr;
        fprintf(stderr,
                "[RBACGET] ti=%llu raw_bd=%p sdb_bd=%p relid=%llu table=%p\n",
                (unsigned long long)get.table_index.index,
                (void*)get.bind_data.get(), (void*)bind,
                bind ? (unsigned long long)bind->RelationId().id() : 0ULL,
                (void*)table.get());
        if (table) {
          // The same table_index may already carry a write action stamped by
          // MarkWriteTarget (the GET under a DML). Record the table, the
          // view-provenance, that it is read, and the columns the scan
          // projects.
          auto& rel = _access.ForTable(get.table_index.index);
          rel.table = table;
          rel.table_read = true;
          rel.inside_view = bind->bound_inside_view;
          for (const auto& col_id : get.GetColumnIds()) {
            if (!col_id.IsRowIdColumn() && !col_id.IsVirtualColumn()) {
              rel.selected.insert(col_id.GetPrimaryIndex());
            }
          }
        }
        is_dml = false;
        break;
      }
      default:
        is_dml = false;
        break;
    }

    const bool skip_projection =
      _skip_next_projection &&
      op.type == LogicalOperatorType::LOGICAL_PROJECTION;
    const bool prev_skip = _skip_next_projection;
    _skip_next_projection =
      op.type == LogicalOperatorType::LOGICAL_UPDATE &&
      op.Cast<duckdb::LogicalUpdate>().update_is_del_and_insert;

    VisitOperatorChildren(op);
    if (!is_dml && !skip_projection) {
      VisitOperatorExpressions(op);
    }
    _skip_next_projection = prev_skip;
  }

  duckdb::unique_ptr<duckdb::Expression> VisitReplace(
    duckdb::BoundColumnRefExpression& col,
    duckdb::unique_ptr<duckdb::Expression>* expr_ptr) override {
    const auto& name = col.GetName();
    if (name != "rowid" && (name.empty() || name.front() != '#')) {
      if (auto logical = ResolveLogicalColumn(col, _gets)) {
        _access.ForEach([&](uint64_t idx, connector::RelationAccess& rel) {
          if (idx == col.binding.table_index.index) {
            (rel.dml_projection ? rel.returned : rel.selected).insert(*logical);
          }
        });
      }
    }
    return LogicalOperatorVisitor::VisitReplace(col, expr_ptr);
  }

 private:
  connector::AccessRecord& _access;
  const GetMap& _gets;
  const catalog::Snapshot& _snapshot;
  bool _skip_next_projection = false;
};

void BuildGetMap(const duckdb::LogicalOperator& op, GetMap& gets) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    const auto& get = op.Cast<duckdb::LogicalGet>();
    gets[get.table_index.index] = &get;
  }
  for (const auto& child : op.children) {
    BuildGetMap(*child, gets);
  }
}

void CollectAndEnforce(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto state =
    input.context.registered_state->Get<connector::SereneDBClientState>(
      connector::kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto snapshot = state->GetConnectionContext().EnsureCatalogSnapshot();
  connector::AccessRecord access;
  GetMap gets;
  BuildGetMap(*plan, gets);
  RbacVisitor{access, gets, *snapshot}.VisitOperator(*plan);
  access.Enforce(state->GetConnectionContext());
}

}  // namespace

void RegisterRbacCollectOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .pre_optimize_function = &CollectAndEnforce,
               });
}

}  // namespace sdb::optimizer
