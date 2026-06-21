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
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <optional>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::optimizer {
namespace {

void CollectReadColumns(duckdb::LogicalGet& get,
                        connector::RelationAccess& rel) {
  if (rel.action != catalog::AclMode::NoRights) {
    return;
  }
  for (const auto& col_id : get.GetColumnIds()) {
    if (col_id.IsRowIdColumn() || col_id.IsVirtualColumn()) {
      continue;
    }
    rel.selected.insert(col_id.GetPrimaryIndex());
  }
}

std::shared_ptr<catalog::Table> WriteTarget(duckdb::TableCatalogEntry& table) {
  if (auto* facade = dynamic_cast<connector::SereneDBTableEntry*>(&table)) {
    return facade->GetSereneDBTable();
  }
  return nullptr;
}

using GetMap = absl::flat_hash_map<uint64_t, const duckdb::LogicalGet*>;

void BuildGetMap(const duckdb::LogicalOperator& op, GetMap& gets) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    const auto& get = op.Cast<duckdb::LogicalGet>();
    gets[get.table_index.index] = &get;
  }
  for (const auto& child : op.children) {
    BuildGetMap(*child, gets);
  }
}

std::optional<uint64_t> ResolveLogicalColumn(
  const duckdb::BoundColumnRefExpression& col, const GetMap& gets) {
  auto it = gets.find(col.binding.table_index.index);
  if (it == gets.end()) {
    return col.binding.column_index.GetIndex();
  }
  const auto& column_ids = it->second->GetColumnIds();
  const auto proj = col.binding.column_index.GetIndex();
  if (proj >= column_ids.size()) {
    return std::nullopt;
  }
  const auto& cid = column_ids[proj];
  if (cid.IsRowIdColumn() || cid.IsVirtualColumn()) {
    return std::nullopt;
  }
  return cid.GetPrimaryIndex();
}

void CollectExprColumns(const duckdb::Expression& expr,
                        connector::AccessRecord& access, const GetMap& gets) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& col = expr.Cast<duckdb::BoundColumnRefExpression>();
    const auto& name = col.GetName();
    if (name == "rowid" || (!name.empty() && name.front() == '#')) {
      return;
    }
    const auto logical = ResolveLogicalColumn(col, gets);
    if (logical.has_value()) {
      access.ForEach([&](uint64_t table_index, connector::RelationAccess& rel) {
        if (table_index == col.binding.table_index.index) {
          auto& dst = rel.dml_projection ? rel.returned : rel.selected;
          dst.insert(*logical);
        }
      });
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      CollectExprColumns(child, access, gets);
    });
}

void Walk(duckdb::LogicalOperator& op, connector::AccessRecord& access) {
  switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_DELETE: {
      auto& del = op.Cast<duckdb::LogicalDelete>();
      access.MarkWriteTarget(del.table_index.index, WriteTarget(del.table),
                             del.is_truncate ? catalog::AclMode::Truncate
                                             : catalog::AclMode::Delete);
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE: {
      auto& upd = op.Cast<duckdb::LogicalUpdate>();
      std::vector<uint64_t> set_cols;
      if (!upd.update_is_del_and_insert) {
        set_cols.reserve(upd.columns.size());
        for (const auto& phys : upd.columns) {
          set_cols.push_back(phys.index);
        }
      }
      access.MarkWriteTarget(upd.table_index.index, WriteTarget(upd.table),
                             catalog::AclMode::Update, set_cols);
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_INSERT: {
      auto& ins = op.Cast<duckdb::LogicalInsert>();
      std::vector<uint64_t> ins_cols;
      if (ins.column_index_map.empty()) {
        const auto n = ins.expected_types.size();
        for (uint64_t i = 0; i < n; ++i) {
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
      access.MarkWriteTarget(ins.table_index.index, WriteTarget(ins.table),
                             catalog::AclMode::Insert, ins_cols);
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO: {
      auto& merge = op.Cast<duckdb::LogicalMergeInto>();
      access.MarkWriteTarget(merge.table_index.index, WriteTarget(merge.table),
                             catalog::AclMode::Insert |
                               catalog::AclMode::Update |
                               catalog::AclMode::Delete);
      break;
    }
    default:
      break;
  }
  for (auto& child : op.children) {
    Walk(*child, access);
  }
}

bool IsDmlOperator(duckdb::LogicalOperatorType type) {
  switch (type) {
    case duckdb::LogicalOperatorType::LOGICAL_INSERT:
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO:
      return true;
    default:
      return false;
  }
}

void WalkReadGets(duckdb::LogicalOperator& op,
                  connector::AccessRecord& access) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = op.Cast<duckdb::LogicalGet>();
    access.ForEach([&](uint64_t table_index, connector::RelationAccess& rel) {
      if (table_index == get.table_index.index) {
        CollectReadColumns(get, rel);
      }
    });
  }
  for (auto& child : op.children) {
    WalkReadGets(*child, access);
  }
}

void WalkExpressions(duckdb::LogicalOperator& op,
                     connector::AccessRecord& access, const GetMap& gets,
                     bool skip_projection = false) {
  const bool is_rebuild_projection =
    skip_projection &&
    op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION;
  if (!IsDmlOperator(op.type) && !is_rebuild_projection) {
    for (auto& expr : op.expressions) {
      CollectExprColumns(*expr, access, gets);
    }
  }
  const bool children_under_del_insert =
    op.type == duckdb::LogicalOperatorType::LOGICAL_UPDATE &&
    op.Cast<duckdb::LogicalUpdate>().update_is_del_and_insert;
  for (auto& child : op.children) {
    WalkExpressions(*child, access, gets, children_under_del_insert);
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
  auto& access = state->Access();
  absl::Cleanup clear_access = [&] { access.Clear(); };
  GetMap gets;
  BuildGetMap(*plan, gets);
  Walk(*plan, access);
  WalkReadGets(*plan, access);
  WalkExpressions(*plan, access, gets);
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
