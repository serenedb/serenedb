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
#include <optional>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::optimizer {
namespace {

std::shared_ptr<catalog::Table> WriteTarget(duckdb::TableCatalogEntry& table) {
  if (auto* facade = dynamic_cast<connector::SereneDBTableEntry*>(&table)) {
    return facade->GetSereneDBTable();
  }
  return nullptr;
}

using GetMap = absl::flat_hash_map<uint64_t, const duckdb::LogicalGet*>;

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
  RbacVisitor(connector::AccessRecord& access, const GetMap& gets)
    : _access{access}, _gets{gets} {}

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
        _access.ForEach([&](uint64_t idx, connector::RelationAccess& rel) {
          if (idx != get.table_index.index ||
              rel.action != catalog::AclMode::NoRights) {
            return;
          }
          for (const auto& col_id : get.GetColumnIds()) {
            if (!col_id.IsRowIdColumn() && !col_id.IsVirtualColumn()) {
              rel.selected.insert(col_id.GetPrimaryIndex());
            }
          }
        });
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
  auto& access = state->Access();
  absl::Cleanup clear_access = [&] { access.Clear(); };
  GetMap gets;
  BuildGetMap(*plan, gets);
  RbacVisitor{access, gets}.VisitOperator(*plan);
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
