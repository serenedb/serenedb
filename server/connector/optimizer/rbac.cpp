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

#include "connector/optimizer/rbac.h"

#include <charconv>
#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/enums/logical_operator_type.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "auth/privilege.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

bool Has(catalog::AclMode action, catalog::AclMode bit) {
  return (action & bit) != catalog::AclMode::NoRights;
}

std::optional<uint64_t> ReadIdTag(
  const duckdb::InsertionOrderPreservingMap<std::string>& tags,
  const char* key) {
  auto it = tags.find(key);
  if (it == tags.end()) {
    return std::nullopt;
  }
  uint64_t id = 0;
  const auto& s = it->second;
  if (std::from_chars(s.data(), s.data() + s.size(), id).ec != std::errc{}) {
    return std::nullopt;
  }
  return id;
}

// The SereneDB relation id behind a base scan: planned over a hidden store
// table, whose DuckTableEntry carries the facade id in an "sdb_table_id" tag.
std::optional<ObjectId> ScanRelationId(const duckdb::LogicalGet& get) {
  auto table = get.GetTable();
  if (!table) {
    return std::nullopt;
  }
  if (auto id = ReadIdTag(table->tags, "sdb_table_id")) {
    return ObjectId{*id};
  }
  return std::nullopt;
}

// A view's security mode and owner, from the tags stamped at CREATE VIEW.
struct ViewSecurity {
  bool invoker = false;  // security_invoker=true -> runs as the caller
  ObjectId owner = id::kInvalid;
};

std::optional<ViewSecurity> ReadViewSecurity(
  const duckdb::CatalogEntry& entry) {
  if (entry.type != duckdb::CatalogType::VIEW_ENTRY) {
    return std::nullopt;
  }
  ViewSecurity sec;
  auto mode = entry.tags.find("sdb_security");
  sec.invoker = mode != entry.tags.end() && mode->second == "invoker";
  if (auto owner = ReadIdTag(entry.tags, "sdb_owner")) {
    sec.owner = ObjectId{*owner};
  }
  return sec;
}

// The DML target's relation id: the LogicalInsert/Update/Delete/MergeInto node
// references the SereneDB facade entry directly.
std::optional<ObjectId> DmlRelationId(const duckdb::TableCatalogEntry& table) {
  if (const auto* facade =
        dynamic_cast<const connector::SereneDBTableEntry*>(&table)) {
    return facade->GetSereneDBTable()->GetId();
  }
  return std::nullopt;
}

struct Relation {
  std::shared_ptr<catalog::Table> table;
  catalog::AclMode action = catalog::AclMode::NoRights;
  containers::FlatHashSet<uint64_t> read;   // logical column ids read
  containers::FlatHashSet<uint64_t> write;  // logical column ids written
  // When this scan came from a definer view, the owner its SELECT is checked
  // against instead of the caller (invalid = check the caller, the default).
  ObjectId read_as = id::kInvalid;
};

// table_index -> the role a base scan's SELECT is checked against, for scans
// inlined from a definer view (absent for direct scans and invoker views).
using DefinerMap = containers::FlatHashMap<uint64_t, ObjectId>;

// Walks the plan once: seeds each base relation from its scan (tag) or DML node
// (facade), records the verb and read/write columns, then enforces.
class RbacVisitor final : public duckdb::LogicalOperatorVisitor {
 public:
  RbacVisitor(const catalog::Snapshot& snapshot, const DefinerMap& definers)
    : _snapshot{snapshot}, _definers{definers} {}

  void VisitOperator(duckdb::LogicalOperator& op) override {
    using duckdb::LogicalOperatorType;
    bool is_dml = true;
    switch (op.type) {
      case LogicalOperatorType::LOGICAL_GET: {
        auto& get = op.Cast<duckdb::LogicalGet>();
        if (auto id = ScanRelationId(get)) {
          auto& rel = Seed(get.table_index.index, *id);
          if (auto it = _definers.find(get.table_index.index);
              it != _definers.end()) {
            rel.read_as = it->second;
          }
          _gets[get.table_index.index] = &get;
        }
        is_dml = false;
        break;
      }
      case LogicalOperatorType::LOGICAL_DELETE: {
        auto& del = op.Cast<duckdb::LogicalDelete>();
        if (auto* rel = SeedDml(del.table_index.index, del.table)) {
          rel->action |= del.is_truncate ? catalog::AclMode::Truncate
                                         : catalog::AclMode::Delete;
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_UPDATE: {
        auto& upd = op.Cast<duckdb::LogicalUpdate>();
        if (auto* rel = SeedDml(upd.table_index.index, upd.table)) {
          rel->action |= catalog::AclMode::Update;
          if (!upd.update_is_del_and_insert) {
            for (const auto& phys : upd.columns) {
              rel->write.insert(phys.index);
            }
          }
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_INSERT: {
        auto& ins = op.Cast<duckdb::LogicalInsert>();
        if (auto* rel = SeedDml(ins.table_index.index, ins.table)) {
          rel->action |= catalog::AclMode::Insert;
          if (ins.column_index_map.empty()) {
            for (uint64_t i = 0; i < ins.expected_types.size(); ++i) {
              rel->write.insert(i);
            }
          } else {
            for (uint64_t i = 0; i < ins.column_index_map.size(); ++i) {
              if (ins.column_index_map[duckdb::PhysicalIndex(i)] !=
                  duckdb::DConstants::INVALID_INDEX) {
                rel->write.insert(i);
              }
            }
          }
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_MERGE_INTO: {
        auto& merge = op.Cast<duckdb::LogicalMergeInto>();
        if (auto* rel = SeedDml(merge.table_index.index, merge.table)) {
          rel->action |= catalog::AclMode::Insert | catalog::AclMode::Update |
                         catalog::AclMode::Delete;
        }
        break;
      }
      default:
        is_dml = false;
        break;
    }
    VisitOperatorChildren(op);
    if (!is_dml) {
      VisitOperatorExpressions(op);
    }
  }

  duckdb::unique_ptr<duckdb::Expression> VisitReplace(
    duckdb::BoundColumnRefExpression& col,
    duckdb::unique_ptr<duckdb::Expression>* expr_ptr) override {
    const auto& name = col.GetName();
    if (name != "rowid" && (name.empty() || name.front() != '#')) {
      if (auto it = _relations.find(col.binding.table_index.index);
          it != _relations.end()) {
        if (auto logical = LogicalColumn(col)) {
          it->second.read.insert(*logical);
        }
      }
    }
    return LogicalOperatorVisitor::VisitReplace(col, expr_ptr);
  }

  void Enforce(ConnectionContext& ctx) const {
    const auto role = ctx.GetRoleId();
    for (const auto& [table_index, rel] : _relations) {
      if (!rel.table) {
        continue;
      }
      auto current = _snapshot.GetObject<catalog::Table>(rel.table->GetId());
      const auto& table = current ? *current : *rel.table;

      const auto table_action =
        rel.action & (catalog::AclMode::Delete | catalog::AclMode::Truncate);
      if (table_action != catalog::AclMode::NoRights) {
        _snapshot.RequireAccess(role, table, table_action);
      }
      // A standalone scan (no write verb on this or any sibling relation for
      // the same table) is a plain SELECT, even when no column was projected.
      const bool reads = !rel.read.empty() || IsBareRead(rel);
      if (reads) {
        // A definer view's base read is checked against the view owner, not the
        // caller (PG definer rights); a plain/invoker scan uses the caller.
        const ObjectId read_role =
          rel.read_as == id::kInvalid ? role : rel.read_as;
        Require(read_role, table, catalog::AclMode::Select, rel.read);
      }
      if (Has(rel.action, catalog::AclMode::Update)) {
        Require(role, table, catalog::AclMode::Update, rel.write);
      }
      if (Has(rel.action, catalog::AclMode::Insert)) {
        Require(role, table, catalog::AclMode::Insert, rel.write);
      }
    }
  }

 private:
  Relation& Seed(uint64_t table_index, ObjectId relation_id) {
    auto& rel = _relations[table_index];
    if (!rel.table) {
      rel.table = _snapshot.GetObject<catalog::Table>(relation_id);
    }
    return rel;
  }

  Relation* SeedDml(uint64_t table_index,
                    const duckdb::TableCatalogEntry& table) {
    auto id = DmlRelationId(table);
    return id ? &Seed(table_index, *id) : nullptr;
  }

  // Maps a column ref to its logical (0-based, generated-PK-excluded) position,
  // via the scan's projection list. Without a scan the binding index is
  // logical.
  std::optional<uint64_t> LogicalColumn(
    const duckdb::BoundColumnRefExpression& col) const {
    const auto proj = col.binding.column_index.GetIndex();
    auto it = _gets.find(col.binding.table_index.index);
    if (it == _gets.end()) {
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

  bool IsBareRead(const Relation& rel) const {
    if (Has(rel.action, catalog::AclMode::Insert | catalog::AclMode::Update |
                          catalog::AclMode::Delete |
                          catalog::AclMode::Truncate)) {
      return false;
    }
    // Another relation entry may be the write target for the same table.
    for (const auto& [_, other] : _relations) {
      if (other.table && rel.table &&
          other.table->GetId() == rel.table->GetId() &&
          Has(other.action,
              catalog::AclMode::Insert | catalog::AclMode::Update |
                catalog::AclMode::Delete | catalog::AclMode::Truncate)) {
        return false;
      }
    }
    return true;
  }

  void Require(ObjectId role, const catalog::Table& table,
               catalog::AclMode need,
               const containers::FlatHashSet<uint64_t>& logical) const {
    std::vector<const catalog::Column*> cols;
    cols.reserve(logical.size());
    uint64_t visible = 0;
    for (const auto& col : table.Columns()) {
      if (col.GetId() == catalog::Column::kGeneratedPKId) {
        continue;
      }
      if (logical.contains(visible)) {
        cols.push_back(&col);
      }
      ++visible;
    }
    if (auth::HasColumnPrivilege(_snapshot, role, table, need, cols)) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for table ", table.GetName()));
  }

  const catalog::Snapshot& _snapshot;
  const DefinerMap& _definers;
  absl::flat_hash_map<uint64_t, Relation> _relations;
  containers::FlatHashMap<uint64_t, const duckdb::LogicalGet*> _gets;
};

void CollectAndEnforce(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto state =
    input.context.registered_state->Get<connector::SereneDBClientState>(
      connector::kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.EnsureCatalogSnapshot();

  const auto caller = ctx.GetRoleId();
  containers::FlatHashMap<uint64_t, std::vector<ViewSecurity>> chains;
  for (const auto& req : requirements) {
    Seed(relations, req,
         snapshot->GetObject<catalog::Table>(ObjectId{req.table_id}));
  }

  GetMap gets;
  BuildGetMap(*plan, gets);
  ReadColumnVisitor{relations, gets}.VisitOperator(*plan);
  visitor.Enforce(ctx);
}

}  // namespace

void RegisterRbacOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .pre_optimize_function = &CollectAndEnforce,
               });
}

}  // namespace sdb::optimizer
