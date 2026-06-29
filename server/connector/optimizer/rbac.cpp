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

#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <memory>
#include <vector>

#include "auth/privilege.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

bool Has(duckdb::AccessVerb verb, duckdb::AccessVerb bit) {
  return (static_cast<uint8_t>(verb) & static_cast<uint8_t>(bit)) != 0;
}

catalog::AclMode ReadAcl(duckdb::AccessVerb verb, catalog::AclMode bit) {
  return Has(verb, static_cast<duckdb::AccessVerb>(static_cast<uint8_t>(bit)))
           ? bit
           : catalog::AclMode::NoRights;
}

// The SereneDB relation behind a bound entry: the base-table facade, or the
// table/view an index-as-table entry (`SELECT * FROM <index_name>`) scans.
// Null for vendored DuckDB entries (e.g. system tables), which we do not own.
std::shared_ptr<const catalog::Object> SereneDBRelation(
  const duckdb::CatalogEntry& entry) {
  if (const auto* facade =
        dynamic_cast<const connector::SereneDBTableEntry*>(&entry)) {
    return facade->GetSereneDBTable();
  }
  if (const auto* index =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(&entry)) {
    return index->GetSereneDBRelation();
  }
  return nullptr;
}

std::shared_ptr<catalog::Table> BaseTable(const duckdb::CatalogEntry& entry) {
  auto rel = SereneDBRelation(entry);
  if (rel && rel->GetType() == catalog::ObjectType::Table) {
    return std::static_pointer_cast<catalog::Table>(
      std::const_pointer_cast<catalog::Object>(rel));
  }
  return nullptr;
}

void RequireColumns(const catalog::Snapshot& snapshot, ObjectId role,
                    const catalog::Table& table, catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
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
  if (auth::HasColumnPrivilege(snapshot, role, table, need, cols)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", table.GetName()));
}

void RequireView(const catalog::Snapshot& snapshot, ObjectId role,
                 const catalog::Object& view) {
  if (auth::HasPrivilege(snapshot, role, view, catalog::AclMode::Select)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for view ", view.GetName()));
}

std::shared_ptr<catalog::Object> Relation(const catalog::Snapshot& snapshot,
                                          ObjectId database,
                                          const duckdb::CatalogEntry& entry) {
  return snapshot.GetRelation(catalog::NoAccessCheck(), database,
                              entry.ParentSchema().name, entry.name);
}

ObjectId EffectiveRole(const catalog::Snapshot& snapshot, ObjectId database,
                       ObjectId caller, const duckdb::CatalogEntry* who) {
  if (!who) {
    return caller;
  }
  auto view = Relation(snapshot, database, *who);
  return view ? view->GetOwner() : caller;
}

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
  const auto database = ctx.GetDatabaseId();

  const auto& properties = input.optimizer.binder.GetStatementProperties();
  const auto& reqs = properties.access_requirements;

  containers::FlatHashSet<uint64_t> write_targets;
  for (const auto& req : reqs) {
    if (req.table &&
        Has(req.verb, duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
                        duckdb::AccessVerb::DELETE |
                        duckdb::AccessVerb::TRUNCATE)) {
      if (auto t = BaseTable(*req.table)) {
        write_targets.insert(t->GetId().id());
      }
    }
  }

  for (const auto& req : reqs) {
    if (!req.table) {
      continue;
    }
    const ObjectId role = EffectiveRole(*snapshot, database, caller, req.who);

    // A view relation (a real VIEW_ENTRY, or an index-as-table over a view):
    // PostgreSQL checks SELECT on the view itself.
    const bool is_view =
      req.table->type == duckdb::CatalogType::VIEW_ENTRY ||
      [&] {
        auto rel = SereneDBRelation(*req.table);
        return rel && rel->GetType() == catalog::ObjectType::PgSqlView;
      }();
    if (is_view) {
      std::shared_ptr<const catalog::Object> view =
        req.table->type == duckdb::CatalogType::VIEW_ENTRY
          ? Relation(*snapshot, database, *req.table)
          : SereneDBRelation(*req.table);
      if (view) {
        RequireView(*snapshot, role, *view);
      }
      continue;
    }

    auto table = BaseTable(*req.table);
    if (!table) {
      continue;  // system table from the vendored catalog: not ours to enforce.
    }
    auto current = snapshot->GetObject<catalog::Table>(table->GetId());
    const auto& t = current ? *current : *table;

    const auto del = ReadAcl(req.verb, catalog::AclMode::Delete) |
                     ReadAcl(req.verb, catalog::AclMode::Truncate);
    if (del != catalog::AclMode::NoRights) {
      snapshot->RequireAccess(role, t, del);
    }
    if (Has(req.verb, duckdb::AccessVerb::SELECT)) {
      // Skip the bare scan a DML makes over its own target (reads no value).
      const bool bare_dml_scan =
        req.read.empty() && write_targets.contains(t.GetId().id());
      if (!bare_dml_scan) {
        RequireColumns(*snapshot, role, t, catalog::AclMode::Select, req.read);
      }
    }
    if (Has(req.verb, duckdb::AccessVerb::UPDATE)) {
      RequireColumns(*snapshot, role, t, catalog::AclMode::Update, req.write);
    }
    if (Has(req.verb, duckdb::AccessVerb::INSERT)) {
      RequireColumns(*snapshot, role, t, catalog::AclMode::Insert, req.write);
    }
  }
}

}  // namespace

void RegisterRbacOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .pre_optimize_function = &CollectAndEnforce,
               });
}

}  // namespace sdb::optimizer
