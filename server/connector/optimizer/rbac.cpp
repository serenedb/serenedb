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
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_system_table_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_view_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

bool Has(duckdb::AccessVerb verb, duckdb::AccessVerb bit) {
  return (static_cast<uint8_t>(verb) & static_cast<uint8_t>(bit)) != 0;
}

catalog::AclMode AclModeOf(duckdb::AccessVerb bit) {
  switch (bit) {
    case duckdb::AccessVerb::INSERT:
      return catalog::AclMode::Insert;
    case duckdb::AccessVerb::SELECT:
      return catalog::AclMode::Select;
    case duckdb::AccessVerb::UPDATE:
      return catalog::AclMode::Update;
    case duckdb::AccessVerb::DELETE:
      return catalog::AclMode::Delete;
    case duckdb::AccessVerb::TRUNCATE:
      return catalog::AclMode::Truncate;
    default:
      return catalog::AclMode::NoRights;
  }
}

catalog::AclMode AsAclMode(duckdb::AccessVerb verb) {
  catalog::AclMode mode = catalog::AclMode::NoRights;
  for (auto bit : {duckdb::AccessVerb::INSERT, duckdb::AccessVerb::SELECT,
                   duckdb::AccessVerb::UPDATE, duckdb::AccessVerb::DELETE,
                   duckdb::AccessVerb::TRUNCATE}) {
    if (Has(verb, bit)) {
      mode |= AclModeOf(bit);
    }
  }
  return mode;
}

bool IsSystemSchema(const duckdb::CatalogEntry& entry) {
  const auto& schema = entry.ParentSchema().name;
  return schema == StaticStrings::kPgCatalogSchema ||
         schema == StaticStrings::kInformationSchema;
}

const catalog::Object* SereneDBRelation(const duckdb::CatalogEntry* entry) {
  if (const auto* facade =
        dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
    return facade->GetSereneDBTable().get();
  }
  if (const auto* view =
        dynamic_cast<const connector::SereneDBViewEntry*>(entry)) {
    return view->GetSereneDBView().get();
  }
  if (const auto* index =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(entry)) {
    return index->GetIndexedRelation();
  }
  if (const auto* system =
        dynamic_cast<const connector::SystemTableEntry*>(entry)) {
    return &system->GetSystemObject();
  }
  return nullptr;
}

void RequireColumns(const auth::RoleClosure& closure,
                    const catalog::Table& table, catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
  if (auth::HasColumnPrivilege(
        closure, table, need, !logical.empty(),
        [&](uint64_t i) { return logical.contains(i); })) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", table.GetName()));
}

ObjectId EffectiveRole(ObjectId caller, const duckdb::CatalogEntry* who) {
  if (const auto* view = SereneDBRelation(who)) {
    return view->GetOwner();
  }
  return caller;
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

  const auto& properties = input.optimizer.binder.GetStatementProperties();
  const auto& reqs = properties.access_requirements;

  std::vector<const catalog::Object*> objects;
  objects.reserve(reqs.size());
  containers::FlatHashSet<uint64_t> write_targets;
  for (const auto& req : reqs) {
    const catalog::Object* obj =
      req.table ? SereneDBRelation(req.table) : nullptr;
    objects.push_back(obj);
    if (obj && obj->GetType() == catalog::ObjectType::Table &&
        Has(req.verb, duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
                        duckdb::AccessVerb::DELETE |
                        duckdb::AccessVerb::TRUNCATE)) {
      write_targets.insert(obj->GetId().id());
    }
  }

  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto& req = reqs[i];
    const catalog::Object* obj = objects[i];

    if (!obj) {
      continue;
    }

    const ObjectId role = EffectiveRole(caller, req.who);
    const auto& closure = snapshot->EffectiveRoleClosure(role);

    // System relations (and views) are read-only from the caller's side: a
    // single SELECT check on the object's ACL, no columns or DML.
    if (req.table && IsSystemSchema(*req.table)) {
      if (!auth::HasPrivilege(closure, *obj, catalog::AclMode::Select,
                              auth::PrivMatch::All)) {
        const char* kind =
          req.table->type == duckdb::CatalogType::VIEW_ENTRY ? "view" : "table";
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
          ERR_MSG("permission denied for ", kind, " ", req.table->name));
      }
      continue;
    }

    if (obj->GetType() == catalog::ObjectType::PgSqlView) {
      if (!auth::HasPrivilege(closure, *obj, catalog::AclMode::Select,
                              auth::PrivMatch::All)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for view ", obj->GetName()));
      }
      continue;
    }

    const auto& t = basics::downCast<catalog::Table>(*obj);

    const auto del = AsAclMode(req.verb) &
                     (catalog::AclMode::Delete | catalog::AclMode::Truncate);
    if (del != catalog::AclMode::NoRights &&
        !auth::HasPrivilege(closure, t, del, auth::PrivMatch::Any)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for table ", t.GetName()));
    }
    if (Has(req.verb, duckdb::AccessVerb::SELECT)) {
      // A DML's own-target scan reads no column, so needs no SELECT (PG);
      // count(*) also has an empty read set but is not a write target.
      const bool bare_dml_scan =
        req.read.empty() && write_targets.contains(t.GetId().id());
      if (!bare_dml_scan) {
        RequireColumns(closure, t, catalog::AclMode::Select, req.read);
      }
    }
    if (Has(req.verb, duckdb::AccessVerb::UPDATE)) {
      RequireColumns(closure, t, catalog::AclMode::Update, req.write);
    }
    if (Has(req.verb, duckdb::AccessVerb::INSERT)) {
      RequireColumns(closure, t, catalog::AclMode::Insert, req.write);
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
