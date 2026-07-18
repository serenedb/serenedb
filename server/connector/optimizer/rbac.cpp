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
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/binder.hpp>
#include <memory>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/object.h"
#include "catalog/store/store.h"
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
  const auto schema = entry.ParentSchema().name.GetIdentifierName();
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

bool IsStoreEntry(const duckdb::CatalogEntry& entry) {
  return entry.ParentCatalog().GetName().GetIdentifierName() ==
         catalog::kStoreDatabaseName;
}

void RequireColumns(const auth::RoleClosure& closure,
                    const catalog::Table& table, catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
  const bool ok = logical.empty()
                    ? closure.CanAnyColumn(table, need)
                    : closure.CanColumns(
                        table, need, [&](uint64_t i, const catalog::Column&) {
                          return logical.contains(i);
                        });
  if (ok) {
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

void CollectAndEnforce(duckdb::ClientContext& context, duckdb::Binder& binder) {
  auto state = context.registered_state->Get<connector::SereneDBClientState>(
    connector::kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.CatalogSnapshot();
  const auto caller = ctx.GetRoleId();

  const auto& properties = binder.GetStatementProperties();
  const auto& reqs = properties.access_requirements;

  // Foreign-server USAGE: a foreign-catalog relation's parent catalog is the
  // attach alias, i.e. the foreign server's name. The querying role needs
  // USAGE (or ownership) on that server (PG-style); the remote enforces the
  // rest. The attachment is instance-global, so resolve the server across ALL
  // databases (names are globally unique), never scoped to the caller's own
  // database -- a server created in one database must still be gated when
  // referenced from a session in another (else the check silently skips ->
  // cross-database bypass). Checked once per distinct catalog alias, on the
  // caller's own role. A null resolve means the catalog is a regular database,
  // a system/temp catalog, or a raw ATTACH foreign catalog with no CREATE
  // SERVER object -- none are governed by foreign-server USAGE.
  containers::FlatHashSet<std::string> checked_catalogs;
  for (const auto& req : reqs) {
    if (!req.table) {
      continue;
    }
    const auto catalog_name =
      req.table->ParentCatalog().GetName().GetIdentifierName();
    if (!checked_catalogs.insert(catalog_name).second) {
      continue;
    }
    auto server = snapshot->GetForeignServerGlobal(catalog_name);
    if (!server) {
      continue;
    }
    const auto& closure = snapshot->ClosureFor(caller);
    if (!closure.Owns(*server) &&
        !closure.Can(*server, catalog::AclMode::Usage)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for foreign server ", server->GetName()));
    }
  }

  std::vector<const catalog::Object*> objects;
  objects.reserve(reqs.size());
  bool store_orphans = false;
  for (const auto& req : reqs) {
    const catalog::Object* obj =
      req.table ? SereneDBRelation(req.table) : nullptr;
    objects.push_back(obj);
    store_orphans |= !obj && req.table && IsStoreEntry(*req.table);
  }

  // DML records the plan's scan table -- the store-side entry, not the user
  // facade. Map store entries back through the facades bound alongside them
  // (forward-composed store name; never parsed).
  if (store_orphans) {
    containers::FlatHashMap<std::string, const catalog::Object*> facades;
    for (size_t i = 0; i < reqs.size(); ++i) {
      const auto* entry = reqs[i].table;
      if (!objects[i] || !entry ||
          !dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
        continue;
      }
      facades.emplace(catalog::StoreTableName(
                        entry->ParentCatalog().GetName().GetIdentifierName(),
                        entry->ParentSchema().name.GetIdentifierName(),
                        entry->name.GetIdentifierName()),
                      objects[i]);
    }
    for (size_t i = 0; i < reqs.size(); ++i) {
      if (objects[i] || !reqs[i].table || !IsStoreEntry(*reqs[i].table)) {
        continue;
      }
      const auto it = facades.find(reqs[i].table->name.GetIdentifierName());
      if (it != facades.end()) {
        objects[i] = it->second;
      }
    }
  }

  containers::FlatHashSet<uint64_t> write_targets;
  for (size_t i = 0; i < reqs.size(); ++i) {
    const catalog::Object* obj = objects[i];
    if (obj && obj->GetType() == catalog::ObjectType::Table &&
        Has(reqs[i].verb,
            duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
              duckdb::AccessVerb::DELETE | duckdb::AccessVerb::TRUNCATE)) {
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
    const auto& closure = snapshot->ClosureFor(role);

    // System relations (and views) are read-only from the caller's side: a
    // single SELECT check on the object's ACL, no columns or DML.
    if (req.table && IsSystemSchema(*req.table)) {
      if (!closure.Can(*obj, catalog::AclMode::Select)) {
        const char* kind =
          req.table->type == duckdb::CatalogType::VIEW_ENTRY ? "view" : "table";
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for ", kind, " ",
                                req.table->name.GetIdentifierName()));
      }
      continue;
    }

    if (obj->GetType() == catalog::ObjectType::PgSqlView) {
      if (!closure.Can(*obj, catalog::AclMode::Select)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for view ", obj->GetName()));
      }
      continue;
    }

    const auto& t = basics::downCast<catalog::Table>(*obj);

    const auto del = AsAclMode(req.verb) &
                     (catalog::AclMode::Delete | catalog::AclMode::Truncate);
    if (del != catalog::AclMode::NoRights && !closure.Can(t, del)) {
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

void RegisterRbacAccessCheck(duckdb::DatabaseInstance& db) {
  db.config.access_check_function = &CollectAndEnforce;
}

}  // namespace sdb::optimizer
