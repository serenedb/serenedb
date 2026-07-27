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
#include <utility>
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

catalog::AclMode AsAclMode(duckdb::AccessVerb verb) {
  static constexpr std::pair<duckdb::AccessVerb, catalog::AclMode> kVerbAcl[]{
    {duckdb::AccessVerb::INSERT, catalog::AclMode::Insert},
    {duckdb::AccessVerb::SELECT, catalog::AclMode::Select},
    {duckdb::AccessVerb::UPDATE, catalog::AclMode::Update},
    {duckdb::AccessVerb::DELETE, catalog::AclMode::Delete},
    {duckdb::AccessVerb::TRUNCATE, catalog::AclMode::Truncate},
  };
  catalog::AclMode mode = catalog::AclMode::NoRights;
  for (const auto& [bit, acl] : kVerbAcl) {
    if (Has(verb, bit)) {
      mode |= acl;
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

using AccessRequirements = duckdb::vector<duckdb::AccessRequirement>;

// A foreign catalog is attached under its foreign server's name, so relations
// behind it need ownership or USAGE on that server. Local catalogs are rejected
// by IS_REMOTE before any lookup; a remote one with no server object is a raw
// ATTACH and ungoverned. The resolve is instance-wide on purpose -- the
// attachment is too, so a session in another database must not slip past -- and
// it walks every database, hence the dedup per alias.
void RequireForeignServerUsage(const catalog::Snapshot& snapshot,
                               ObjectId caller,
                               const AccessRequirements& reqs) {
  containers::FlatHashSet<std::string_view> checked;
  for (const auto& req : reqs) {
    if (!req.table) {
      continue;
    }
    const auto& parent = req.table->ParentCatalog();
    if (!parent.Supports(duckdb::RemoteCapability::IS_REMOTE)) {
      continue;
    }
    const std::string_view catalog = parent.GetName().GetIdentifierName();
    if (!checked.insert(catalog).second) {
      continue;
    }
    const auto server = snapshot.GetForeignServer(catalog);
    if (!server) {
      continue;
    }
    const auto& closure = snapshot.ClosureFor(caller);
    if (!closure.Owns(*server) &&
        !closure.Can(*server, catalog::AclMode::Usage)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for foreign server ", server->GetName()));
    }
  }
}

// The store-side name of a table facade, composed exactly as the store composes
// it. Never the reverse: a store name is not split back into its parts.
std::string StoreNameOf(const duckdb::CatalogEntry& facade) {
  return catalog::StoreTableName(
    facade.ParentCatalog().GetName().GetIdentifierName(),
    facade.ParentSchema().name.GetIdentifierName(),
    facade.name.GetIdentifierName());
}

// The catalog object each requirement grants against, indexed like `reqs`.
//
// A DML plan scans the store-side table, whose entry is a plain DuckTableEntry
// that SereneDBRelation cannot resolve -- so it lands here with no object and
// its privileges would go unchecked. Every such orphan is matched to the table
// facade bound alongside it, whose store name it carries verbatim.
std::vector<const catalog::Object*> CollectRelations(
  const AccessRequirements& reqs) {
  std::vector<const catalog::Object*> objects(reqs.size(), nullptr);
  std::vector<size_t> facades;
  std::vector<size_t> orphans;
  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto* entry = reqs[i].table;
    if (!entry) {
      continue;
    }
    objects[i] = SereneDBRelation(entry);
    if (!objects[i]) {
      if (IsStoreEntry(*entry)) {
        orphans.push_back(i);
      }
    } else if (dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
      facades.push_back(i);
    }
  }
  // Statements that touch no store table -- everything but DML -- stop here,
  // having composed no names.
  if (orphans.empty()) {
    return objects;
  }
  containers::FlatHashMap<std::string, const catalog::Object*> by_store_name;
  by_store_name.reserve(facades.size());
  for (const size_t i : facades) {
    by_store_name.emplace(StoreNameOf(*reqs[i].table), objects[i]);
  }
  for (const size_t i : orphans) {
    const auto it = by_store_name.find(reqs[i].table->name.GetIdentifierName());
    if (it != by_store_name.end()) {
      objects[i] = it->second;
    }
  }
  return objects;
}

containers::FlatHashSet<uint64_t> CollectWriteTargets(
  const AccessRequirements& reqs,
  const std::vector<const catalog::Object*>& objects) {
  containers::FlatHashSet<uint64_t> targets;
  for (size_t i = 0; i < reqs.size(); ++i) {
    const catalog::Object* obj = objects[i];
    if (obj && obj->GetType() == catalog::ObjectType::Table &&
        Has(reqs[i].verb,
            duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
              duckdb::AccessVerb::DELETE | duckdb::AccessVerb::TRUNCATE)) {
      targets.insert(obj->GetId().id());
    }
  }
  return targets;
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

  RequireForeignServerUsage(*snapshot, caller, reqs);

  const auto objects = CollectRelations(reqs);
  const auto write_targets = CollectWriteTargets(reqs, objects);

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

    if (obj->GetType() == catalog::ObjectType::View) {
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
