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

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/store/store.h"
#include "connector/duckdb_catalog_sets.h"
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

// What a bound relation entry contributes to an access check: the owner and ACL
// it carries, the per-column grants it answers with where the check goes
// per-column, and the definition behind it for the name an error reports and
// the kind the check branches on.
//
// A relation the check governs but that has no ACL of its own -- an index --
// answers with the permissions of the relation it hangs off, which is postgres'
// rule, and with that relation's column grants for the same reason.
struct Governed {
  // The identity the check reports and branches on, spelled out rather than
  // reached through the definition: every kind's entry carries its own.
  duckdb::CatalogType type = duckdb::CatalogType::INVALID;
  ObjectId id;
  std::string_view name;
  const catalog::Permissions* perm = nullptr;
  // The entry a column check reads: its ColumnList spells the columns the
  // plan's indices name, and its per-column grants hang off the same ids. Not
  // the entry the requirement points at -- a DML scans the store table, whose
  // columns carry no identity -- but the facade it was matched to.
  const duckdb::TableCatalogEntry* relation = nullptr;

  explicit operator bool() const noexcept { return perm != nullptr; }
};

Governed SereneDBRelation(const duckdb::CatalogEntry* entry) {
  if (const auto* facade =
        dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
    const auto& table = facade->Table();
    return {duckdb::CatalogType::TABLE_ENTRY, table.GetId(), table.GetName(),
            &facade->permissions, facade};
  }
  if (const auto* view =
        dynamic_cast<const connector::SereneDBViewEntry*>(entry)) {
    return {duckdb::CatalogType::VIEW_ENTRY, ObjectId{view->oid},
            view->name.GetIdentifierName(), &view->permissions, nullptr};
  }
  if (const auto* index =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(entry)) {
    // Reading an index is gated on the relation it is built on, so that is what
    // a denial names -- the entry carries its identity either way.
    return {index->GetIndexedRelationType(), index->GetIndexedRelationId(),
            index->GetIndexedRelationName(),
            &index->GetIndexedRelationPermissions(), index};
  }
  if (const auto* system =
        dynamic_cast<const connector::SystemTableEntry*>(entry)) {
    return {duckdb::CatalogType::TABLE_ENTRY, catalog::IdOf(*system),
            system->name.GetIdentifierName(), &system->permissions, system};
  }
  return {};
}

// The grants of the columns the plan named, in the entry's own column order.
// `logical` indexes that order, and the ColumnList it indexes already leaves
// out the hidden generated primary key -- so a plan can never name it and no
// filtering is owed here. An empty `logical` means "every column", which is the
// bare-read case CanAnyColumn answers.
std::vector<catalog::AclView> SelectedColumnAcls(
  const duckdb::TableCatalogEntry& entry,
  const duckdb::unordered_set<uint64_t>& logical) {
  // Resolved once, not per column: the grants hang off whichever entry the
  // requirement was matched to, and almost every table has none at all.
  const auto* acls_by_column = connector::RelationColumnAcls(entry);
  const auto& columns = entry.GetColumns();
  std::vector<catalog::AclView> acls;
  acls.reserve(logical.empty() ? columns.LogicalColumnCount() : logical.size());
  uint64_t i = 0;
  for (const auto& column : columns.Logical()) {
    if (logical.empty() || logical.contains(i)) {
      acls.push_back(
        catalog::ColumnAclOf(acls_by_column, ObjectId{column.CatalogOid()}));
    }
    ++i;
  }
  return acls;
}

void RequireColumns(const auth::RoleClosure& closure, const Governed& governed,
                    catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
  const auto acls = SelectedColumnAcls(*governed.relation, logical);
  const bool ok = logical.empty()
                    ? closure.CanAnyColumn(*governed.perm, need, acls)
                    : closure.CanColumns(*governed.perm, need, acls);
  if (ok) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", governed.name));
}

ObjectId EffectiveRole(ObjectId caller, const duckdb::CatalogEntry* who) {
  const auto view = SereneDBRelation(who);
  return view ? catalog::OwnerOf(*view.perm) : caller;
}

using AccessRequirements = duckdb::vector<duckdb::AccessRequirement>;

// A foreign catalog is attached under its foreign server's name, so relations
// behind it need ownership or USAGE on that server. Local catalogs are rejected
// by IS_REMOTE before any lookup; a remote one with no server object is a raw
// ATTACH and ungoverned. The resolve is instance-wide on purpose -- the
// attachment is too, so a session in another database must not slip past -- and
// it walks every attached catalog's set, hence the dedup per alias.
void RequireForeignServerUsage(duckdb::ClientContext& context, ObjectId caller,
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
    catalog::Permissions perm;
    const auto server =
      connector::FindForeignServerAnywhere(&context, catalog, &perm);
    if (!server) {
      continue;
    }
    const auto closure = auth::ClosureFor(&context, caller);
    if (!closure->Owns(catalog::OwnerOf(perm)) &&
        !closure->Can(duckdb::CatalogType::FOREIGN_SERVER_ENTRY, perm,
                      catalog::AclMode::Usage)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for foreign server ", server->GetName()));
    }
  }
}

// The catalog object each requirement grants against, indexed like `reqs`.
std::vector<Governed> CollectRelations(const AccessRequirements& reqs) {
  std::vector<Governed> objects(reqs.size());
  for (size_t i = 0; i < reqs.size(); ++i) {
    if (const auto* entry = reqs[i].table) {
      objects[i] = SereneDBRelation(entry);
    }
  }
  return objects;
}

containers::FlatHashSet<uint64_t> CollectWriteTargets(
  const AccessRequirements& reqs, const std::vector<Governed>& objects) {
  containers::FlatHashSet<uint64_t> targets;
  for (size_t i = 0; i < reqs.size(); ++i) {
    if (objects[i].type == duckdb::CatalogType::TABLE_ENTRY &&
        Has(reqs[i].verb,
            duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
              duckdb::AccessVerb::DELETE | duckdb::AccessVerb::TRUNCATE)) {
      targets.insert(objects[i].id.id());
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
  // The data store's own index builds are the engine's work, not a role's.
  if (ctx.IsStorageConnection()) {
    return;
  }
  const auto caller = ctx.GetRoleId();

  const auto& properties = binder.GetStatementProperties();
  const auto& reqs = properties.access_requirements;

  RequireForeignServerUsage(ctx.GetClientContext(), caller, reqs);

  const auto objects = CollectRelations(reqs);
  const auto write_targets = CollectWriteTargets(reqs, objects);

  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto& req = reqs[i];
    const auto& governed = objects[i];
    if (!governed) {
      continue;
    }
    const auto& perm = *governed.perm;
    const auto type = governed.type;

    const ObjectId role = EffectiveRole(caller, req.who);
    const auto closure_ptr = auth::ClosureFor(&ctx.GetClientContext(), role);
    const auto& closure = *closure_ptr;

    // System relations (and views) are read-only from the caller's side: a
    // single SELECT check on the object's ACL, no columns or DML.
    if (req.table && IsSystemSchema(*req.table)) {
      if (!closure.Can(type, perm, catalog::AclMode::Select)) {
        const char* kind =
          req.table->type == duckdb::CatalogType::VIEW_ENTRY ? "view" : "table";
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for ", kind, " ",
                                req.table->name.GetIdentifierName()));
      }
      continue;
    }

    if (type == duckdb::CatalogType::VIEW_ENTRY) {
      if (!closure.Can(type, perm, catalog::AclMode::Select)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for view ", governed.name));
      }
      continue;
    }

    const auto del = AsAclMode(req.verb) &
                     (catalog::AclMode::Delete | catalog::AclMode::Truncate);
    if (del != catalog::AclMode::NoRights && !closure.Can(type, perm, del)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for table ", governed.name));
    }
    if (Has(req.verb, duckdb::AccessVerb::SELECT)) {
      // A DML's own-target scan reads no column, so needs no SELECT (PG);
      // count(*) also has an empty read set but is not a write target.
      const bool bare_dml_scan =
        req.read.empty() && write_targets.contains(governed.id.id());
      if (!bare_dml_scan) {
        RequireColumns(closure, objects[i], catalog::AclMode::Select, req.read);
      }
    }
    if (Has(req.verb, duckdb::AccessVerb::UPDATE)) {
      RequireColumns(closure, objects[i], catalog::AclMode::Update, req.write);
    }
    if (Has(req.verb, duckdb::AccessVerb::INSERT)) {
      RequireColumns(closure, objects[i], catalog::AclMode::Insert, req.write);
    }
  }
}

}  // namespace

void RegisterRbacAccessCheck(duckdb::DatabaseInstance& db) {
  db.config.access_check_function = &CollectAndEnforce;
}

}  // namespace sdb::optimizer
