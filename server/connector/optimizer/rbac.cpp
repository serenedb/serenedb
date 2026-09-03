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
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
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
#include "catalog1/catalog.h"
#include "catalog1/entry/foreign_server.h"
#include "catalog1/permissions.h"
#include "connector/duckdb_client_state.h"
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
  duckdb::idx_t id;
  std::string_view name;
  const catalog::Permissions* perm = nullptr;
  // The entry a column check reads: its ColumnList spells the columns the
  // plan's indices name, and the relation's per-column grants hang off the
  // same ids. Not the entry the requirement points at -- a DML scans the store
  // table, whose columns carry no identity -- but the facade it was matched
  // to.
  const duckdb::TableCatalogEntry* relation = nullptr;
  const catalog::ColumnAcls* acls = nullptr;

  explicit operator bool() const noexcept { return perm != nullptr; }
};

Governed SereneDBRelation(const duckdb::CatalogEntry* entry,
                          duckdb::ClientContext& context) {
  if (const auto* facade =
        dynamic_cast<const duckdb::TableCatalogEntry*>(entry)) {
    return {duckdb::CatalogType::TABLE_ENTRY,
            facade->oid,
            facade->name.GetIdentifierName(),
            &facade->permissions,
            facade,
            &facade->GetColumnAcls()};
  }
  if (const auto* view = dynamic_cast<const duckdb::ViewCatalogEntry*>(entry)) {
    return {duckdb::CatalogType::VIEW_ENTRY,
            view->oid,
            view->name.GetIdentifierName(),
            &view->permissions,
            nullptr,
            nullptr};
  }
  if (const auto* index =
        dynamic_cast<const catalog::SereneDBIndexScanEntry*>(entry)) {
    // Reading an index is gated on the relation it is built on, so that is
    // what a denial names -- resolved live by the id the wrapper holds: an
    // index has no ACL of its own, and a regrant or rename of the relation
    // does not rewrite the wrapper. The columns stay the wrapper's, which is
    // the ColumnList the plan's indices name.
    auto governed = SereneDBRelation(
      catalog::LookupEntryIn(
        &context, const_cast<duckdb::Catalog&>(index->ParentCatalog()),
        index->GetIndexedRelationId())
        .get(),
      context);
    governed.relation = index;
    return governed;
  }
  if (const auto* system =
        dynamic_cast<const catalog::SystemTableEntry*>(entry)) {
    return {duckdb::CatalogType::TABLE_ENTRY,
            (*system).oid,
            system->name.GetIdentifierName(),
            &system->permissions,
            system,
            nullptr};
  }
  return {};
}

// The grants of the columns the plan named, in the entry's own column order.
// `logical` indexes that order, and the ColumnList it indexes already leaves
// out the hidden generated primary key -- so a plan can never name it and no
// filtering is owed here. An empty `logical` means "every column", which is the
// bare-read case CanAnyColumn answers.
std::vector<catalog::AclView> SelectedColumnAcls(
  const Governed& governed, const duckdb::unordered_set<uint64_t>& logical) {
  const auto* acls_by_column = governed.acls;
  const auto& columns = governed.relation->GetColumns();
  std::vector<catalog::AclView> acls;
  acls.reserve(logical.empty() ? columns.LogicalColumnCount() : logical.size());
  uint64_t i = 0;
  for (const auto& column : columns.Logical()) {
    if (logical.empty() || logical.contains(i)) {
      acls.push_back(catalog::ColumnAclOf(acls_by_column, column.Oid()));
    }
    ++i;
  }
  return acls;
}

void RequireColumns(const auth::RoleClosure& closure, const Governed& governed,
                    catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
  const auto acls = SelectedColumnAcls(governed, logical);
  const bool ok = logical.empty()
                    ? closure.CanAnyColumn(*governed.perm, need, acls)
                    : closure.CanColumns(*governed.perm, need, acls);
  if (ok) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", governed.name));
}

duckdb::idx_t EffectiveRole(duckdb::idx_t caller,
                            const duckdb::CatalogEntry* who,
                            duckdb::ClientContext& context) {
  const auto view = SereneDBRelation(who, context);
  return view ? view.perm->owner : caller;
}

using AccessRequirements = duckdb::vector<duckdb::AccessRequirement>;

// A foreign catalog is attached under its foreign server's name, so relations
// behind it need ownership or USAGE on that server. Local catalogs are rejected
// by IS_REMOTE before any lookup; a remote one with no server object is a raw
// ATTACH and ungoverned. The resolve is instance-wide on purpose -- the
// attachment is too, so a session in another database must not slip past -- and
// it walks every attached catalog's set, hence the dedup per alias.
void RequireForeignServerUsage(duckdb::ClientContext& context,
                               duckdb::idx_t caller,
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
    const auto* server = catalog::FindForeignServerAnywhere(&context, catalog);
    if (server == nullptr) {
      continue;
    }
    const auto& perm = server->permissions;
    const auto closure = auth::ClosureFor(&context, caller);
    if (!closure->Owns(perm.owner) &&
        !closure->Can(duckdb::CatalogType::FOREIGN_SERVER_ENTRY, perm,
                      catalog::AclMode::Usage)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for foreign server ", server->GetName()));
    }
  }
}

// The catalog object each requirement grants against, indexed like `reqs`.
std::vector<Governed> CollectRelations(const AccessRequirements& reqs,
                                       duckdb::ClientContext& context) {
  std::vector<Governed> objects(reqs.size());
  for (size_t i = 0; i < reqs.size(); ++i) {
    if (const auto* entry = reqs[i].table) {
      objects[i] = SereneDBRelation(entry, context);
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
      targets.insert(objects[i].id);
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

  const auto objects = CollectRelations(reqs, ctx.GetClientContext());
  const auto write_targets = CollectWriteTargets(reqs, objects);

  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto& req = reqs[i];
    const auto& governed = objects[i];
    if (!governed) {
      continue;
    }
    const auto& perm = *governed.perm;
    const auto type = governed.type;

    const duckdb::idx_t role =
      EffectiveRole(caller, req.who, ctx.GetClientContext());
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
        req.read.empty() && write_targets.contains(governed.id);
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
