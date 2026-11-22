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

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>

#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/dependency.h"

namespace sdb::catalog {
namespace {

bool TopLevel(duckdb::CatalogType type) {
  const auto container = duckdb::ObjectKindOf(type).container;
  return container == duckdb::ObjectContainer::DATABASE ||
         container == duckdb::ObjectContainer::CLUSTER;
}

bool TracksOwner(duckdb::CatalogType type) {
  const auto container = duckdb::ObjectKindOf(type).container;
  return container == duckdb::ObjectContainer::DATABASE ||
         container == duckdb::ObjectContainer::SCHEMA;
}

bool IsMacro(const duckdb::CatalogEntry& entry) {
  return entry.type == duckdb::CatalogType::MACRO_ENTRY ||
         entry.type == duckdb::CatalogType::TABLE_MACRO_ENTRY;
}

bool GrantsTo(std::span<const duckdb::AclItem> acl, duckdb::idx_t role) {
  return absl::c_any_of(
    acl, [&](const duckdb::AclItem& item) { return item.grantee == role; });
}

duckdb::QualifiedName OwnedEntryName(duckdb::CatalogTransaction transaction,
                                     duckdb::CatalogEntry& entry) {
  const auto& catalog = entry.ParentCatalog().GetName();
  if (TopLevel(entry.type)) {
    return duckdb::QualifiedName{catalog, duckdb::Identifier{}, entry.name};
  }
  return duckdb::QualifiedName::FromCatalogSchema(
    catalog, entry.ParentSchemaPath(transaction), entry.name);
}

struct OwnedAlter {
  duckdb::reference<duckdb::Catalog> catalog;
  duckdb::CatalogTransaction transaction;
  duckdb::unique_ptr<duckdb::AlterInfo> info;
};

duckdb::AlterPermissionsInfo& AddAlter(std::vector<OwnedAlter>& alters,
                                       duckdb::Catalog& catalog,
                                       duckdb::CatalogTransaction transaction,
                                       duckdb::CatalogEntry& entry) {
  const auto type = entry.type == duckdb::CatalogType::VIEW_ENTRY
                      ? duckdb::CatalogType::TABLE_ENTRY
                      : entry.type;
  alters.push_back({catalog, transaction,
                    duckdb::make_uniq<duckdb::AlterPermissionsInfo>(
                      type, OwnedEntryName(transaction, entry))});
  return alters.back().info->Cast<duckdb::AlterPermissionsInfo>();
}

duckdb::AlterPermissionsInfo& AddReassign(
  std::vector<OwnedAlter>& alters, duckdb::Catalog& catalog,
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& entry,
  const duckdb::ReassignOwnedInfo& info) {
  auto& alter = AddAlter(alters, catalog, transaction, entry);
  alter.new_owner = info.new_owner;
  alter.new_owner_id = info.new_owner_id;
  return alter;
}

duckdb::AlterPermissionsInfo& AddRevoke(std::vector<OwnedAlter>& alters,
                                        duckdb::Catalog& catalog,
                                        duckdb::CatalogTransaction transaction,
                                        duckdb::CatalogEntry& entry,
                                        duckdb::idx_t role,
                                        duckdb::CatalogType acl_class,
                                        const duckdb::DropOwnedInfo& info) {
  auto& alter = AddAlter(alters, catalog, transaction, entry);
  alter.privileges = duckdb::ObjectKindOf(acl_class).privileges;
  alter.revoke = true;
  alter.cascade = true;
  alter.grantee_id = role;
  alter.grantors = info.grantors;
  return alter;
}

void Apply(std::vector<OwnedAlter>& alters) {
  for (auto& alter : alters) {
    alter.catalog.get().Alter(alter.transaction, *alter.info);
  }
}

bool Reassigned(const duckdb::ReassignOwnedInfo& info, duckdb::idx_t owner) {
  return owner != info.new_owner_id &&
         absl::c_linear_search(info.role_ids, owner);
}

void ScanOwnable(SereneDBCatalog& catalog,
                 duckdb::CatalogTransaction transaction,
                 absl::FunctionRef<void(duckdb::CatalogEntry&)> visit) {
  catalog.ScanEntries(transaction, [&](duckdb::CatalogEntry& entry) {
    if (entry.internal || !TracksOwner(entry.type) ||
        (entry.type == duckdb::CatalogType::SEQUENCE_ENTRY &&
         OwnerOf(transaction, entry))) {
      return;
    }
    visit(entry);
  });
}

void ReassignShared(ClusterCatalog& cluster, duckdb::ClientContext& context,
                    const duckdb::ReassignOwnedInfo& info,
                    std::vector<OwnedAlter>& alters) {
  const auto transaction = cluster.GetCatalogTransaction(context);
  cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      if (!entry.internal && Reassigned(info, entry.permissions.owner)) {
        AddReassign(alters, cluster, transaction, entry, info);
      }
    });
}

void RevokeShared(ClusterCatalog& cluster, duckdb::ClientContext& context,
                  const duckdb::DropOwnedInfo& info,
                  duckdb::optional_ptr<const duckdb::Identifier> local,
                  std::vector<OwnedAlter>& alters) {
  const auto& config = duckdb::DBConfig::GetConfig(context);
  const auto bootstrap = config.bootstrap_role;
  const auto transaction = cluster.GetCatalogTransaction(context);
  cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      if (entry.internal) {
        return;
      }
      const auto& permissions = entry.permissions;
      const auto database =
        [&](duckdb::idx_t role,
            duckdb::CatalogType type) -> duckdb::AlterPermissionsInfo& {
        return AddRevoke(alters, cluster, transaction, entry, role, type, info);
      };
      for (const auto role : info.role_ids) {
        if (permissions.owner != role && GrantsTo(permissions.acl, role)) {
          database(role, duckdb::CatalogType::DATABASE_ENTRY);
        }
        if (!local || entry.name != *local) {
          continue;
        }
        for (const auto& row : permissions.Rows().rows) {
          if (!GrantsTo(row.acl, role)) {
            continue;
          }
          auto& alter = database(role, row.kind);
          alter.database_rows = {duckdb::GrantRow{row.kind, row.oid, {}}};
          alter.object_owner_id = bootstrap;
        }
        if (absl::c_any_of(permissions.defaults,
                           [&](const duckdb::DefaultAcl& row) {
                             return row.role == role || GrantsTo(row.acl, role);
                           })) {
          AddAlter(alters, cluster, transaction, entry).drop_owned_role_id =
            role;
        }
      }
    });
  cluster.Set(duckdb::CatalogType::ROLE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& member) {
      for (const auto& edge : member.Cast<RoleCatalogEntry>().MemberOf()) {
        if (edge.grantor == bootstrap ||
            !absl::c_linear_search(info.role_ids, edge.grantor)) {
          continue;
        }
        auto alter = duckdb::make_uniq<duckdb::AlterRoleInfo>(member.name);
        alter->grant_role_id = edge.role;
        alter->grantor_id = edge.grantor;
        alter->revoke = true;
        alters.push_back({cluster, transaction, std::move(alter)});
      }
    });
  cluster.Set(duckdb::CatalogType::PARAMETER_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& parameter) {
      for (const auto role : info.role_ids) {
        if (GrantsTo(parameter.permissions.acl, role)) {
          AddRevoke(alters, cluster, transaction, parameter, role,
                    duckdb::CatalogType::PARAMETER_ENTRY, info);
        }
      }
    });
}

}  // namespace

void SereneDBCatalog::ReassignOwned(duckdb::CatalogTransaction transaction,
                                    const duckdb::ReassignOwnedInfo& info) {
  std::vector<OwnedAlter> alters;
  ScanOwnable(*this, transaction, [&](duckdb::CatalogEntry& entry) {
    if (!IsMacro(entry)) {
      if (Reassigned(info, entry.permissions.owner)) {
        AddReassign(alters, *this, transaction, entry, info);
      }
      return;
    }
    const auto& macro = entry.Cast<duckdb::MacroCatalogEntry>();
    for (const auto& overload : macro.macros) {
      if (Reassigned(info, macro.OverloadPermissions(*overload).owner)) {
        AddReassign(alters, *this, transaction, entry, info).overload_oid =
          macro.OverloadOid(*overload);
      }
    }
  });
  auto& context = transaction.GetContext();
  ReassignShared(ClusterOf(context), context, info, alters);
  Apply(alters);
}

void ClusterCatalog::ReassignOwned(duckdb::CatalogTransaction transaction,
                                   const duckdb::ReassignOwnedInfo& info) {
  std::vector<OwnedAlter> alters;
  ReassignShared(*this, transaction.GetContext(), info, alters);
  Apply(alters);
}

void SereneDBCatalog::DropOwned(duckdb::CatalogTransaction transaction,
                                const duckdb::DropOwnedInfo& info) {
  const auto owns = [&](const duckdb::MacroCatalogEntry& macro,
                        const duckdb::MacroFunction& overload) {
    return absl::c_linear_search(info.role_ids,
                                 macro.OverloadPermissions(overload).owner);
  };
  const auto owned = [&](const duckdb::CatalogEntry& entry) {
    if (!IsMacro(entry)) {
      return absl::c_linear_search(info.role_ids, entry.permissions.owner);
    }
    const auto& macro = entry.Cast<duckdb::MacroCatalogEntry>();
    return absl::c_all_of(macro.macros, [&](const auto& overload) {
      return owns(macro, *overload);
    });
  };
  std::vector<OwnedAlter> alters;
  ScanOwnable(*this, transaction, [&](duckdb::CatalogEntry& entry) {
    if (owned(entry)) {
      return;
    }
    const auto acl_class = duckdb::ObjectKindOf(entry.type).acl_class;
    if (!IsMacro(entry)) {
      for (const auto role : info.role_ids) {
        if (duckdb::ReferencesRole(entry, role)) {
          AddRevoke(alters, *this, transaction, entry, role, acl_class, info);
        }
      }
      return;
    }
    const auto& macro = entry.Cast<duckdb::MacroCatalogEntry>();
    for (const auto& overload : macro.macros) {
      if (owns(macro, *overload)) {
        continue;
      }
      for (const auto role : info.role_ids) {
        if (GrantsTo(macro.OverloadPermissions(*overload).acl, role)) {
          AddRevoke(alters, *this, transaction, entry, role, acl_class, info)
            .overload_oid = macro.OverloadOid(*overload);
        }
      }
    }
  });
  auto& context = transaction.GetContext();
  RevokeShared(ClusterOf(context), context, info, &GetName(), alters);
  Apply(alters);

  duckdb::catalog_entry_vector_t dropped;
  std::vector<duckdb::DropInfo> overload_drops;
  ScanOwnable(*this, transaction, [&](duckdb::CatalogEntry& entry) {
    if (owned(entry)) {
      if (!duckdb::ObjectKindOf(entry.type).shared) {
        dropped.push_back(entry);
      }
      return;
    }
    if (!IsMacro(entry)) {
      return;
    }
    const auto& macro = entry.Cast<duckdb::MacroCatalogEntry>();
    for (const auto& overload : macro.macros) {
      if (!owns(macro, *overload)) {
        continue;
      }
      duckdb::DropInfo drop;
      drop.type = entry.type;
      drop.SetQualifiedName(OwnedEntryName(transaction, entry));
      drop.func_parameters = overload->ParameterTypes();
      drop.has_func_args = true;
      drop.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
      overload_drops.push_back(std::move(drop));
    }
  });
  if (!info.cascade) {
    RefuseDroppingTogether(transaction, dropped);
  }
  std::vector<duckdb::DropInfo> drops;
  for (auto& entry : DropOrder(transaction, dropped)) {
    auto& drop = drops.emplace_back();
    drop.type = entry.get().type;
    drop.SetQualifiedName(OwnedEntryName(transaction, entry));
    drop.cascade = true;
    drop.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
  }
  for (auto& drop : drops) {
    DropEntry(context, drop);
  }
  for (auto& drop : overload_drops) {
    DropEntry(context, drop);
  }
}

void ClusterCatalog::DropOwned(duckdb::CatalogTransaction transaction,
                               const duckdb::DropOwnedInfo& info) {
  std::vector<OwnedAlter> alters;
  RevokeShared(*this, transaction.GetContext(), info, nullptr, alters);
  Apply(alters);
}

}  // namespace sdb::catalog
