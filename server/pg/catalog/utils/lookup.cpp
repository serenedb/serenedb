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

#include "pg/catalog/utils/lookup.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/replication_lsn_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <vector>

#include "auth/enforce.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/subscription.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/utils/columns.h"

namespace sdb::pg {

duckdb::idx_t CatalogClassOid(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  switch (type) {
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
      return kPgClassTable;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return kPgProcTable;
    case TYPE_ENTRY:
      return kPgTypeTable;
    case SCHEMA_ENTRY:
      return kPgNamespaceTable;
    case DATABASE_ENTRY:
      return kPgDatabaseTable;
    case ROLE_ENTRY:
      return kPgAuthidTable;
    case FOREIGN_SERVER_ENTRY:
      return kPgForeignServerTable;
    case TOKENIZER_ENTRY:
      return kPgTsDictTable;
    case SUBSCRIPTION_ENTRY:
      return kPgSubscriptionTable;
    default:
      return kInvalidOid;
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntryByOid(const Session& session,
                                                      uint64_t oid) {
  if (!session.database || oid == kInvalidOid) {
    return nullptr;
  }
  auto entry =
    session.database->GetOidIndex().GetVisible(oid, session.transaction->view);
  if (!entry || entry->internal) {
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::CatalogEntry> TemporaryEntryByOid(
  const Session& session, uint64_t oid) {
  if (!session.context || oid == kInvalidOid) {
    return nullptr;
  }
  auto& temporary = duckdb::ClientData::Get(*session.context)
                      .temporary_objects->GetCatalog()
                      .Cast<duckdb::DuckCatalog>();
  auto entry = temporary.GetOidIndex().GetVisible(
    oid, temporary.GetCatalogTransaction(*session.context).view);
  if (!entry || entry->internal) {
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SchemaByOid(
  const Session& session, uint64_t oid) {
  if (!session.database) {
    return nullptr;
  }
  auto entry =
    session.database->GetOidIndex().GetVisible(oid, session.transaction->view);
  if (!entry || entry->type != duckdb::CatalogType::SCHEMA_ENTRY) {
    return nullptr;
  }
  return entry->Cast<duckdb::SchemaCatalogEntry>();
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> OwnerTable(
  const Session& session, uint64_t oid) {
  if (!session.database || oid == kInvalidOid) {
    return nullptr;
  }
  auto owner = session.database->GetOidIndex().GetVisibleOwner(
    oid, session.transaction->view);
  if (!owner || owner->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return owner->Cast<duckdb::TableCatalogEntry>();
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabase(
  duckdb::ClientContext& context, std::string_view name) {
  auto& cluster = catalog::ClusterOf(context);
  return cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .GetEntry(cluster.GetCatalogTransaction(context), duckdb::Identifier{name});
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabaseById(
  duckdb::ClientContext& context, duckdb::idx_t id) {
  auto& cluster = catalog::ClusterOf(context);
  const auto entry = cluster.GetOidIndex().GetVisible(
    id, cluster.GetCatalogTransaction(context).view);
  if (!entry || entry->type != duckdb::CatalogType::DATABASE_ENTRY) {
    return nullptr;
  }
  return entry;
}

std::vector<duckdb::reference<duckdb::CatalogEntry>> Databases(
  duckdb::ClientContext& context) {
  auto& cluster = catalog::ClusterOf(context);
  std::vector<duckdb::reference<duckdb::CatalogEntry>> databases;
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.GetCatalogTransaction(context),
          [&](duckdb::CatalogEntry& entry) { databases.emplace_back(entry); });
  return databases;
}

std::optional<Relation> FindRelation(const Session& session, uint64_t oid) {
  if (const auto entry = EntryByOid(session, oid)) {
    if (CatalogClassOid(entry->type) != kPgClassTable &&
        (entry->type != duckdb::CatalogType::TYPE_ENTRY ||
         !duckdb::StructType::IsStruct(
           entry->Cast<duckdb::TypeCatalogEntry>().user_type))) {
      return std::nullopt;
    }
    return Relation{entry, nullptr, nullptr, nullptr};
  }
  if (const auto* system = FindSystemTable(oid)) {
    return Relation{nullptr, nullptr, system, nullptr};
  }
  if (const auto* view = FindSystemView(oid)) {
    return Relation{nullptr, nullptr, nullptr, view};
  }
  if (const auto key = FindKeyIndex(session, oid); key.table) {
    return Relation{key.table, key.constraint, nullptr, nullptr};
  }
  if (const auto entry = TemporaryEntryByOid(session, oid);
      entry && CatalogClassOid(entry->type) == kPgClassTable) {
    return Relation{entry, nullptr, nullptr, nullptr};
  }
  return std::nullopt;
}

std::optional<Relation> RowTypeOwner(const Session& session, uint64_t oid) {
  auto relation = FindRelation(session, RowTypeRelation(oid));
  if (!relation || !relation->entry || relation->key ||
      relation->entry->internal ||
      (relation->entry->type != duckdb::CatalogType::TABLE_ENTRY &&
       relation->entry->type != duckdb::CatalogType::VIEW_ENTRY)) {
    return std::nullopt;
  }
  return relation;
}

ObjectName NameOf(duckdb::CatalogTransaction transaction,
                  const Relation& relation) {
  if (relation.key) {
    return {
      .schema =
        relation.entry->ParentSchema(transaction).name.GetIdentifierName(),
      .name = relation.key->constraint_name};
  }
  if (IsTemporary(relation)) {
    return {.schema = kTemporarySchema,
            .name = relation.entry->name.GetIdentifierName()};
  }
  if (relation.entry) {
    return NameOf(transaction, *relation.entry);
  }
  if (relation.system) {
    return {.schema = relation.system->Sql().schema,
            .name = relation.system->Sql().name};
  }
  return {.schema = relation.view->schema, .name = relation.view->name};
}

const duckdb::ColumnList* TableColumns(const Relation& relation) {
  if (relation.system) {
    return &relation.system->Columns();
  }
  if (relation.key || !relation.entry ||
      relation.entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &relation.entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
}

std::optional<RelationAccess> RelationByOid(const Session& session,
                                            uint64_t oid) {
  auto relation = FindRelation(session, oid);
  if (!relation) {
    return std::nullopt;
  }
  if (relation->system) {
    return RelationAccess{
      {*relation}, &SystemPermissions(relation->system->SuperuserOnly())};
  }
  if (relation->view) {
    auto schema =
      session.database
        ? session.database->GetSchema(
            *session.transaction, duckdb::Identifier{relation->view->schema},
            duckdb::OnEntryNotFound::RETURN_NULL)
        : nullptr;
    const auto entry = schema
                         ? FindMember(*session.transaction, *schema,
                                      duckdb::CatalogType::VIEW_ENTRY,
                                      duckdb::Identifier{relation->view->name})
                         : nullptr;
    if (!entry) {
      return std::nullopt;
    }
    relation->entry = entry;
    return RelationAccess{{*relation}, &PrivilegePermissions(*entry)};
  }
  if (relation->entry->type == duckdb::CatalogType::INDEX_ENTRY) {
    const auto table_oid =
      relation->entry->Cast<duckdb::IndexCatalogEntry>().table_oid;
    const auto owner = IsTemporary(*relation)
                         ? TemporaryEntryByOid(session, table_oid)
                         : EntryByOid(session, table_oid);
    return RelationAccess{
      {*relation}, &PrivilegePermissions(owner ? *owner : *relation->entry)};
  }
  return RelationAccess{{*relation}, &PrivilegePermissions(*relation->entry)};
}

const duckdb::Permissions& PrivilegePermissions(
  const duckdb::CatalogEntry& entry) {
  static const duckdb::Permissions kUnowned{.owner = kInvalidOid};
  return auth::Unowned(entry) ? kUnowned : entry.permissions;
}

std::string ArrayTypeName(duckdb::CatalogTransaction transaction,
                          const duckdb::CatalogEntry& element) {
  using enum duckdb::CatalogType;
  const auto& name = element.name.GetIdentifierName();
  auto& schema = element.ParentSchema(transaction);
  auto array = absl::StrCat("_", name);
  const auto taken = [&](std::string_view candidate) {
    return absl::c_any_of(std::array{TYPE_ENTRY, TABLE_ENTRY}, [&](auto type) {
      return static_cast<bool>(schema.LookupEntry(
        transaction,
        duckdb::EntryLookupInfo{type, duckdb::Identifier{candidate}}));
    });
  };
  if (!name.starts_with('_') && !taken(array)) {
    return array;
  }
  const auto stem = [](std::string_view text) {
    const auto first = text.find_first_not_of('_');
    return first == std::string_view::npos ? std::string_view{}
                                           : text.substr(first);
  };
  std::vector<std::pair<duckdb::idx_t, std::string_view>> family;
  for (const auto type : {TYPE_ENTRY, TABLE_ENTRY}) {
    schema.Scan(transaction, type, [&](duckdb::CatalogEntry& entry) {
      if (!entry.internal &&
          stem(entry.name.GetIdentifierName()) == stem(name)) {
        family.emplace_back(entry.oid, entry.name.GetIdentifierName());
      }
    });
  }
  absl::c_sort(family);
  irs::containers::FlatHashSet<std::string> names;
  for (const auto& member : family) {
    names.emplace(member.second);
  }
  for (const auto& [oid, member] : family) {
    auto candidate = absl::StrCat("_", member);
    while (names.contains(candidate)) {
      candidate.insert(0, "_");
    }
    names.insert(candidate);
    if (oid == element.oid) {
      return candidate;
    }
  }
  return array;
}

SystemText ArrayTypeNames::Of(const duckdb::CatalogEntry& element) {
  if (element.ParentSchemaOid() != _schema) {
    static const SystemPrefix kUnderscored{
      .lower = duckdb::Identifier{std::string_view{"_"}},
      .upper = duckdb::Identifier{std::string_view{"_\xff", 2}}};
    auto& schema = element.ParentSchema(_scan.Transaction());
    _schema = element.ParentSchemaOid();
    _unprefixed = absl::c_none_of(
      std::array{duckdb::CatalogType::TYPE_ENTRY,
                 duckdb::CatalogType::TABLE_ENTRY},
      [&](duckdb::CatalogType type) {
        return !_scan.MemberSet(schema, type, &kUnderscored).entries.empty();
      });
  }
  const auto& name = element.name.GetIdentifierName();
  if (!name.starts_with('_') && _unprefixed) {
    return SystemConcatText{{"_", name}};
  }
  return ArrayTypeName(_scan.Transaction(), element);
}

std::optional<ObjectName> TypeObject(const Session& session, uint64_t oid) {
  if (oid & kRowTypeOidBit) {
    if (oid & kRowArrayTypeOidBit) {
      return std::nullopt;
    }
    const auto relation = RowTypeOwner(session, oid);
    if (!relation) {
      return std::nullopt;
    }
    return NameOf(*session.transaction, *relation);
  }
  if (oid < kMaxSystem) {
    if (const auto* builtin = FindBuiltinType(oid)) {
      return ObjectName{FindSystemObject(kSystemNamespaces, builtin->nsp)->name,
                        builtin->name};
    }
  }
  if (auto entry = EntryByOid(session, oid);
      entry && entry->type == duckdb::CatalogType::TYPE_ENTRY) {
    return NameOf(*session.transaction, *entry);
  }
  return std::nullopt;
}

uint64_t ArrayElementOid(const Session& session, uint64_t oid) {
  if (oid & kRowTypeOidBit) {
    return oid & kRowArrayTypeOidBit ? TypeElementOid(oid) : kInvalidOid;
  }
  const auto element = TypeElementOid(oid);
  const auto entry = EntryByOid(session, element);
  return entry && entry->type == duckdb::CatalogType::TYPE_ENTRY ? element
                                                                 : kInvalidOid;
}

std::vector<ReplicationOrigin> ReplicationOrigins(
  duckdb::DatabaseInstance& db) {
  std::vector<ReplicationOrigin> origins;
  for (const auto& database : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<duckdb::DuckCatalog>()
      .GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
      .Scan([&](duckdb::CatalogEntry& entry) {
        origins.emplace_back(
          entry.oid, absl::StrCat("pg_", entry.oid),
          entry.Cast<catalog::SubscriptionCatalogEntry>().RemoteLsn());
      });
  }
  catalog::ClusterOf(db)
    .GetCatalogSet(duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY)
    .Scan([&](duckdb::CatalogEntry& entry) {
      origins.emplace_back(
        entry.oid, entry.name.GetIdentifierName(),
        entry.Cast<duckdb::ReplicationLsnEntry>().RemoteLsn());
    });
  return origins;
}

catalog::CatalogKey FindKeyIndex(const Session& session, uint64_t oid) {
  if (auto table = OwnerTable(session, oid)) {
    for (const auto& key : KeyIndexes(*table)) {
      if (key.index_oid == oid) {
        return {.table = table.get(), .constraint = &key};
      }
    }
  }
  return {};
}

catalog::CatalogKey FindKeyIndex(duckdb::ClientContext& context,
                                 duckdb::SchemaCatalogEntry& schema,
                                 std::string_view name) {
  catalog::CatalogKey found;
  const auto match = [&](duckdb::CatalogEntry& entry) {
    if (found.table || entry.type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    auto& table = entry.Cast<duckdb::TableCatalogEntry>();
    for (const auto& key : KeyIndexes(table)) {
      if (key.constraint_name == name) {
        found = {.table = &table, .constraint = &key};
        return;
      }
    }
  };
  const auto transaction =
    schema.ParentCatalog().GetCatalogTransaction(context);
  for (auto end = name.rfind('_'); end != 0 && end != std::string_view::npos;
       end = name.rfind('_', end - 1)) {
    if (auto entry =
          FindMember(transaction, schema, duckdb::CatalogType::TABLE_ENTRY,
                     duckdb::Identifier{name.substr(0, end)})) {
      match(*entry);
      if (found.table) {
        return found;
      }
    }
  }
  auto& catalog = schema.ParentCatalog();
  const auto storage = catalog.GetCatalogType();
  const auto snapshot =
    storage == catalog::SereneDBCatalog::kStorageType ||
        storage == catalog::SereneDBCatalog::kReadOnlyStorageType
      ? catalog.Cast<catalog::SereneDBCatalog>().Snapshots().Get(
          context, schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(
                     duckdb::CatalogType::TABLE_ENTRY))
      : nullptr;
  if (!snapshot) {
    schema.Scan(context, duckdb::CatalogType::TABLE_ENTRY, match);
    return found;
  }
  const auto& keys = snapshot->keys.Get([&](auto& keys) {
    for (auto* entry : snapshot->entries) {
      if (entry->type != duckdb::CatalogType::TABLE_ENTRY) {
        continue;
      }
      auto& table = entry->Cast<duckdb::TableCatalogEntry>();
      for (const auto& key : KeyIndexes(table)) {
        keys.try_emplace(key.constraint_name, &table, &key);
      }
    }
  });
  if (const auto it = keys.find(name); it != keys.end()) {
    found = it->second;
  }
  return found;
}

}  // namespace sdb::pg
