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

#include "catalog/entry_set.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_versions.hpp>
#include <duckdb/catalog/schema_info.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/search_table.h"
#include "catalog/transaction.h"

namespace sdb::catalog {
namespace {

bool Dropped(duckdb::AttachedDatabase& database) {
  auto& catalog = database.GetCatalog();
  return IsSereneDB(catalog) && catalog.Cast<SereneDBCatalog>().IsDropped();
}

duckdb::CatalogEntry* Visible(const duckdb::SnapshotView& view,
                              duckdb::CatalogEntry& head) {
  for (auto* entry = &head;; entry = &entry->Child()) {
    if (view.Sees(entry->timestamp)) {
      return entry;
    }
    if (!entry->HasChild()) {
      return nullptr;
    }
  }
}

bool Holds(duckdb::CatalogEntry* chain, duckdb::idx_t oid) {
  for (auto* version = chain; version;
       version = version->HasChild() ? &version->Child() : nullptr) {
    if (version->oid == oid) {
      return true;
    }
  }
  return false;
}

duckdb::CatalogEntry* Committed(duckdb::CatalogEntry& head) {
  for (auto* entry = &head;; entry = &entry->Child()) {
    if (entry->timestamp < duckdb::TRANSACTION_ID_START) {
      return entry;
    }
    if (!entry->HasChild()) {
      return nullptr;
    }
  }
}

void CheckWritable(const duckdb::SnapshotView& view,
                   const duckdb::CatalogEntry& head) {
  const duckdb::transaction_t timestamp = head.timestamp;
  if (timestamp == view.transaction_id ||
      (timestamp < duckdb::TRANSACTION_ID_START && view.Sees(timestamp))) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent update of \"",
            head.name.GetIdentifierName(), "\""));
}

void ForEachSubject(
  const duckdb::CatalogEntry& version,
  absl::FunctionRef<void(duckdb::idx_t, const duckdb::LogicalDependency*)>
    visit) {
  if (version.deleted || !version.oid) {
    return;
  }
  const auto subject = [&](duckdb::idx_t oid,
                           const duckdb::LogicalDependency* dependency) {
    if (oid && oid != version.oid) {
      visit(oid, dependency);
    }
  };
  subject(version.ParentSchemaOid(), nullptr);
  if (version.type == duckdb::CatalogType::INDEX_ENTRY) {
    subject(version.Cast<duckdb::IndexCatalogEntry>().table_oid, nullptr);
  }
  for (const auto& dependency :
       version.Cast<duckdb::StandardEntry>().dependencies.Set()) {
    subject(dependency.oid, &dependency);
  }
  if (IsSearchTable(version)) {
    for (const auto& spec : version.Cast<SearchTableEntry>().KeySpecs()) {
      if (spec.kind == search::KeyKind::Reference && !spec.self_reference) {
        subject(spec.referenced_field, nullptr);
      }
    }
  }
  if (version.type == duckdb::CatalogType::ROLE_ENTRY) {
    return;
  }
  subject(version.permissions.owner, nullptr);
  for (const auto& item : version.permissions.acl) {
    subject(item.grantee, nullptr);
    subject(item.grantor, nullptr);
  }
}

bool DependsOn(const duckdb::CatalogEntry& member, duckdb::idx_t subject,
               const duckdb::Identifier& catalog, bool local) {
  bool depends = false;
  ForEachSubject(member, [&](duckdb::idx_t oid,
                             const duckdb::LogicalDependency* dependency) {
    if (oid != subject) {
      return;
    }
    if (!dependency) {
      depends |= local;
      return;
    }
    depends |=
      dependency->catalog == catalog || (local && dependency->catalog.empty());
  });
  return depends;
}

const duckdb::Constraint* FindConstraint(const duckdb::CatalogEntry& table,
                                         duckdb::idx_t oid) {
  const auto& constraints =
    table.Cast<duckdb::TableCatalogEntry>().GetConstraints();
  const auto it = absl::c_find_if(constraints, [&](const auto& constraint) {
    return constraint->oid == oid;
  });
  return it == constraints.end() ? nullptr : it->get();
}

bool Redefined(const duckdb::Constraint& before,
               const duckdb::Constraint* after) {
  return !after || before.ToString() != after->ToString();
}

}  // namespace

void ForEachSubject(const duckdb::CatalogEntry& version,
                    absl::FunctionRef<void(duckdb::idx_t)> visit) {
  ForEachSubject(
    version,
    [&](duckdb::idx_t oid, const duckdb::LogicalDependency*) { visit(oid); });
}

void DependentIndex::Add(duckdb::idx_t subject, duckdb::idx_t dependent,
                         OidIndex& index) {
  auto database = index.Database();
  absl::MutexLock lock{&_mutex};
  auto& versions = _dependents[subject][{dependent, &index}];
  if (versions.count++ == 0) {
    versions.database = std::move(database);
  }
}

void DependentIndex::Remove(duckdb::idx_t subject, duckdb::idx_t dependent,
                            OidIndex& index) {
  absl::MutexLock lock{&_mutex};
  const auto it = _dependents.find(subject);
  SDB_ASSERT(it != _dependents.end());
  const auto versions = it->second.find(std::pair{dependent, &index});
  SDB_ASSERT(versions != it->second.end());
  if (--versions->second.count == 0) {
    it->second.erase(versions);
    if (it->second.empty()) {
      _dependents.erase(it);
    }
  }
}

std::vector<Dependent> DependentIndex::Of(duckdb::idx_t subject) {
  std::vector<Dependent> dependents;
  absl::ReaderMutexLock lock{&_mutex};
  if (const auto it = _dependents.find(subject); it != _dependents.end()) {
    dependents.reserve(it->second.size());
    for (const auto& [key, versions] : it->second) {
      dependents.push_back(
        {.oid = key.first, .index = key.second, .database = versions.database});
    }
  }
  return dependents;
}

void DependentIndex::Forget(const OidIndex& index) {
  absl::MutexLock lock{&_mutex};
  absl::erase_if(_dependents, [&](auto& subject) {
    absl::erase_if(subject.second, [&](const auto& dependent) {
      return dependent.first.second == &index;
    });
    return subject.second.empty();
  });
}

duckdb::weak_ptr<duckdb::AttachedDatabase> OidIndex::Database() const {
  return _database.weak_from_this();
}

void OidIndex::Add(duckdb::idx_t oid, EntrySet& set,
                   const duckdb::Identifier& name) {
  absl::MutexLock lock{&_mutex};
  _slots[oid].push_back({&set, name});
}

void OidIndex::Erase(duckdb::idx_t oid, const EntrySet& set,
                     const duckdb::Identifier& name) {
  const auto it = _slots.find(oid);
  if (it == _slots.end()) {
    return;
  }
  std::erase_if(it->second, [&](const Slot& slot) {
    return slot.set == &set && set.SameName(slot.name, name);
  });
  if (it->second.empty()) {
    _slots.erase(it);
  }
}

void OidIndex::Remove(duckdb::idx_t oid, const EntrySet& set,
                      const duckdb::Identifier& name) {
  absl::MutexLock lock{&_mutex};
  const auto it = _slots.find(oid);
  if (it == _slots.end()) {
    return;
  }
  auto& slots = it->second;
  for (auto slot = slots.rbegin(); slot != slots.rend(); ++slot) {
    if (slot->set == &set && set.SameName(slot->name, name)) {
      slots.erase(std::next(slot).base());
      break;
    }
  }
  if (slots.empty()) {
    _slots.erase(it);
  }
}

void OidIndex::Forget(const EntrySet& set,
                      std::span<duckdb::CatalogEntry* const> versions) {
  absl::MutexLock lock{&_mutex};
  for (const auto* version : versions) {
    Erase(version->oid, set, version->name);
  }
}

void OidIndex::Alias(std::span<const duckdb::idx_t> oids, duckdb::idx_t owner) {
  absl::MutexLock lock{&_owners_mutex};
  for (const auto oid : oids) {
    _owners.try_emplace(oid, owner);
  }
}

duckdb::idx_t OidIndex::Owner(duckdb::idx_t oid) {
  absl::ReaderMutexLock lock{&_owners_mutex};
  const auto it = _owners.find(oid);
  return it == _owners.end() ? 0 : it->second;
}

duckdb::optional_ptr<duckdb::CatalogEntry> OidIndex::Find(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  duckdb::idx_t owner = 0;
  {
    absl::ReaderMutexLock lock{&_mutex};
    const auto it = _slots.find(oid);
    if (it == _slots.end()) {
      return nullptr;
    }
    for (auto slot = it->second.rbegin(); slot != it->second.rend(); ++slot) {
      entry = slot->set->GetByOid(transaction, slot->name, oid);
      if (entry) {
        owner = slot->set->Owner();
        break;
      }
    }
  }
  if (entry && owner && !Find(transaction, owner)) {
    return nullptr;
  }
  return entry;
}

duckdb::idx_t OidIndex::SetOwner(duckdb::idx_t oid) {
  duckdb::idx_t owner = 0;
  VisitNewest(oid, [&](EntrySet& set, const duckdb::Identifier&) {
    owner = set.Owner();
  });
  return owner;
}

void OidIndex::AddDependents(const duckdb::CatalogEntry& version,
                             duckdb::idx_t dependent) {
  ForEachSubject(version, [&](duckdb::idx_t subject) {
    _dependents->Add(subject, dependent, *this);
  });
}

void OidIndex::RemoveDependents(const duckdb::CatalogEntry& version,
                                duckdb::idx_t dependent) {
  ForEachSubject(version, [&](duckdb::idx_t subject) {
    _dependents->Remove(subject, dependent, *this);
  });
}

void OidIndex::ForEachDependent(
  duckdb::CatalogTransaction transaction, duckdb::idx_t subject,
  absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  for (const auto& dependent : _dependents->Of(subject)) {
    duckdb::shared_ptr<duckdb::AttachedDatabase> pinned;
    if (dependent.index != this) {
      pinned = duckdb::AttachedDatabase::TryGetReference(dependent.database);
      if (!pinned || Dropped(*pinned)) {
        continue;
      }
    }
    const bool local = dependent.index == this;
    if (auto member = dependent.index->Find(transaction, dependent.oid);
        member && DependsOn(*member, subject, _database.GetName(), local)) {
      callback(*member);
    }
  }
}

void OidIndex::ForEachForeignDependent(
  duckdb::CatalogTransaction transaction,
  absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  std::vector<duckdb::idx_t> oids;
  {
    absl::ReaderMutexLock lock{&_mutex};
    oids.reserve(_slots.size());
    for (const auto& [oid, slots] : _slots) {
      oids.push_back(oid);
    }
  }
  for (const auto oid : oids) {
    for (const auto& dependent : _dependents->Of(oid)) {
      if (dependent.index == this) {
        continue;
      }
      const auto pinned =
        duckdb::AttachedDatabase::TryGetReference(dependent.database);
      if (!pinned || Dropped(*pinned)) {
        continue;
      }
      if (auto member = dependent.index->Find(transaction, dependent.oid);
          member && DependsOn(*member, oid, _database.GetName(), false)) {
        callback(*member);
      }
    }
  }
}

void OidIndex::VisitNewest(
  duckdb::idx_t oid,
  absl::FunctionRef<void(EntrySet&, const duckdb::Identifier&)> visit) {
  absl::ReaderMutexLock lock{&_mutex};
  if (const auto it = _slots.find(oid); it != _slots.end()) {
    visit(*it->second.back().set, it->second.back().name);
  }
}

void OidIndex::CheckCreated(const duckdb::SnapshotView& view,
                            const duckdb::CatalogEntry& version) {
  ForEachSubject(version, [&](duckdb::idx_t subject,
                              const duckdb::LogicalDependency* dependency) {
    auto* index = this;
    duckdb::shared_ptr<duckdb::AttachedDatabase> database;
    if (dependency && !dependency->catalog.empty() &&
        dependency->catalog != _database.GetName()) {
      database = duckdb::DatabaseManager::Get(_database.GetDatabase())
                   .GetDatabase(dependency->catalog);
      if (!database) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
          ERR_MSG("Failed to commit: Could not commit creation of dependency, "
                  "subject \"",
                  dependency->entry.name.GetIdentifierName(),
                  "\" has been deleted"));
      }
      index = &database->GetCatalog().Cast<BaseCatalog>().Oids();
    }
    index->CheckLive(view, subject);
  });
  if (version.deleted || version.type == duckdb::CatalogType::ROLE_ENTRY) {
    return;
  }
  auto& roles = ClusterOf().Oids();
  const auto check_role = [&](duckdb::idx_t role) {
    if (role) {
      roles.CheckLive(view, role);
    }
  };
  check_role(version.permissions.owner);
  for (const auto& item : version.permissions.acl) {
    check_role(item.grantee);
    check_role(item.grantor);
  }
}

void OidIndex::CheckLive(const duckdb::SnapshotView& view, duckdb::idx_t oid) {
  bool indexed = false;
  VisitNewest(oid, [&](EntrySet& set, const duckdb::Identifier& name) {
    indexed = true;
    set.CheckSubjectLive(view, name, oid, 0);
  });
  if (indexed) {
    return;
  }
  if (const auto owner = Owner(oid)) {
    VisitNewest(owner, [&](EntrySet& set, const duckdb::Identifier& name) {
      set.CheckSubjectLive(view, name, owner, oid);
    });
  }
}

void OidIndex::CheckChangedParts(const duckdb::SnapshotView& view,
                                 duckdb::CatalogEntry& version) {
  if (version.type != duckdb::CatalogType::TABLE_ENTRY || !version.HasChild() ||
      version.Child().type != duckdb::CatalogType::TABLE_ENTRY) {
    return;
  }
  for (const auto& constraint :
       version.Child().Cast<duckdb::TableCatalogEntry>().GetConstraints()) {
    if (Redefined(*constraint, FindConstraint(version, constraint->oid))) {
      CheckDropped(
        view, {.oid = constraint->oid, .name = constraint->constraint_name});
    }
  }
}

void OidIndex::CheckDropped(const duckdb::SnapshotView& view,
                            const DroppedSubject& dropped) {
  for (const auto& dependent : _dependents->Of(dropped.oid)) {
    duckdb::shared_ptr<duckdb::AttachedDatabase> pinned;
    if (dependent.index != this) {
      pinned = duckdb::AttachedDatabase::TryGetReference(dependent.database);
      if (!pinned) {
        continue;
      }
    }
    const bool local = dependent.index == this;
    dependent.index->VisitNewest(
      dependent.oid, [&](EntrySet& set, const duckdb::Identifier& name) {
        set.CheckNoLaterDependent(view, name, dependent.oid, dropped,
                                  _database.GetName(), local);
      });
  }
}

EntrySet::~EntrySet() {
  std::vector<duckdb::CatalogEntry*> indexed;
  {
    absl::ReaderMutexLock lock{&_mutex};
    for (const auto& [name, head] : _entries) {
      for (auto* version = head.get(); version;
           version = version->HasChild() ? &version->Child() : nullptr) {
        UnindexDependents(*version);
        if (version->oid) {
          indexed.push_back(version);
        }
      }
    }
  }
  _oids.Forget(*this, indexed);
}

void EntrySet::CheckSubjectLive(const duckdb::SnapshotView& view,
                                const duckdb::Identifier& name,
                                duckdb::idx_t oid, duckdb::idx_t part) {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _entries.find(name);
  const auto* head = it == _entries.end() ? nullptr : it->second.get();
  if (head && head->timestamp >= duckdb::TRANSACTION_ID_START) {
    return;
  }
  if (!head || head->deleted) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("Failed to commit: Could not commit creation of dependency, "
              "subject \"",
              name.GetIdentifierName(), "\" has been deleted"));
  }
  if (head->oid != oid) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("Failed to commit: Could not commit creation of dependency, "
              "subject \"",
              name.GetIdentifierName(),
              "\" was dropped and re-created by another transaction"));
  }
  if (!part) {
    return;
  }
  const auto* seen = Visible(view, *it->second);
  const auto* bound = seen ? FindConstraint(*seen, part) : nullptr;
  if (bound && Redefined(*bound, FindConstraint(*head, part))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("Failed to commit: Could not commit creation of dependency, "
              "constraint \"",
              bound->constraint_name, "\" of \"", name.GetIdentifierName(),
              "\" was changed by another transaction"));
  }
}

void EntrySet::CheckNoLaterDependent(const duckdb::SnapshotView& view,
                                     const duckdb::Identifier& name,
                                     duckdb::idx_t oid,
                                     const DroppedSubject& dropped,
                                     const duckdb::Identifier& catalog,
                                     bool local) {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _entries.find(name);
  if (it == _entries.end()) {
    return;
  }
  for (auto* version = it->second.get(); version;
       version = version->HasChild() ? &version->Child() : nullptr) {
    if (version->timestamp >= duckdb::TRANSACTION_ID_START) {
      continue;
    }
    if (view.Sees(version->timestamp)) {
      return;
    }
    bool depends = false;
    if (version->oid == oid) {
      if (dropped.role) {
        ForEachSubject(*version, [&](duckdb::idx_t subject) {
          depends |= subject == dropped.oid;
        });
      } else {
        depends = DependsOn(*version, dropped.oid, catalog, local);
      }
    }
    if (depends) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Failed to commit: Could not commit DROP of \"", dropped.name,
                "\" because a dependency was created after the transaction "
                "started"));
    }
  }
}

void EntrySet::Index(const duckdb::CatalogEntry& entry) {
  if (entry.oid) {
    _oids.Add(entry.oid, *this, entry.name);
  }
}

void EntrySet::IndexDependents(const duckdb::CatalogEntry& version) {
  if (_members) {
    _oids.AddDependents(version, version.oid);
  }
}

void EntrySet::UnindexDependents(const duckdb::CatalogEntry& version) {
  if (_members) {
    _oids.RemoveDependents(version, version.oid);
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntrySet::Get(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name) {
  {
    absl::ReaderMutexLock lock{&_mutex};
    const auto it = _entries.find(name);
    if (it != _entries.end()) {
      auto* entry = Visible(transaction.view, *it->second);
      if (!entry || entry->deleted) {
        return nullptr;
      }
      return entry;
    }
  }
  return CreateDefault(transaction, name);
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntrySet::GetByOid(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name,
  duckdb::idx_t oid) {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _entries.find(name);
  if (it == _entries.end()) {
    return nullptr;
  }
  auto* entry = Visible(transaction.view, *it->second);
  if (!entry || entry->deleted || entry->oid != oid) {
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntrySet::CreateDefault(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name) {
  if (!_defaults || _defaults->created_all_entries) {
    return nullptr;
  }
  auto entry = _defaults->CreateDefaultEntry(transaction, name);
  if (!entry) {
    return nullptr;
  }
  duckdb::CatalogEntry* created = nullptr;
  duckdb::optional_ptr<duckdb::CatalogEntry> found;
  {
    absl::MutexLock lock{&_mutex};
    auto& head = _entries[name];
    if (!head) {
      entry->timestamp = 0;
      head = std::move(entry);
      created = head.get();
      IndexDependents(*head);
    }
    if (auto* visible = Visible(transaction.view, *head);
        visible && !visible->deleted) {
      found = visible;
    }
  }
  if (created) {
    Index(*created);
  }
  return found;
}

void EntrySet::CreateDefaults(duckdb::CatalogTransaction transaction) {
  if (!_defaults || _defaults->created_all_entries) {
    return;
  }
  for (const auto& name : _defaults->GetDefaultEntries()) {
    {
      absl::ReaderMutexLock lock{&_mutex};
      if (_entries.contains(name)) {
        continue;
      }
    }
    auto entry = _defaults->CreateDefaultEntry(transaction, name);
    SDB_ENSURE(entry, "failed to create the default entry ",
               name.GetIdentifierName());
    duckdb::CatalogEntry* created = nullptr;
    {
      absl::MutexLock lock{&_mutex};
      auto& head = _entries[name];
      if (!head) {
        entry->timestamp = 0;
        head = std::move(entry);
        created = head.get();
        IndexDependents(*head);
      }
    }
    if (created) {
      Index(*created);
    }
  }
  _defaults->created_all_entries = true;
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntrySet::GetCommitted(
  const duckdb::Identifier& name) {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _entries.find(name);
  if (it == _entries.end()) {
    return nullptr;
  }
  auto* entry = Committed(*it->second);
  if (!entry || entry->deleted) {
    return nullptr;
  }
  return entry;
}

duckdb::CatalogEntry& EntrySet::Push(
  duckdb::CatalogTransaction transaction,
  duckdb::unique_ptr<duckdb::CatalogEntry> entry, ChangeKind kind,
  std::string alter) {
  auto& head = _entries.find(entry->name)->second;
  entry->timestamp = transaction.view.transaction_id;
  entry->SetChild(std::move(head));
  head = std::move(entry);
  IndexDependents(*head);
  if (head->type == duckdb::CatalogType::SCHEMA_ENTRY) {
    head->Cast<duckdb::SchemaCatalogEntry>().GetSchemaInfo()->versions->Install(
      *head);
  } else if (head->type == duckdb::CatalogType::TABLE_ENTRY) {
    _oids.Alias(head->Cast<duckdb::TableCatalogEntry>().GetSubObjectOids(),
                head->oid);
  }
  PushChange(transaction, Change{.set = this,
                                 .version = head.get(),
                                 .kind = kind,
                                 .alter = std::move(alter)});
  return *head;
}

duckdb::CatalogEntry* EntrySet::PushNew(
  duckdb::CatalogTransaction transaction,
  duckdb::unique_ptr<duckdb::CatalogEntry>& entry, ChangeKind kind,
  std::string alter) {
  auto& head = _entries[entry->name];
  if (!head) {
    head = duckdb::make_uniq<duckdb::CatalogEntry>(duckdb::CatalogType::INVALID,
                                                   entry->name, 0);
    head->timestamp = 0;
    head->deleted = true;
  } else {
    CheckWritable(transaction.view, *head);
    if (!head->deleted) {
      return nullptr;
    }
  }
  return &Push(transaction, std::move(entry), kind, std::move(alter));
}

bool EntrySet::PushTombstone(duckdb::CatalogTransaction transaction,
                             const duckdb::Identifier& name, ChangeKind kind) {
  const auto it = _entries.find(name);
  if (it == _entries.end()) {
    return false;
  }
  CheckWritable(transaction.view, *it->second);
  if (it->second->deleted) {
    return false;
  }
  auto tombstone = duckdb::make_uniq<duckdb::CatalogEntry>(
    duckdb::CatalogType::DELETED_ENTRY, name, 0);
  tombstone->deleted = true;
  Push(transaction, std::move(tombstone), kind, {});
  return true;
}

void EntrySet::CheckNameWritable(duckdb::CatalogTransaction transaction,
                                 const duckdb::Identifier& name) {
  absl::MutexLock lock{&_mutex};
  if (const auto it = _entries.find(name); it != _entries.end()) {
    CheckWritable(transaction.view, *it->second);
  }
}

bool EntrySet::Create(duckdb::CatalogTransaction transaction,
                      duckdb::unique_ptr<duckdb::CatalogEntry> entry) {
  duckdb::CatalogEntry* created = nullptr;
  {
    absl::MutexLock lock{&_mutex};
    created = PushNew(transaction, entry, ChangeKind::Create, {});
  }
  if (!created) {
    return false;
  }
  Index(*created);
  return true;
}

void EntrySet::Alter(duckdb::CatalogTransaction transaction,
                     duckdb::unique_ptr<duckdb::CatalogEntry> entry,
                     std::string alter) {
  absl::MutexLock lock{&_mutex};
  const auto it = _entries.find(entry->name);
  SDB_ASSERT(it != _entries.end());
  CheckWritable(transaction.view, *it->second);
  SDB_ASSERT(!it->second->deleted);
  Push(transaction, std::move(entry), ChangeKind::Alter, std::move(alter));
}

bool EntrySet::Rename(duckdb::CatalogTransaction transaction,
                      const duckdb::Identifier& name,
                      duckdb::unique_ptr<duckdb::CatalogEntry> entry,
                      std::string alter) {
  duckdb::CatalogEntry* renamed = nullptr;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _entries.find(entry->name);
    if (it != _entries.end()) {
      CheckWritable(transaction.view, *it->second);
      if (!it->second->deleted) {
        return false;
      }
    }
    PushTombstone(transaction, name, ChangeKind::Rename);
    renamed = PushNew(transaction, entry, ChangeKind::Alter, std::move(alter));
  }
  if (!renamed) {
    return false;
  }
  Index(*renamed);
  return true;
}

bool EntrySet::Move(duckdb::CatalogTransaction transaction,
                    const duckdb::Identifier& name, EntrySet& target,
                    duckdb::unique_ptr<duckdb::CatalogEntry> entry,
                    std::string alter) {
  SDB_ASSERT(&target != this);
  duckdb::CatalogEntry* moved = nullptr;
  {
    const bool ours_first = this < &target;
    absl::MutexLock first{ours_first ? &_mutex : &target._mutex};
    absl::MutexLock second{ours_first ? &target._mutex : &_mutex};
    const auto it = target._entries.find(entry->name);
    if (it != target._entries.end()) {
      CheckWritable(transaction.view, *it->second);
      if (!it->second->deleted) {
        return false;
      }
    }
    PushTombstone(transaction, name, ChangeKind::Rename);
    moved =
      target.PushNew(transaction, entry, ChangeKind::Alter, std::move(alter));
  }
  if (!moved) {
    return false;
  }
  target.Index(*moved);
  return true;
}

bool EntrySet::Drop(duckdb::CatalogTransaction transaction,
                    const duckdb::Identifier& name) {
  absl::MutexLock lock{&_mutex};
  return PushTombstone(transaction, name, ChangeKind::Drop);
}

void EntrySet::Scan(duckdb::CatalogTransaction transaction,
                    absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  CreateDefaults(transaction);
  std::vector<duckdb::CatalogEntry*> entries;
  {
    absl::ReaderMutexLock lock{&_mutex};
    entries.reserve(_entries.size());
    for (const auto& [name, head] : _entries) {
      auto* entry = Visible(transaction.view, *head);
      if (entry && !entry->deleted) {
        entries.push_back(entry);
      }
    }
  }
  for (auto* entry : entries) {
    callback(*entry);
  }
}

void EntrySet::ScanCommitted(
  absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  const CommittedView committed;
  const auto view = committed.View();
  std::vector<duckdb::CatalogEntry*> entries;
  {
    absl::ReaderMutexLock lock{&_mutex};
    entries.reserve(_entries.size());
    for (const auto& [name, head] : _entries) {
      auto* entry = Visible(view, *head);
      if (entry && !entry->deleted) {
        entries.push_back(entry);
      }
    }
  }
  for (auto* entry : entries) {
    callback(*entry);
  }
}

void EntrySet::Commit(duckdb::CatalogEntry& version) {
  if (version.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    version.Cast<duckdb::SchemaCatalogEntry>()
      .GetSchemaInfo()
      ->versions->Commit(version);
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> EntrySet::Trim(
  duckdb::CatalogEntry& version) {
  duckdb::unique_ptr<duckdb::CatalogEntry> trimmed;
  std::vector<duckdb::idx_t> gone;
  {
    absl::MutexLock lock{&_mutex};
    version.previous_version.store(nullptr, std::memory_order_release);
    const auto it = _entries.find(version.name);
    duckdb::CatalogEntry* kept =
      it == _entries.end() ? nullptr : it->second.get();
    if (version.deleted && !_defaults && kept == &version) {
      trimmed = std::move(it->second);
      _entries.erase(it);
      kept = nullptr;
    }
    if (!trimmed) {
      trimmed = version.TakeChild();
    }
    for (auto* old = trimmed.get(); old;
         old = old->HasChild() ? &old->Child() : nullptr) {
      UnindexDependents(*old);
      if (old->oid && !absl::c_linear_search(gone, old->oid) &&
          !Holds(kept, old->oid)) {
        gone.push_back(old->oid);
      }
    }
  }
  for (const auto oid : gone) {
    _oids.Remove(oid, *this, version.name);
  }
  return trimmed;
}

void EntrySet::Undo(duckdb::CatalogEntry& version, ChangeKind kind) {
  if (version.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    version.Cast<duckdb::SchemaCatalogEntry>()
      .GetSchemaInfo()
      ->versions->Rollback(version);
  }
  if (kind != ChangeKind::Alter ||
      !duckdb::CatalogEntry::CreatedObject(version.Child())) {
    version.Rollback(version.Child());
  }
  duckdb::unique_ptr<duckdb::CatalogEntry> removed;
  bool unindex = false;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _entries.find(version.name);
    SDB_ASSERT(it != _entries.end() && it->second.get() == &version);
    removed = std::move(it->second);
    it->second = removed->TakeChild();
    UnindexDependents(*removed);
    unindex = removed->oid && it->second->deleted;
    if (!it->second->deleted &&
        it->second->type != duckdb::CatalogType::INVALID) {
      it->second->SetAsRoot(nullptr, nullptr);
    }
    if (it->second->type == duckdb::CatalogType::INVALID) {
      _entries.erase(it);
    }
  }
  if (unindex) {
    _oids.Remove(removed->oid, *this, removed->name);
  }
}

}  // namespace sdb::catalog
