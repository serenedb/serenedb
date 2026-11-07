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

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/default/default_generator.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/main/attached_database.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace sdb::catalog {

enum class ChangeKind : uint8_t {
  Create,
  Alter,
  Drop,
  Rename,
};

class EntrySet;
class OidIndex;

struct Dependent {
  duckdb::idx_t oid = 0;
  OidIndex* index = nullptr;
  duckdb::weak_ptr<duckdb::AttachedDatabase> database;
};

class DependentIndex final {
 public:
  void Add(duckdb::idx_t subject, duckdb::idx_t dependent, OidIndex& index);

  void Remove(duckdb::idx_t subject, duckdb::idx_t dependent);

  std::vector<Dependent> Of(duckdb::idx_t subject);

  void Forget(const OidIndex& index);

 private:
  struct Versions {
    OidIndex* index = nullptr;
    duckdb::weak_ptr<duckdb::AttachedDatabase> database;
    uint32_t count = 0;
  };

  absl::Mutex _mutex;
  irs::containers::FlatHashMap<
    duckdb::idx_t, irs::containers::FlatHashMap<duckdb::idx_t, Versions>>
    _dependents ABSL_GUARDED_BY(_mutex);
};

class OidIndex {
 public:
  OidIndex(duckdb::AttachedDatabase& database,
           std::shared_ptr<DependentIndex> dependents)
    : _database{database}, _dependents{std::move(dependents)} {}

  ~OidIndex() { _dependents->Forget(*this); }

  duckdb::weak_ptr<duckdb::AttachedDatabase> Database() const;

  const std::shared_ptr<DependentIndex>& SharedDependents() const noexcept {
    return _dependents;
  }

  void Add(duckdb::idx_t oid, EntrySet& set, const duckdb::Identifier& name);

  void Remove(duckdb::idx_t oid, const EntrySet& set);

  void Alias(std::span<const duckdb::idx_t> oids, duckdb::idx_t owner);

  duckdb::idx_t Owner(duckdb::idx_t oid);

  duckdb::optional_ptr<duckdb::CatalogEntry> Find(
    duckdb::CatalogTransaction transaction, duckdb::idx_t oid);

  duckdb::idx_t SetOwner(duckdb::idx_t oid);

  void AddDependents(const duckdb::CatalogEntry& version,
                     duckdb::idx_t dependent);

  void RemoveDependents(const duckdb::CatalogEntry& version,
                        duckdb::idx_t dependent);

  void ForEachDependent(
    duckdb::CatalogTransaction transaction, duckdb::idx_t subject,
    absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  void ForEachForeignDependent(
    duckdb::CatalogTransaction transaction,
    absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  void CheckCreated(const duckdb::CatalogEntry& version);

  void CheckDropped(const duckdb::SnapshotView& view,
                    const duckdb::CatalogEntry& dropped);

 private:
  struct Slot {
    EntrySet* set;
    duckdb::Identifier name;
  };

  Slot Newest(duckdb::idx_t oid);

  absl::Mutex _mutex;
  irs::containers::FlatHashMap<duckdb::idx_t, std::vector<Slot>> _slots
    ABSL_GUARDED_BY(_mutex);
  irs::containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t> _owners
    ABSL_GUARDED_BY(_mutex);
  duckdb::AttachedDatabase& _database;
  std::shared_ptr<DependentIndex> _dependents;
};

class EntrySet {
 public:
  EntrySet(bool case_sensitive, OidIndex& oids, duckdb::idx_t owner = 0,
           bool members = false)
    : _less{case_sensitive},
      _oids{oids},
      _owner{owner},
      _members{members},
      _entries{_less} {}
  ~EntrySet();

  duckdb::idx_t Owner() const noexcept { return _owner; }

  void SetDefaults(duckdb::unique_ptr<duckdb::DefaultGenerator> defaults) {
    _defaults = std::move(defaults);
  }

  bool SameName(const duckdb::Identifier& a,
                const duckdb::Identifier& b) const {
    return !_less(a, b) && !_less(b, a);
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> Get(
    duckdb::CatalogTransaction transaction, const duckdb::Identifier& name);

  duckdb::optional_ptr<duckdb::CatalogEntry> GetCommitted(
    const duckdb::Identifier& name);

  void CheckNameWritable(duckdb::CatalogTransaction transaction,
                         const duckdb::Identifier& name);

  bool Create(duckdb::CatalogTransaction transaction,
              duckdb::unique_ptr<duckdb::CatalogEntry> entry);

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::unique_ptr<duckdb::CatalogEntry> entry, std::string alter);

  bool Rename(duckdb::CatalogTransaction transaction,
              const duckdb::Identifier& name,
              duckdb::unique_ptr<duckdb::CatalogEntry> entry,
              std::string alter);
  bool Move(duckdb::CatalogTransaction transaction,
            const duckdb::Identifier& name, EntrySet& target,
            duckdb::unique_ptr<duckdb::CatalogEntry> entry,
            std::string alter) ABSL_NO_THREAD_SAFETY_ANALYSIS;

  bool Drop(duckdb::CatalogTransaction transaction,
            const duckdb::Identifier& name);

  void Scan(duckdb::CatalogTransaction transaction,
            absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  void ScanCommitted(absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  void Commit(duckdb::CatalogEntry& version);

  void Undo(duckdb::CatalogEntry& version, ChangeKind kind);

  duckdb::unique_ptr<duckdb::CatalogEntry> Trim(duckdb::CatalogEntry& version);

  std::weak_ptr<const void> Alive() const noexcept { return _alive; }

  void CreateDefaults(duckdb::CatalogTransaction transaction);

  bool Members() const noexcept { return _members; }

  OidIndex& Oids() noexcept { return _oids; }

  void CheckSubjectLive(const duckdb::Identifier& name, duckdb::idx_t oid);

  void CheckNoLaterDependent(const duckdb::SnapshotView& view,
                             const duckdb::Identifier& name, duckdb::idx_t oid,
                             const duckdb::CatalogEntry& dropped);

 private:
  duckdb::CatalogEntry& Push(duckdb::CatalogTransaction transaction,
                             duckdb::unique_ptr<duckdb::CatalogEntry> entry,
                             ChangeKind kind, std::string alter)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  bool PushNew(duckdb::CatalogTransaction transaction,
               duckdb::unique_ptr<duckdb::CatalogEntry> entry, ChangeKind kind,
               std::string alter) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  bool PushTombstone(duckdb::CatalogTransaction transaction,
                     const duckdb::Identifier& name, ChangeKind kind)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDefault(
    duckdb::CatalogTransaction transaction, const duckdb::Identifier& name);

  void Index(const duckdb::CatalogEntry& entry);

  void IndexDependents(const duckdb::CatalogEntry& version);

  void UnindexDependents(const duckdb::CatalogEntry& version);

  const duckdb::IdentifierCompare _less;
  OidIndex& _oids;
  const duckdb::idx_t _owner;
  const bool _members;
  std::shared_ptr<const char> _alive = std::make_shared<const char>();
  duckdb::unique_ptr<duckdb::DefaultGenerator> _defaults;
  absl::Mutex _mutex;
  duckdb::identifier_tree_t<duckdb::unique_ptr<duckdb::CatalogEntry>> _entries
    ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::catalog
