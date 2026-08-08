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

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <span>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class Catalog;
class CatalogEntry;
class ClientContext;
class DependencyManager;

}  // namespace duckdb
namespace sdb::catalog {

// How duckdb's dependency graph addresses one SereneDB object: the stable id,
// and nothing else. An id is unique across the cluster, so neither the schema
// nor the attachment is part of the address -- and a rename therefore never has
// to rewrite an edge, which is the whole reason duckdb's own name-keyed form is
// not used here. SereneDBCatalog::GetDependencyInfo / GetDependencyEntry are
// the two ends of it.
duckdb::CatalogEntryInfo DependencyInfo(ObjectId id);

// The id an address names, unset when the address is a name-keyed one.
ObjectId DependencyInfoId(const duckdb::CatalogEntryInfo& info) noexcept;

// The dependency list a set of ids makes. What reaches duckdb is only that the
// two objects are connected: what a DROP of the referenced one does to the
// dependent is re-derived from the dependent's own definition, so the verb is
// never written down.
duckdb::LogicalDependencyList DependencyList(std::span<const ObjectId> ids);
// The roles one object names as owner, grantee or grantor, together with
// whatever else it references -- for a kind whose body states its references on
// duckdb's own CreateInfo::dependencies.
duckdb::LogicalDependencyList EntryDependencies(
  const duckdb::CreateInfo& info, const catalog::Permissions& perm);

// What one object depends on, stated in full. Every other kind states it on the
// CatalogSet::CreateEntry that writes its version; this is for the two shapes
// that cannot:
//   * a schema entry, which is mutated in place rather than versioned because
//   it
//     owns the CatalogSets of its whole contents, so no create call carries it;
//   * the contents of a schema being dropped, whose entries die with those sets
//     and so never reach DropEntry to have their references retired (empty
//     list).
// `owner` is the catalog keeping the edges, which outlives the schema entry.
void SetEntryDependencies(duckdb::ClientContext* context,
                          duckdb::Catalog& owner, ObjectId id,
                          const duckdb::LogicalDependencyList& deps);

// One object that names another. The entry is resolved unless the dependent is
// a cluster-global kind -- a database naming its owner, a role naming one in
// its default privileges -- which hangs off no database and so answers to no
// id index.
struct Dependent {
  ObjectId id;
  duckdb::CatalogType type{duckdb::CatalogType::INVALID};
  duckdb::CatalogEntry* entry = nullptr;
};

// The reverse index as one reader sees it: every edge pointing at an object.
//
// duckdb's managers record that one object depends on another; what a DROP of
// the referenced object does to the dependent is re-derived from the
// dependent's own definition, which is where the verb came from in the first
// place. Nothing about a cascade is therefore written down twice.
//
// An edge lives in the dependent's own catalog, so the scan visits every
// attached serenedb manager: a role belongs to no database and its dependents
// belong to all of them, and an ATTACHed database's view can name a relation
// here.
class DependencyView {
 public:
  // `context` is the statement asking, or null for boot, WAL replay and the
  // background drop paths, which see only what is committed.
  explicit DependencyView(duckdb::ClientContext* context);

  std::vector<Dependent> Dependents(ObjectId referenced) const;

  std::size_t CountDependents(ObjectId referenced) const;

 private:
  struct Attachment {
    duckdb::Catalog* catalog;
    duckdb::DependencyManager* manager;
    duckdb::CatalogTransaction transaction;
  };

  // The dependent ids of `referenced`, collected before anything is resolved:
  // resolving inside the scan would re-enter a set the scan is holding.
  std::vector<ObjectId> DependentIds(ObjectId referenced) const;

  // The entry one id names, or null. Only the per-database catalogs are asked:
  // they answer through an id index, while the cluster-global sets hang off the
  // catalog and would have to be scanned -- which is why the kinds living there
  // are taken once, below.
  duckdb::optional_ptr<duckdb::CatalogEntry> Resolve(ObjectId id) const;

  std::vector<Attachment> _attachments;
  // The cluster-global kinds, taken once: a role is the subject of every Block
  // edge, so asking per edge would scan the role set per edge.
  containers::FlatHashMap<ObjectId, duckdb::CatalogType> _global_kinds;
};

// Every recorded edge in the cluster, for the introspection projection.
void VisitAllEdges(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(ObjectId referenced, ObjectId dependent)> visitor);

}  // namespace sdb::catalog
