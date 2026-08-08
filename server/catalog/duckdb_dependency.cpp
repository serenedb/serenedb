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

#include "catalog/duckdb_dependency.h"

#include <algorithm>
#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "basics/duckdb_engine.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_global_catalog.h"
#include "catalog/duckdb_object_index.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/object_dependency.h"
#include "catalog/store/store.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kHexDigits = "0123456789abcdef";
constexpr size_t kIdWidth = 16;

// Fixed width, so the address of an object is one string of a known shape and
// an ordered set keyed on it is keyed on identity alone.
std::string HexId(ObjectId id) {
  std::string name(kIdWidth, '0');
  auto value = id.id();
  for (size_t i = 0; i < kIdWidth; ++i) {
    name[kIdWidth - 1 - i] = kHexDigits[value & 0xF];
    value >>= 4;
  }
  return name;
}

// The transaction one catalog's edges are read or written through. A null
// context is boot, WAL replay or a background drop: no transaction of its own,
// and everything it may see is committed.
duckdb::CatalogTransaction EdgeTransaction(duckdb::ClientContext* context,
                                           duckdb::Catalog& owner) {
  if (context != nullptr && context->transaction.HasActiveTransaction()) {
    return owner.GetCatalogTransaction(*context);
  }
  auto transaction = duckdb::CatalogTransaction::GetSystemTransaction(
    owner.GetAttached().GetDatabase());
  transaction.start_time = duckdb::TRANSACTION_ID_START - 1;
  return transaction;
}

}  // namespace

duckdb::CatalogEntryInfo DependencyInfo(ObjectId id) {
  return duckdb::CatalogEntryInfo{
    duckdb::CatalogType::INVALID, duckdb::Identifier{},
    duckdb::Identifier{HexId(id)}, duckdb::Identifier{}};
}

ObjectId DependencyInfoId(const duckdb::CatalogEntryInfo& info) noexcept {
  if (!info.schema.empty() || !info.catalog.empty()) {
    return {};
  }
  const std::string_view name = info.name.GetIdentifierName();
  if (name.size() != kIdWidth) {
    return {};
  }
  uint64_t value = 0;
  for (const char ch : name) {
    const auto digit = kHexDigits.find(ch);
    if (digit == std::string_view::npos) {
      return {};
    }
    value = (value << 4) | digit;
  }
  return ObjectId{value};
}

duckdb::LogicalDependencyList DependencyList(std::span<const ObjectId> ids) {
  duckdb::LogicalDependencyList out;
  for (const auto id : ids) {
    // PUBLIC (id 0) and an unset id name no droppable object.
    if (id.isSet()) {
      out.AddDependency(duckdb::LogicalDependency{nullptr, DependencyInfo(id),
                                                  duckdb::Identifier{}});
    }
  }
  return out;
}

duckdb::LogicalDependencyList EntryDependencies(
  const duckdb::CreateInfo& info, const catalog::Permissions& perm) {
  std::vector<ObjectId> roles{catalog::OwnerOf(perm)};
  for (const auto& item : perm.acl) {
    roles.push_back(catalog::GranteeOf(item));
    roles.push_back(catalog::GrantorOf(item));
  }
  auto out = DependencyList(roles);
  // What the body resolved to when this version was written, which duckdb
  // carries on the info itself and every record therefore round-trips.
  for (const auto& dep : info.dependencies.Set()) {
    out.AddDependency(dep);
  }
  return out;
}

void SetEntryDependencies(duckdb::ClientContext* context,
                          duckdb::Catalog& owner, ObjectId id,
                          const duckdb::LogicalDependencyList& deps) try {
  if (auto manager = owner.GetDependencyManager()) {
    manager->ReplaceSubjects(EdgeTransaction(context, owner),
                             DependencyInfo(id), deps);
  }
} catch (const duckdb::TransactionException& e) {
  (void)e;
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent DDL on a dependency "
            "of the same object"));
}

DependencyView::DependencyView(duckdb::ClientContext* context) {
  auto& instance = DuckDBEngine::Instance().instance();
  for (auto& attached : duckdb::DatabaseManager::Get(instance).GetDatabases()) {
    if (attached->IsClosed()) {
      continue;
    }
    auto& duck_catalog = attached->GetCatalog();
    const auto type = duck_catalog.GetCatalogType();
    const bool global = type == kGlobalStorageType;
    if (type != kSereneDBCatalogType && !global) {
      continue;
    }
    auto manager = duck_catalog.GetDependencyManager();
    if (!manager) {
      continue;
    }
    const auto transaction = EdgeTransaction(context, duck_catalog);
    _attachments.push_back({&duck_catalog, manager.get(), transaction});
    if (!global) {
      continue;
    }
    auto& global_catalog = duck_catalog.Cast<SereneDBGlobalCatalog>();
    const auto take = [&](duckdb::CatalogSet& set) {
      set.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
        _global_kinds.emplace(catalog::IdOf(entry), entry.type);
      });
    };
    take(global_catalog.GetRoleSet());
    take(global_catalog.GetDatabaseSet());
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> DependencyView::Resolve(
  ObjectId id) const {
  const auto info = DependencyInfo(id);
  for (const auto& attachment : _attachments) {
    if (attachment.catalog->GetCatalogType() == kGlobalStorageType) {
      continue;
    }
    if (auto entry = attachment.catalog->GetDependencyEntry(
          attachment.transaction, info)) {
      return entry;
    }
  }
  return nullptr;
}

std::vector<ObjectId> DependencyView::DependentIds(ObjectId referenced) const {
  std::vector<ObjectId> ids;
  const auto info = DependencyInfo(referenced);
  for (const auto& attachment : _attachments) {
    // Collected, not resolved: resolving a dependent opens the set holding it,
    // and the scan is inside one already.
    attachment.manager->ScanDependents(
      attachment.transaction, info, [&](duckdb::DependencyEntry& dep) {
        if (const auto id = DependencyInfoId(dep.EntryInfo()); id.isSet()) {
          ids.push_back(id);
        }
      });
  }
  std::ranges::sort(ids);
  ids.erase(std::ranges::unique(ids).begin(), ids.end());
  return ids;
}

std::vector<Dependent> DependencyView::Dependents(ObjectId referenced) const {
  std::vector<Dependent> out;
  for (const auto id : DependentIds(referenced)) {
    if (const auto kind = _global_kinds.find(id); kind != _global_kinds.end()) {
      out.push_back({id, kind->second, nullptr});
      continue;
    }
    if (auto entry = Resolve(id)) {
      out.push_back({id, entry->type, entry.get()});
    }
  }
  return out;
}

std::size_t DependencyView::CountDependents(ObjectId referenced) const {
  // The recorded dependents, without resolving any of them. Exact because a
  // dropped entry retires its own outgoing edges, and cheaper -- but also the
  // only correct answer for a schema, which has no id index to be resolved
  // through and would otherwise not count as a dependent of the role owning it.
  return DependentIds(referenced).size();
}

void VisitAllEdges(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(ObjectId referenced, ObjectId dependent)> visitor) {
  auto& instance = DuckDBEngine::Instance().instance();
  std::vector<std::pair<ObjectId, ObjectId>> edges;
  for (auto& attached :
       duckdb::DatabaseManager::Get(instance).GetDatabases(context)) {
    if (attached->IsClosed()) {
      continue;
    }
    auto& duck_catalog = attached->GetCatalog();
    const auto type = duck_catalog.GetCatalogType();
    if (type != kSereneDBCatalogType && type != kGlobalStorageType) {
      continue;
    }
    auto manager = duck_catalog.GetDependencyManager();
    if (!manager) {
      continue;
    }
    manager->ScanAllEdges(
      EdgeTransaction(&context, duck_catalog),
      [&](duckdb::DependencyEntry& dep) {
        const auto referenced = DependencyInfoId(dep.SourceInfo());
        const auto dependent = DependencyInfoId(dep.EntryInfo());
        if (referenced.isSet() && dependent.isSet()) {
          edges.emplace_back(referenced, dependent);
        }
      });
  }
  for (const auto& [referenced, dependent] : edges) {
    visitor(referenced, dependent);
  }
}

}  // namespace sdb::catalog
