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

#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

namespace sdb::catalog {

template<typename T>
class Derived final {
 public:
  template<typename Build>
  const T& Get(Build&& build) const {
    std::call_once(_once, [&] {
      T value;
      build(value);
      _value = std::move(value);
    });
    return _value;
  }

 private:
  mutable std::once_flag _once;
  mutable T _value;
};

struct CatalogKey {
  duckdb::TableCatalogEntry* table = nullptr;
  const duckdb::UniqueConstraint* constraint = nullptr;
};

struct CatalogEdge {
  duckdb::CatalogEntry* subject;
  duckdb::DependencyDependentFlags flags;

  bool operator==(const CatalogEdge&) const = default;
};

struct CatalogSnapshot {
  std::vector<duckdb::CatalogEntry*> entries;
  Derived<irs::containers::FlatHashMap<std::string_view, CatalogKey>> keys;
  Derived<irs::containers::FlatHashSet<duckdb::idx_t>> indexed;
  Derived<irs::containers::FlatHashSet<duckdb::idx_t>> triggered;
  Derived<irs::containers::FlatHashMap<const duckdb::CatalogEntry*,
                                       std::vector<CatalogEdge>>>
    subjects;
};

class CatalogSnapshots final {
 public:
  explicit CatalogSnapshots(duckdb::DuckCatalog& catalog) : _catalog{catalog} {}

  std::shared_ptr<const CatalogSnapshot> Get(duckdb::ClientContext& context,
                                             duckdb::CatalogSet& set);

 private:
  duckdb::DuckCatalog& _catalog;
  absl::Mutex _mutex;
  duckdb::idx_t _version = 0;
  irs::containers::FlatHashMap<const duckdb::CatalogSet*,
                               std::shared_ptr<const CatalogSnapshot>>
    _snapshots;
};

}  // namespace sdb::catalog
