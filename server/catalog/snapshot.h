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

#include <absl/base/call_once.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace duckdb {

class TableCatalogEntry;
class UniqueConstraint;

}  // namespace duckdb
namespace sdb::catalog {

template<typename T>
class Derived final {
 public:
  template<typename Build>
  const T& Get(Build&& build) const {
    absl::call_once(_once, [&] { build(_value); });
    return _value;
  }

 private:
  mutable absl::once_flag _once;
  mutable T _value;
};

struct CatalogSnapshot {
  std::vector<duckdb::CatalogEntry*> entries;
  Derived<irs::containers::FlatHashMap<
    std::string_view,
    std::pair<duckdb::TableCatalogEntry*, const duckdb::UniqueConstraint*>>>
    keys;
  Derived<irs::containers::FlatHashSet<duckdb::idx_t>> indexed;
  Derived<irs::containers::FlatHashSet<duckdb::idx_t>> triggered;
  Derived<irs::containers::FlatHashMap<
    const duckdb::CatalogEntry*,
    std::vector<
      std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>>>
    subjects;
};

class SnapshotCatalog : public duckdb::DuckCatalog {
 public:
  using duckdb::DuckCatalog::DuckCatalog;

  std::shared_ptr<const CatalogSnapshot> Snapshot(
    duckdb::ClientContext& context, duckdb::CatalogSet& set);

 private:
  absl::Mutex _snapshots_mutex;
  duckdb::idx_t _snapshots_version = 0;
  irs::containers::FlatHashMap<const duckdb::CatalogSet*,
                               std::shared_ptr<const CatalogSnapshot>>
    _snapshots;
};

}  // namespace sdb::catalog
