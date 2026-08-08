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

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <memory>
#include <optional>

#include "catalog/duckdb_entry.h"
#include "catalog/entry.h"
#include "catalog/view.h"

namespace sdb::catalog {

// The bound entry for a SereneDB view, holding the SereneDB view object it was
// resolved from. Mirrors SereneDBTableEntry for tables: it lets the plan-level
// access-control rule read the view's identity, owner and ACL straight off the
// bound entry (a pointer hand-off) instead of resolving the view again by name.
class SereneDBViewEntry final : public duckdb::ViewCatalogEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::TABLE_ENTRY;

 public:
  SereneDBViewEntry(duckdb::Catalog& catalog,
                    duckdb::SchemaCatalogEntry& schema,
                    duckdb::CreateViewInfo& info, catalog::Permissions perm)
    : duckdb::ViewCatalogEntry(catalog, schema, info) {
    catalog::AdoptEntryIdentity(*this, ObjectId{info.oid}, std::move(perm));
  }
};

}  // namespace sdb::catalog
