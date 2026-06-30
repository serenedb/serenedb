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

#include "catalog/view.h"

namespace sdb::connector {

// The bound entry for a SereneDB view, holding the SereneDB view object it was
// resolved from. Mirrors SereneDBTableEntry for tables: it lets the plan-level
// access-control rule read the view's identity, owner and ACL straight off the
// bound entry (a pointer hand-off) instead of resolving the view again by name.
class SereneDBViewEntry final : public duckdb::ViewCatalogEntry {
 public:
  SereneDBViewEntry(duckdb::Catalog& catalog,
                    duckdb::SchemaCatalogEntry& schema,
                    duckdb::CreateViewInfo& info,
                    std::shared_ptr<const catalog::PgSqlView> sdb_view)
    : duckdb::ViewCatalogEntry(catalog, schema, info),
      _sdb_view(std::move(sdb_view)) {}

  const std::shared_ptr<const catalog::PgSqlView>& GetSereneDBView() const {
    return _sdb_view;
  }

 private:
  std::shared_ptr<const catalog::PgSqlView> _sdb_view;
};

}  // namespace sdb::connector
