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

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <string_view>
#include <vector>

#include "catalog1/permissions.h"
#include "pg/pg_types.h"

namespace sdb::pg {

struct MaterializedData {
  std::vector<duckdb::Vector> columns;
  duckdb::idx_t size = 0;
};

inline constexpr std::array kSystemTableAcl{catalog::AclItem{
  .grantee = kPublicGrantee, .privs = catalog::AclMode::Select}};

class VirtualTable {
 public:
  constexpr VirtualTable() noexcept = default;

  VirtualTable(const VirtualTable&) = delete;
  VirtualTable& operator=(const VirtualTable&) = delete;

  constexpr virtual ~VirtualTable() = default;

  duckdb::idx_t Id() const noexcept { return _id; }
  std::string_view GetName() const noexcept { return _name; }
  catalog::AclView GetAcl() const noexcept { return _acl; }

  virtual duckdb::LogicalType RowType() const noexcept = 0;

  virtual MaterializedData Materialize(
    duckdb::Catalog& database, duckdb::ClientContext& context) const = 0;

 protected:
  duckdb::idx_t _id = 0;
  std::string_view _name;
  catalog::AclView _acl = kSystemTableAcl;
};

}  // namespace sdb::pg
