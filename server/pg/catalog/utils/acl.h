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

#include <array>
#include <duckdb/catalog/permissions.hpp>
#include <string>
#include <string_view>

#include "pg/catalog/utils/oids.h"

namespace duckdb {

class CatalogEntry;
class ClientContext;

}  // namespace duckdb
namespace sdb {
namespace auth {

struct RoleGraph;

}  // namespace auth
namespace pg {

struct AclPrivilege {
  duckdb::AclMode mode;
  char chr;
  std::string_view name;
};
inline constexpr std::array kAclPrivileges{
  AclPrivilege{duckdb::AclMode::Insert, 'a', "INSERT"},
  AclPrivilege{duckdb::AclMode::Select, 'r', "SELECT"},
  AclPrivilege{duckdb::AclMode::Update, 'w', "UPDATE"},
  AclPrivilege{duckdb::AclMode::Delete, 'd', "DELETE"},
  AclPrivilege{duckdb::AclMode::Truncate, 'D', "TRUNCATE"},
  AclPrivilege{duckdb::AclMode::References, 'x', "REFERENCES"},
  AclPrivilege{duckdb::AclMode::Trigger, 't', "TRIGGER"},
  AclPrivilege{duckdb::AclMode::Execute, 'X', "EXECUTE"},
  AclPrivilege{duckdb::AclMode::Usage, 'U', "USAGE"},
  AclPrivilege{duckdb::AclMode::Create, 'C', "CREATE"},
  AclPrivilege{duckdb::AclMode::CreateTemp, 'T', "TEMPORARY"},
  AclPrivilege{duckdb::AclMode::Connect, 'c', "CONNECT"},
  AclPrivilege{duckdb::AclMode::Set, 's', "SET"},
  AclPrivilege{duckdb::AclMode::AlterSystem, 'A', "ALTER SYSTEM"},
  AclPrivilege{duckdb::AclMode::Maintain, 'm', "MAINTAIN"},
};

inline constexpr std::array kForeignDataWrapperAcl{duckdb::AclItem{
  .grantee = kPublicGrantee,
  .grantor = kRootUser,
  .privs = duckdb::AclMode::Usage,
}};

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const auth::RoleGraph& roles);

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor);

}  // namespace pg
}  // namespace sdb
