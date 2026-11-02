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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/role_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/parser/parsed_data/create_role_info.hpp>
#include <span>
#include <string>
#include <string_view>

#include "pg/catalog/oids.h"

namespace sdb::catalog {

using RoleOption = duckdb::RoleOption;
using duckdb::HasOption;

struct PredefinedRole {
  duckdb::idx_t oid;
  std::string_view name;
  duckdb::RoleCapability capability;
  std::span<const duckdb::idx_t> member_of;
};

inline constexpr duckdb::idx_t kPgMonitorMemberOf[] = {3374, 3375, 3377};
inline constexpr duckdb::idx_t kPgDatabaseOwner = 6171;

// PostgreSQL's predefined roles, with its oids; pg_monitor comes after the
// roles it is a member of.
inline constexpr PredefinedRole kPredefinedRoles[] = {
  {3374, "pg_read_all_settings", duckdb::RoleCapability::ReadAllSettings, {}},
  {3375, "pg_read_all_stats", duckdb::RoleCapability::ReadAllStats, {}},
  {3377, "pg_stat_scan_tables", duckdb::RoleCapability::None, {}},
  {3373, "pg_monitor", duckdb::RoleCapability::None, kPgMonitorMemberOf},
  {4200, "pg_signal_backend", duckdb::RoleCapability::SignalBackend, {}},
  {4544, "pg_checkpoint", duckdb::RoleCapability::Checkpoint, {}},
  {4550, "pg_use_reserved_connections", duckdb::RoleCapability::None, {}},
  {kPgDatabaseOwner, "pg_database_owner", duckdb::RoleCapability::None, {}},
  {4569, "pg_read_server_files", duckdb::RoleCapability::ReadServerFiles, {}},
  {4570, "pg_write_server_files", duckdb::RoleCapability::WriteServerFiles, {}},
  {4571, "pg_execute_server_program", duckdb::RoleCapability::None, {}},
  {6181, "pg_read_all_data", duckdb::RoleCapability::ReadAllData, {}},
  {6182, "pg_write_all_data", duckdb::RoleCapability::WriteAllData, {}},
  {6304,
   "pg_create_subscription",
   duckdb::RoleCapability::CreateSubscription,
   {}},
  {6337, "pg_maintain", duckdb::RoleCapability::Maintain, {}},
  {6392, "pg_signal_autovacuum_worker", duckdb::RoleCapability::None, {}},
};

constexpr bool IsPredefinedRole(duckdb::idx_t oid) {
  return oid != pg::kInvalidOid && oid < pg::kMinSystem;
}

duckdb::RoleCapability PredefinedCapability(duckdb::idx_t oid);

void RequireUnreservedRoleName(const duckdb::Identifier& name);
std::string StoredPassword(std::string_view password);
duckdb::idx_t GrantorOfMembership(duckdb::ClientContext& context);

class RoleCatalogEntry final : public duckdb::RoleEntry {
 public:
  RoleCatalogEntry(duckdb::Catalog& catalog, duckdb::CreateRoleInfo& info);

  bool CanLogin() const noexcept {
    return HasOption(Options(), RoleOption::Login);
  }

  int32_t ConnLimit() const noexcept { return _conn_limit; }
  int64_t ValidUntil() const noexcept { return _valid_until; }
  bool HasValidUntil() const noexcept { return _valid_until != 0; }

  const std::string& Password() const noexcept { return _password; }
  const duckdb::vector<std::string>& Config() const noexcept { return _config; }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }

 private:
  int32_t _conn_limit;
  int64_t _valid_until;
  std::string _password;
  duckdb::vector<std::string> _config;
};

void ApplyConfigChange(duckdb::vector<std::string>& config,
                       std::span<const std::string> set_config,
                       std::span<const std::string> reset_config,
                       bool reset_all_config);

}  // namespace sdb::catalog
