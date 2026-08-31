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
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <string>
#include <vector>

namespace sdb::catalog {

enum class RoleOption : uint32_t {
  None = 0,
  Superuser = 1U << 0U,
  Inherit = 1U << 1U,
  CreateRole = 1U << 2U,
  CreateDb = 1U << 3U,
  Login = 1U << 4U,
  Replication = 1U << 5U,
  BypassRls = 1U << 6U,
};

constexpr RoleOption operator|(RoleOption lhs, RoleOption rhs) noexcept {
  return static_cast<RoleOption>(static_cast<uint32_t>(lhs) |
                                 static_cast<uint32_t>(rhs));
}

constexpr RoleOption operator&(RoleOption lhs, RoleOption rhs) noexcept {
  return static_cast<RoleOption>(static_cast<uint32_t>(lhs) &
                                 static_cast<uint32_t>(rhs));
}

constexpr bool HasOption(RoleOption options, RoleOption option) noexcept {
  return (options & option) == option;
}

struct Membership {
  duckdb::idx_t role{0};
  bool admin_option{false};
  bool inherit_option{true};
  bool set_option{true};

  bool operator==(const Membership& rhs) const noexcept = default;
};

// A role is cluster-wide. In this reference implementation it carries no
// privileges and nothing checks any: owner, ACLs and default privileges are
// the RBAC phase. Existence and the membership graph are needed now, because
// login and pg_auth_members read them.
class CreateRoleInfo final : public duckdb::CreateInfo {
 public:
  static constexpr int32_t kNoConnLimit = -1;
  static constexpr int64_t kNoValidUntil = 0;

  CreateRoleInfo() : duckdb::CreateInfo{duckdb::CatalogType::ROLE_ENTRY} {}

  RoleOption options{RoleOption::Inherit};
  int32_t conn_limit{kNoConnLimit};
  int64_t valid_until{kNoValidUntil};
  std::string password;
  std::vector<Membership> member_of;
  std::vector<std::string> config;

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::string ToString() const final;
};

class RoleCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type = duckdb::CatalogType::ROLE_ENTRY;
  static constexpr const char* Name = "role";

  RoleCatalogEntry(duckdb::Catalog& catalog, CreateRoleInfo& info);

  RoleOption Options() const noexcept { return _options; }
  bool CanLogin() const noexcept {
    return HasOption(_options, RoleOption::Login);
  }
  bool IsSuperuser() const noexcept {
    return HasOption(_options, RoleOption::Superuser);
  }

  int32_t ConnLimit() const noexcept { return _conn_limit; }
  int64_t ValidUntil() const noexcept { return _valid_until; }
  bool HasValidUntil() const noexcept {
    return _valid_until != CreateRoleInfo::kNoValidUntil;
  }

  const std::string& Password() const noexcept { return _password; }
  const std::vector<Membership>& MemberOf() const noexcept {
    return _member_of;
  }
  const std::vector<std::string>& Config() const noexcept { return _config; }

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) override;
  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

 private:
  RoleOption _options;
  int32_t _conn_limit;
  int64_t _valid_until;
  std::string _password;
  std::vector<Membership> _member_of;
  std::vector<std::string> _config;
};

}  // namespace sdb::catalog
