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
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <optional>
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

constexpr RoleOption operator~(RoleOption value) noexcept {
  return static_cast<RoleOption>(~static_cast<uint32_t>(value));
}

struct Membership {
  duckdb::idx_t role{0};
  duckdb::idx_t grantor{0};
  bool admin_option{false};
  bool inherit_option{true};
  bool set_option{true};

  bool operator==(const Membership& rhs) const noexcept = default;
};

class CreateRoleInfo final : public duckdb::CreateInfo {
 public:
  static constexpr int32_t kNoConnLimit = -1;
  static constexpr int64_t kNoValidUntil = 0;

  CreateRoleInfo() : duckdb::CreateInfo{duckdb::CatalogType::ROLE_ENTRY} {}

  duckdb::idx_t oid{0};
  RoleOption options{RoleOption::Inherit};
  int32_t conn_limit{kNoConnLimit};
  int64_t valid_until{kNoValidUntil};
  std::string password;
  std::vector<Membership> member_of;
  std::vector<std::string> config;

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::string ToString() const final;
};

class AlterRoleInfo final : public duckdb::AlterInfo {
 public:
  AlterRoleInfo() : duckdb::AlterInfo{duckdb::AlterType::ALTER_ROLE} {}
  explicit AlterRoleInfo(duckdb::Identifier name);

  RoleOption set_options{RoleOption::None};
  RoleOption clear_options{RoleOption::None};
  std::optional<std::string> password;
  std::optional<int32_t> conn_limit;
  std::optional<int64_t> valid_until;
  std::optional<duckdb::Identifier> new_name;
  bool reset_all_config{false};
  std::vector<std::string> reset_config;
  std::vector<std::string> set_config;
  std::vector<Membership> upsert_member_of;
  std::vector<duckdb::idx_t> remove_member_of;

  duckdb::CatalogType GetCatalogType() const final {
    return duckdb::CatalogType::ROLE_ENTRY;
  }
  duckdb::unique_ptr<duckdb::AlterInfo> Copy() const final;
  std::string ToString() const final;
  void Serialize(duckdb::Serializer& serializer) const final;
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

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) override;
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
