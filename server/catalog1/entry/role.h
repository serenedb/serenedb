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
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <string>
#include <vector>

namespace sdb::catalog {

using RoleOption = duckdb::RoleOption;
using duckdb::HasOption;

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
