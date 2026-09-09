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
#include <duckdb/parser/parsed_data/create_role_info.hpp>
#include <string>

namespace sdb::catalog {

using RoleOption = duckdb::RoleOption;
using duckdb::HasOption;

class RoleCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type = duckdb::CatalogType::ROLE_ENTRY;
  static constexpr const char* Name = "role";

  RoleCatalogEntry(duckdb::Catalog& catalog, duckdb::CreateRoleInfo& info);

  RoleOption Options() const noexcept { return _options; }
  bool CanLogin() const noexcept {
    return HasOption(_options, RoleOption::Login);
  }
  bool IsSuperuser() const noexcept {
    return HasOption(_options, RoleOption::Superuser);
  }

  int32_t ConnLimit() const noexcept { return _conn_limit; }
  int64_t ValidUntil() const noexcept { return _valid_until; }
  bool HasValidUntil() const noexcept { return _valid_until != 0; }

  const std::string& Password() const noexcept { return _password; }
  const duckdb::vector<duckdb::Membership>& MemberOf() const noexcept {
    return _member_of;
  }
  const duckdb::vector<std::string>& Config() const noexcept { return _config; }

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
  duckdb::vector<duckdb::Membership> _member_of;
  duckdb::vector<std::string> _config;
};

}  // namespace sdb::catalog
