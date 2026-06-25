////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/function_ref.h>

#include <set>
#include <span>
#include <string>
#include <vector>

#include "basics/bit_utils.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/persistence/role.h"

namespace sdb::catalog {

using persistence::RoleData;

// One membership edge with its PG16 per-edge options.
using Membership = persistence::MembershipData;

// pg_authid role attributes; checked directly, never inherited via membership.
enum class RoleOption : uint32_t {
  None = 0,
  Superuser = 1U << 0,
  Login = 1U << 1,
  Inherit = 1U << 2,
  CreateDb = 1U << 3,
  CreateRole = 1U << 4,
  Replication = 1U << 5,
  BypassRls = 1U << 6,
  All = Superuser | Login | Inherit | CreateDb | CreateRole | Replication |
        BypassRls,
};

ENABLE_BITMASK_ENUM(RoleOption);

class Role final : public catalog::Object {
 public:
  explicit Role(RoleData data);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  static std::shared_ptr<Role> Deserialize(duckdb::Deserializer& src,
                                           ReadContext ctx);

  RoleOption Options() const noexcept { return _options; }
  bool Has(RoleOption o) const noexcept {
    return (_options & o) != RoleOption::None;
  }
  bool IsSuperuser() const noexcept { return Has(RoleOption::Superuser); }
  bool CanLogin() const noexcept { return Has(RoleOption::Login) && _active; }
  void SetOptions(RoleOption o) noexcept { _options = o; }

  // pg_authid attributes surfaced for catalog introspection. CONNECTION LIMIT
  // and VALID UNTIL are unsupported (rejected when set), so these always carry
  // their defaults; the getters exist for pg_authid rendering.
  int32_t ConnLimit() const noexcept { return _conn_limit; }
  std::string_view ValidUntil() const noexcept { return _valid_until; }

  // Per-role GUC settings surfaced as pg_roles.rolconfig ("guc=value" each).
  std::span<const std::string> Config() const noexcept { return _config; }
  void SetConfig(std::string_view guc, std::string_view value);
  void ResetConfig(std::string_view guc);
  void ResetAllConfig() noexcept { _config.clear(); }

  using DefaultAclData = persistence::DefaultAclData;
  std::span<const DefaultAclData> DefaultAcls() const noexcept {
    return _default_acls;
  }
  void ChangeDefaultAcl(ObjectId schema, char objtype, ObjectType type,
                        absl::FunctionRef<void(Acl&)> mutate);

  std::span<const Membership> MemberOf() const noexcept { return _member_of; }

  void AddMembership(const Membership& edge);
  void RemoveMembership(ObjectId role);

 private:
  RoleData BuildData() const;

  bool _active = true;
  RoleOption _options = RoleOption::None;
  std::vector<Membership> _member_of;
  int32_t _conn_limit = -1;
  std::string _valid_until;
  std::vector<std::string> _config;
  std::vector<persistence::DefaultAclData> _default_acls;
};

}  // namespace sdb::catalog
