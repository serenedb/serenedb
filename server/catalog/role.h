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

#include <limits>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "basics/bit_utils.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

struct Membership {
  ObjectId role;
  bool admin_option = false;
  bool inherit_option = true;
  bool set_option = true;
};

struct DefaultAcl {
  ObjectId schema;
  char objtype = 'r';
  Acl acl;
};

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

namespace persistence {

struct RoleData;
}

class Role final : public catalog::Object {
 public:
  explicit Role(persistence::RoleData data);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  static std::shared_ptr<Role> Deserialize(duckdb::Deserializer& src,
                                           ReadContext ctx);

  RoleOption Options() const noexcept { return _options; }
  bool Has(RoleOption o) const noexcept {
    return (_options & o) != RoleOption::None;
  }
  bool IsSuperuser() const noexcept { return Has(RoleOption::Superuser); }
  bool CanLogin() const noexcept { return Has(RoleOption::Login); }
  void SetOptions(RoleOption o) noexcept { _options = o; }

  static constexpr int32_t kNoConnLimit = -1;
  int32_t ConnLimit() const noexcept { return _conn_limit; }
  bool HasConnLimit() const noexcept { return _conn_limit != kNoConnLimit; }
  void SetConnLimit(int32_t limit) noexcept { _conn_limit = limit; }

  static constexpr int64_t kNoValidUntil = std::numeric_limits<int64_t>::min();
  int64_t ValidUntil() const noexcept { return _valid_until; }
  bool HasValidUntil() const noexcept { return _valid_until != kNoValidUntil; }
  void SetValidUntil(int64_t micros) noexcept { _valid_until = micros; }

  std::span<const std::string> Config() const noexcept { return _config; }
  void SetConfig(std::string_view guc, std::string_view value);
  void ResetConfig(std::string_view guc);
  void ResetAllConfig() noexcept { _config.clear(); }

  std::span<const DefaultAcl> DefaultAcls() const noexcept {
    return _default_acls;
  }
  void ChangeDefaultAcl(ObjectId schema, char objtype, ObjectType type,
                        absl::FunctionRef<void(Acl&)> mutate);

  std::span<const Membership> MemberOf() const noexcept { return _member_of; }

  void AddMembership(const Membership& edge);
  void RemoveMembership(ObjectId role);

  std::string_view PasswordVerifier() const noexcept {
    return _password_verifier;
  }
  void SetPasswordVerifier(std::string verifier) {
    _password_verifier = std::move(verifier);
  }

 private:
  RoleOption _options = RoleOption::None;
  std::vector<Membership> _member_of;
  int32_t _conn_limit = kNoConnLimit;
  int64_t _valid_until = kNoValidUntil;
  std::vector<std::string> _config;
  std::vector<DefaultAcl> _default_acls;
  std::string _password_verifier;
};

}  // namespace sdb::catalog
