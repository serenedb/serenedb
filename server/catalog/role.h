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

#include <duckdb/parser/parsed_data/create_info.hpp>
#include <limits>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "basics/bit_utils.hpp"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

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

// One role, as the catalog holds it: the record a mutator authors, the log
// writes and SereneDBRoleEntry is built from. Roles are cluster-wide and duckdb
// has no counterpart, so CatalogType gained ROLE_ENTRY. A role carries no
// schema, conflict mode or SQL text, so the fields below are all there is to
// one -- the entry that holds this record is the role.
class CreateRoleInfo final : public duckdb::CreateInfo {
 public:
  CreateRoleInfo();
  CreateRoleInfo(ObjectId id, persistence::RoleData data);

  persistence::RoleData ToData() const;
  // The role's own fields and none of CreateInfo's: the catalog log reads it
  // back through Deserialize below rather than through CreateInfo's type
  // switch.
  void SerializePayload(duckdb::Serializer& sink) const;
  void Serialize(duckdb::Serializer& sink) const final;
  std::string ToString() const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  duckdb::unique_ptr<CreateRoleInfo> CopyRecord() const;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return _id; }
  void SetId(ObjectId id) noexcept;

  std::string_view GetName() const noexcept { return _name; }
  void SetRoleName(std::string_view name);

  RoleOption Options() const noexcept { return _options; }
  bool Has(RoleOption o) const noexcept {
    return (_options & o) != RoleOption::None;
  }
  bool IsSuperuser() const noexcept { return Has(RoleOption::Superuser); }
  bool CanLogin() const noexcept { return Has(RoleOption::Login); }
  void SetOptions(RoleOption o) noexcept { _options = o; }

  static constexpr int32_t kNoConnLimit = -1;
  int32_t ConnLimit() const noexcept { return _conn_limit; }
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
  void ChangeDefaultAcl(ObjectId schema, char objtype, duckdb::CatalogType type,
                        absl::FunctionRef<void(Acl&)> mutate);

  std::span<const Membership> MemberOf() const noexcept { return _member_of; }

  void AddMembership(const Membership& edge);
  void RemoveMembership(ObjectId role);

  std::string_view Password() const noexcept { return _password; }
  void SetPassword(std::string password) { _password = std::move(password); }

 private:
  RoleOption _options = RoleOption::None;
  std::vector<Membership> _member_of;
  int32_t _conn_limit = kNoConnLimit;
  int64_t _valid_until = kNoValidUntil;
  std::vector<std::string> _config;
  std::vector<DefaultAcl> _default_acls;
  std::string _password;
  std::string _name;
  ObjectId _id;
};

}  // namespace sdb::catalog
