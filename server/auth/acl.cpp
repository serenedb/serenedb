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

#include "auth/acl.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <optional>
#include <string>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/system-compiler.h"

namespace sdb::auth {
namespace {

using catalog::AclItem;
using catalog::AclMode;
using catalog::ObjectType;

// Lowercase keyword -> AclMode; callers lowercase the input before lookup.
const containers::FlatHashMap<std::string_view, AclMode> kPrivNames{
  {"select", AclMode::Select},     {"insert", AclMode::Insert},
  {"update", AclMode::Update},     {"delete", AclMode::Delete},
  {"truncate", AclMode::Truncate}, {"references", AclMode::References},
  {"trigger", AclMode::Trigger},   {"maintain", AclMode::Maintain},
  {"execute", AclMode::Execute},   {"usage", AclMode::Usage},
  {"create", AclMode::Create},     {"temporary", AclMode::CreateTemp},
  {"temp", AclMode::CreateTemp},   {"connect", AclMode::Connect},
};

AclMode ClassPrivs(ObjectType type) noexcept {
  switch (type) {
    case ObjectType::Table:
      return AclMode::Select | AclMode::Insert | AclMode::Update |
             AclMode::Delete | AclMode::Truncate | AclMode::References |
             AclMode::Trigger | AclMode::Maintain;
    case ObjectType::Sequence:
      return AclMode::Select | AclMode::Update | AclMode::Usage;
    case ObjectType::Database:
      return AclMode::Create | AclMode::CreateTemp | AclMode::Connect;
    case ObjectType::Schema:
      return AclMode::Usage | AclMode::Create;
    case ObjectType::PgSqlFunction:
      return AclMode::Execute;
    case ObjectType::PgSqlType:
      return AclMode::Usage;
    case ObjectType::Invalid:
    case ObjectType::Tombstone:
    case ObjectType::Role:
    case ObjectType::Tokenizer:
    case ObjectType::PgSqlView:
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
    case ObjectType::Policy:
    case ObjectType::RowSecurity:
    case ObjectType::Column:
    case ObjectType::CheckConstraint:
    case ObjectType::Virtual:
      return AclMode::NoRights;
  }
  SDB_UNREACHABLE();
}

bool Has(AclMode have, AclMode need) noexcept {
  return (have & need) == need && need != AclMode::NoRights;
}

AclMode PublicDefaultPrivs(ObjectType type) noexcept {
  switch (type) {
    case ObjectType::Database:
      return AclMode::Connect | AclMode::CreateTemp;
    case ObjectType::PgSqlFunction:
      return AclMode::Execute;
    case ObjectType::PgSqlType:
      return AclMode::Usage;
    case ObjectType::Table:
    case ObjectType::Sequence:
    case ObjectType::Schema:
    case ObjectType::Invalid:
    case ObjectType::Tombstone:
    case ObjectType::Role:
    case ObjectType::Tokenizer:
    case ObjectType::PgSqlView:
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
    case ObjectType::Policy:
    case ObjectType::RowSecurity:
    case ObjectType::Column:
    case ObjectType::CheckConstraint:
    case ObjectType::Virtual:
      return AclMode::NoRights;
  }
  SDB_UNREACHABLE();
}

bool RolesContain(RoleIdSpan roles, ObjectId id) noexcept {
  return std::ranges::binary_search(roles, id);
}

bool IsGranteeInRoles(ObjectId grantee, RoleIdSpan roles) {
  return grantee == catalog::kPublicGrantee || RolesContain(roles, grantee);
}

AclMode AclModeHeld(catalog::AclView acl, RoleIdSpan roles,
                    AclMode AclItem::* field) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (IsGranteeInRoles(item.grantee, roles)) {
      held |= item.*field;
    }
  }
  return held;
}

}  // namespace

catalog::Acl AclDefault(ObjectType type, ObjectId owner) {
  catalog::Acl acl;
  const AclMode owner_privs = ClassPrivs(type);
  if (owner_privs == AclMode::NoRights) {
    return acl;
  }
  acl.push_back(AclItem{
    .grantee = owner,
    .grantor = owner,
    .privs = owner_privs,
  });

  const AclMode public_privs = PublicDefaultPrivs(type);
  if (public_privs != AclMode::NoRights) {
    acl.push_back(AclItem{
      .grantee = catalog::kPublicGrantee,
      .grantor = owner,
      .privs = public_privs,
    });
  }
  return acl;
}

catalog::Acl AclForStorage(catalog::AclView stored, ObjectType type,
                           ObjectId owner) {
  if (stored.empty()) {
    return AclDefault(type, owner);
  }
  return catalog::Acl{stored.begin(), stored.end()};
}

bool AclCheckSorted(catalog::AclView stored, ObjectType type, ObjectId owner,
                    RoleIdSpan roles, AclMode need, PrivMatch match) {
  SDB_ASSERT(std::ranges::is_sorted(roles),
             "AclCheckSorted requires an ascending-sorted roles span");
  if (need == AclMode::NoRights) {
    return false;
  }
  const auto done = [&](AclMode have) {
    return match == PrivMatch::Any ? (have & need) != AclMode::NoRights
                                   : Has(have, need);
  };

  AclMode have = AclMode::NoRights;
  if (RolesContain(roles, owner)) {
    have |= ClassPrivs(type);
    if (done(have)) {
      return true;
    }
  }

  if (stored.empty()) {
    have |= PublicDefaultPrivs(type);
    return done(have);
  }

  for (const auto& item : stored) {
    if (!IsGranteeInRoles(item.grantee, roles)) {
      continue;
    }
    have |= item.privs;
    if (done(have)) {
      return true;
    }
  }
  return false;
}

AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles) {
  return AclModeHeld(acl, roles, &AclItem::grant_option);
}

AclMode AclPrivsHeld(catalog::AclView acl, RoleIdSpan roles) {
  return AclModeHeld(acl, roles, &AclItem::privs);
}

std::optional<AclMode> TryParseAclKeyword(std::string_view keyword,
                                          ObjectType type) {
  const AclMode allowed = ClassPrivs(type);
  if (absl::EqualsIgnoreCase(keyword, "ALL")) {
    return allowed;
  }
  std::string lowered{keyword};
  absl::AsciiStrToLower(&lowered);
  const auto it = kPrivNames.find(lowered);
  if (it == kPrivNames.end() || (allowed & it->second) != it->second) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace sdb::auth
