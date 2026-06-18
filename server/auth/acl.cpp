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
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <utility>
#include <vector>

#include "basics/containers/trivial_map.h"
#include "basics/exceptions.h"

namespace sdb::auth {
namespace {

using catalog::AclItem;
using catalog::AclMode;
using catalog::ObjectType;

// In canonical PG print order.
constexpr containers::TrivialBiMap kPrivNames{[](auto selector) {
  return selector()
    .Case("select", AclMode::Select)
    .Case("insert", AclMode::Insert)
    .Case("update", AclMode::Update)
    .Case("delete", AclMode::Delete)
    .Case("truncate", AclMode::Truncate)
    .Case("references", AclMode::References)
    .Case("trigger", AclMode::Trigger)
    .Case("maintain", AclMode::Maintain)
    .Case("execute", AclMode::Execute)
    .Case("usage", AclMode::Usage)
    .Case("create", AclMode::Create)
    .Case("temporary", AclMode::CreateTemp)
    .Case("connect", AclMode::Connect);
}};

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
    default:
      return AclMode::NoRights;
  }
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
    default:
      return AclMode::NoRights;
  }
}

bool RolesContain(RoleIdSpan roles, ObjectId id) noexcept {
  return std::ranges::binary_search(roles, id);
}

}  // namespace

AclMode OwnerPrivs(ObjectType type) noexcept { return ClassPrivs(type); }

catalog::Acl AclDefault(ObjectType type, ObjectId owner) {
  catalog::Acl acl;
  // Owner self-grant with NO stored grant options: PG's acldefault stores
  // goptions = ACL_NO_RIGHTS (owner grantability is implicit by ownership),
  // so owner rows print without '*'.
  const AclMode owner_privs = OwnerPrivs(type);
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

catalog::Acl AclEffective(catalog::AclView stored, ObjectType type,
                          ObjectId owner) {
  if (stored.empty()) {
    return AclDefault(type, owner);
  }
  return catalog::Acl{stored.begin(), stored.end()};
}

bool AclCheck(catalog::AclView acl, const RoleIdSet& roles, AclMode need) {
  AclMode have = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantee != catalog::kPublicGrantee &&
        !roles.contains(item.grantee)) {
      continue;
    }
    have |= item.privs;
    if (Has(have, need)) {
      return true;
    }
  }
  return false;
}

bool AclCheckSorted(catalog::AclView stored, ObjectType type, ObjectId owner,
                    RoleIdSpan roles, AclMode need, bool any_of) {
  if (need == AclMode::NoRights) {
    return false;
  }
  const auto done = [&](AclMode have) {
    return any_of ? (have & need) != AclMode::NoRights : Has(have, need);
  };

  if (stored.empty()) {
    // NULL acl -> acldefault, evaluated inline.
    AclMode have = AclMode::NoRights;
    if (RolesContain(roles, owner)) {
      have |= OwnerPrivs(type);
      if (done(have)) {
        return true;
      }
    }
    have |= PublicDefaultPrivs(type);
    return done(have);
  }

  AclMode have = AclMode::NoRights;
  for (const auto& item : stored) {
    if (item.grantee != catalog::kPublicGrantee &&
        !RolesContain(roles, item.grantee)) {
      continue;
    }
    have |= item.privs;
    if (done(have)) {
      return true;
    }
  }
  return false;
}

AclMode AclGrantOptionHeld(catalog::AclView acl, const RoleIdSet& roles) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantee == catalog::kPublicGrantee ||
        roles.contains(item.grantee)) {
      held |= item.grant_option;
    }
  }
  return held;
}

AclMode AclPrivsHeld(catalog::AclView acl, const RoleIdSet& roles) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantee == catalog::kPublicGrantee ||
        roles.contains(item.grantee)) {
      held |= item.privs;
    }
  }
  return held;
}

void AclGrant(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
              AclMode privs, AclMode grant_option) {
  for (auto& item : acl) {
    if (item.grantee == grantee && item.grantor == grantor) {
      item.privs |= privs;
      item.grant_option |= (grant_option & privs);
      return;
    }
  }
  acl.push_back(AclItem{
    .grantee = grantee,
    .grantor = grantor,
    .privs = privs,
    .grant_option = grant_option & privs,
  });
}

void AclRevoke(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
               AclMode privs) {
  for (auto it = acl.begin(); it != acl.end(); ++it) {
    if (it->grantee != grantee || it->grantor != grantor) {
      continue;
    }
    it->privs &= ~privs;
    it->grant_option &= ~privs;
    if (it->privs == AclMode::NoRights) {
      acl.erase(it);
    }
    return;
  }
  // No matching (grantee, grantor): silent no-op, matching PostgreSQL.
}

void AclRemoveGrantOption(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                          AclMode privs) {
  for (auto& item : acl) {
    if (item.grantee == grantee && item.grantor == grantor) {
      item.grant_option &= ~privs;
      return;
    }
  }
  // No matching (grantee, grantor): silent no-op, matching PostgreSQL.
}

AclMode AclDependentPrivs(catalog::AclView acl, ObjectId grantee,
                          AclMode privs) {
  AclMode dependent = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantor == grantee) {
      dependent |= item.privs & privs;
    }
  }
  return dependent;
}

void AclRevokeCascade(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                      AclMode privs) {
  // (revokee, revoked-bits) worklist. Revoking from `grantee` may strip
  // privileges that `grantee` re-granted; those revokees then cascade in turn.
  std::vector<std::pair<ObjectId, AclMode>> work{{grantee, privs}};
  while (!work.empty()) {
    const auto [who, bits] = work.back();
    work.pop_back();
    // Any item whose grantor == `who` and which carries a revoked bit must lose
    // that privilege (its grant chain is gone); schedule its grantee next.
    for (const auto& item : acl) {
      if (item.grantor != who) {
        continue;
      }
      const AclMode dependent = item.privs & bits;
      if (dependent != AclMode::NoRights) {
        work.emplace_back(item.grantee, dependent);
      }
    }
    // Revoke `bits` from every item granted BY `who` (i.e. grantor == who) and,
    // for the top of the chain, also from (grantee, grantor).
    for (auto it = acl.begin(); it != acl.end();) {
      const bool top = it->grantee == grantee && it->grantor == grantor &&
                       who == grantee;
      if (it->grantor == who || top) {
        it->privs &= ~bits;
        it->grant_option &= ~bits;
        if (it->privs == AclMode::NoRights) {
          it = acl.erase(it);
          continue;
        }
      }
      ++it;
    }
  }
}

AclMode ParseAclMode(std::string_view privileges, ObjectType type) {
  const AclMode allowed = ClassPrivs(type);
  AclMode out = AclMode::NoRights;
  for (std::string_view tok :
       absl::StrSplit(privileges, ',', absl::SkipEmpty())) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (stripped.empty()) {
      continue;
    }
    if (absl::EqualsIgnoreCase(stripped, "ALL")) {
      out |= allowed;
      continue;
    }
    // PG accepts TEMP as a synonym for TEMPORARY.
    const auto lookup =
      absl::EqualsIgnoreCase(stripped, "TEMP") ? std::string_view{"temporary"}
                                               : stripped;
    const auto mode = kPrivNames.TryFindICaseByFirst(lookup);
    if (!mode) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER, "unrecognized privilege type: \"",
                tok, "\"");
    }
    if ((allowed & *mode) != *mode) {
      // PG names the GRANT object class: a plain relation target is "relation",
      // others use their object keyword.
      const auto object_word = [type] -> std::string_view {
        switch (type) {
          case ObjectType::Sequence:
            return "sequence";
          case ObjectType::PgSqlFunction:
            return "function";
          case ObjectType::PgSqlType:
            return "type";
          case ObjectType::Schema:
            return "schema";
          case ObjectType::Database:
            return "database";
          default:
            return "relation";
        }
      }();
      SDB_THROW(sdb::ERROR_BAD_PARAMETER, "invalid privilege type ", stripped,
                " for ", object_word);
    }
    out |= *mode;
  }
  return out;
}

namespace {

// PG aclitem privilege characters, in postgres' canonical print order. Mirrors
// the ACL_*_CHR table in src/include/utils/acl.h.
struct PrivChar {
  AclMode mode;
  char chr;
};
constexpr std::array kPrivChars{
  PrivChar{AclMode::Insert, 'a'},     PrivChar{AclMode::Select, 'r'},
  PrivChar{AclMode::Update, 'w'},     PrivChar{AclMode::Delete, 'd'},
  PrivChar{AclMode::Truncate, 'D'},   PrivChar{AclMode::References, 'x'},
  PrivChar{AclMode::Trigger, 't'},    PrivChar{AclMode::Maintain, 'm'},
  PrivChar{AclMode::Execute, 'X'},    PrivChar{AclMode::Usage, 'U'},
  PrivChar{AclMode::Create, 'C'},     PrivChar{AclMode::CreateTemp, 'T'},
  PrivChar{AclMode::Connect, 'c'},    PrivChar{AclMode::Set, 's'},
  PrivChar{AclMode::AlterSystem, 'A'},
};

// PG putid() (src/backend/utils/adt/acl.c): append a role name to an aclitem
// string, double-quoting it when any character is not ASCII alphanumeric or
// '_' (high-bit-set chars always force quotes), and doubling embedded '"'.
void PutId(std::string& out, std::string_view name) {
  const bool safe = std::ranges::all_of(name, [](unsigned char c) {
    return !(c & 0x80) && (absl::ascii_isalnum(c) || c == '_');
  });
  if (safe) {
    out.append(name);
    return;
  }
  out.push_back('"');
  for (char c : name) {
    if (c == '"') {
      out.push_back('"');
    }
    out.push_back(c);
  }
  out.push_back('"');
}

}  // namespace

std::string AclItemToText(
  const AclItem& item, absl::FunctionRef<std::string_view(ObjectId)> name_of) {
  std::string out;
  // grantee ("" for PUBLIC).
  if (item.grantee != catalog::kPublicGrantee) {
    PutId(out, name_of(item.grantee));
  }
  out.push_back('=');
  // privilege chars, each optionally followed by '*' for the grant option.
  for (const auto& p : kPrivChars) {
    if ((item.privs & p.mode) == p.mode) {
      out.push_back(p.chr);
      if ((item.grant_option & p.mode) == p.mode) {
        out.push_back('*');
      }
    }
  }
  out.push_back('/');
  PutId(out, name_of(item.grantor));
  return out;
}

std::string AclModeToString(AclMode mode) {
  // Canonical PG order, uppercase verb spelling. Multi-bit masks join with '/'
  // (e.g. MERGE requires INSERT/UPDATE/DELETE).
  std::string out;
  for (const auto& p : kPrivChars) {
    if ((mode & p.mode) != p.mode) {
      continue;
    }
    if (auto name = kPrivNames.TryFindBySecond(p.mode)) {
      if (!out.empty()) {
        out.push_back('/');
      }
      absl::StrAppend(&out, absl::AsciiStrToUpper(*name));
    }
  }
  return out;
}

std::vector<std::string> AclToText(
  catalog::AclView acl, absl::FunctionRef<std::string_view(ObjectId)> name_of) {
  std::vector<std::string> out;
  out.reserve(acl.size());
  for (const auto& item : acl) {
    out.push_back(AclItemToText(item, name_of));
  }
  return out;
}

}  // namespace sdb::auth
