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
#include <array>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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
    // Object classes with no SQL privileges of their own. Views are granted as
    // Table (they share the relation namespace), so PgSqlView never reaches
    // here; indexes derive access from their table; the rest are not ACL
    // objects.
    case ObjectType::Invalid:
    case ObjectType::Tombstone:
    case ObjectType::Role:
    case ObjectType::Tokenizer:
    case ObjectType::PgSqlView:
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
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

auto FindAclItem(catalog::Acl& acl, ObjectId grantee, ObjectId grantor) {
  return std::ranges::find_if(acl, [&](const AclItem& item) {
    return item.grantee == grantee && item.grantor == grantor;
  });
}

AclMode PublicDefaultPrivs(ObjectType type) noexcept {
  switch (type) {
    case ObjectType::Database:
      return AclMode::Connect | AclMode::CreateTemp;
    case ObjectType::PgSqlFunction:
      return AclMode::Execute;
    case ObjectType::PgSqlType:
      return AclMode::Usage;
    // PUBLIC gets no implicit privileges on these; either there is no default
    // grant to PUBLIC (Table/Sequence/Schema) or the class is not an ACL
    // object.
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

}  // namespace

catalog::Acl AclDefault(ObjectType type, ObjectId owner) {
  catalog::Acl acl;
  // Owner self-grant with NO stored grant options: PG's acldefault stores
  // goptions = ACL_NO_RIGHTS (owner grantability is implicit by ownership),
  // so owner rows print without '*'. The owner holds the object class's full
  // privilege set.
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
  // An empty stored ACL means "never touched": the implicit default (owner +
  // PUBLIC) applies. The first GRANT/REVOKE must materialize that default so it
  // becomes explicit, editable rows -- otherwise a REVOKE FROM PUBLIC would
  // collapse back to empty and read as "never touched" again. This mirrors PG's
  // acldefault() materialization; the owner row also keeps the ACL non-NULL
  // after a revoke (PG's relacl is {owner=.../owner}, not NULL).
  if (stored.empty()) {
    return AclDefault(type, owner);
  }
  return catalog::Acl{stored.begin(), stored.end()};
}

bool AclCheckSorted(catalog::AclView stored, ObjectType type, ObjectId owner,
                    RoleIdSpan roles, AclMode need, bool any_of) {
  if (need == AclMode::NoRights) {
    return false;
  }
  const auto done = [&](AclMode have) {
    return any_of ? (have & need) != AclMode::NoRights : Has(have, need);
  };

  // The owner's privileges are derived from ownership, never stored as an ACL
  // row, so they are always the current full class set (and cannot be revoked).
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

AclMode AclGrantOptionHeld(catalog::AclView acl, RoleIdSpan roles) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantee == catalog::kPublicGrantee ||
        std::ranges::binary_search(roles, item.grantee)) {
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

AclMode AclPrivsHeld(catalog::AclView acl, RoleIdSpan roles) {
  AclMode held = AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantee == catalog::kPublicGrantee ||
        std::ranges::binary_search(roles, item.grantee)) {
      held |= item.privs;
    }
  }
  return held;
}

void AclGrant(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
              AclMode privs, AclMode grant_option) {
  if (auto it = FindAclItem(acl, grantee, grantor); it != acl.end()) {
    it->privs |= privs;
    it->grant_option |= (grant_option & privs);
    return;
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
  auto it = FindAclItem(acl, grantee, grantor);
  if (it == acl.end()) {
    return;  // silent no-op, matching PostgreSQL
  }
  it->privs &= ~privs;
  it->grant_option &= ~privs;
  if (it->privs == AclMode::NoRights) {
    acl.erase(it);
  }
}

void AclRemoveGrantOption(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                          AclMode privs) {
  if (auto it = FindAclItem(acl, grantee, grantor); it != acl.end()) {
    it->grant_option &= ~privs;
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
      const bool top =
        it->grantee == grantee && it->grantor == grantor && who == grantee;
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

namespace {

// PG aclitem privilege characters, in postgres' canonical print order. Mirrors
// the ACL_*_CHR table in src/include/utils/acl.h.
struct PrivChar {
  AclMode mode;
  char chr;
};
constexpr std::array kPrivChars{
  PrivChar{AclMode::Insert, 'a'},      PrivChar{AclMode::Select, 'r'},
  PrivChar{AclMode::Update, 'w'},      PrivChar{AclMode::Delete, 'd'},
  PrivChar{AclMode::Truncate, 'D'},    PrivChar{AclMode::References, 'x'},
  PrivChar{AclMode::Trigger, 't'},     PrivChar{AclMode::Maintain, 'm'},
  PrivChar{AclMode::Execute, 'X'},     PrivChar{AclMode::Usage, 'U'},
  PrivChar{AclMode::Create, 'C'},      PrivChar{AclMode::CreateTemp, 'T'},
  PrivChar{AclMode::Connect, 'c'},     PrivChar{AclMode::Set, 's'},
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

// Render one aclitem to PG's text form: "grantee=privchars/grantor" ("" grantee
// for PUBLIC), each priv char optionally followed by '*' for the grant option.
std::string AclItemToText(
  const AclItem& item, absl::FunctionRef<std::string_view(ObjectId)> name_of) {
  std::string out;
  if (item.grantee != catalog::kPublicGrantee) {
    PutId(out, name_of(item.grantee));
  }
  out.push_back('=');
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

}  // namespace sdb::auth
