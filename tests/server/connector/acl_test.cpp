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

#include <algorithm>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <string_view>
#include <vector>

#include "auth/acl.h"
#include "basics/exceptions.h"
#include "basics/serializer.h"
#include "gtest/gtest.h"

namespace {

using sdb::ObjectId;
using sdb::auth::AclCheck;
using sdb::auth::AclCheckSorted;
using sdb::auth::AclDefault;
using sdb::auth::AclEffective;
using sdb::auth::AclGrant;
using sdb::auth::AclItemToText;
using sdb::auth::AclRevoke;
using sdb::auth::AclToText;
using sdb::auth::OwnerPrivs;
using sdb::auth::ParseAclMode;
using sdb::auth::RoleIdSet;
using sdb::auth::RoleIdSpan;
using sdb::catalog::Acl;
using sdb::catalog::AclItem;
using sdb::catalog::AclMode;
using sdb::catalog::kPublicGrantee;
using sdb::catalog::ObjectType;

constexpr ObjectId kOwner{100};
constexpr ObjectId kAlice{200};
constexpr ObjectId kBob{300};

RoleIdSet Roles(std::initializer_list<ObjectId> ids) {
  RoleIdSet s;
  for (auto id : ids) {
    s.insert(id);
  }
  return s;
}

// A NULL (empty) ACL is not "no privileges": the owner holds all class
// privileges with grant option, and class-specific PUBLIC defaults apply.
TEST(AclDefaultTest, TableOwnerHoldsAllNoPublic) {
  auto acl = AclDefault(ObjectType::Table, kOwner);
  ASSERT_EQ(acl.size(), 1u);
  EXPECT_EQ(acl[0].grantee, kOwner);
  EXPECT_EQ(acl[0].grantor, kOwner);
  EXPECT_EQ(acl[0].privs & AclMode::Select, AclMode::Select);
  EXPECT_EQ(acl[0].privs & AclMode::Insert, AclMode::Insert);
  // PG acldefault stores NO grant options: the owner's grantability is
  // implicit by ownership, and synthesized aclitem text shows no '*'.
  EXPECT_EQ(acl[0].grant_option, AclMode::NoRights);
  // Tables grant nothing to PUBLIC by default.
  for (const auto& item : acl) {
    EXPECT_NE(item.grantee, kPublicGrantee);
  }
}

TEST(AclDefaultTest, DatabaseGrantsPublicConnectTemp) {
  auto acl = AclDefault(ObjectType::Database, kOwner);
  bool found_public = false;
  for (const auto& item : acl) {
    if (item.grantee == kPublicGrantee) {
      found_public = true;
      EXPECT_EQ(item.privs & AclMode::Connect, AclMode::Connect);
      EXPECT_EQ(item.privs & AclMode::CreateTemp, AclMode::CreateTemp);
    }
  }
  EXPECT_TRUE(found_public)
    << "database default ACL must grant PUBLIC CONNECT+TEMP";
}

TEST(AclDefaultTest, FunctionGrantsPublicExecute) {
  auto acl = AclDefault(ObjectType::PgSqlFunction, kOwner);
  bool found_public = false;
  for (const auto& item : acl) {
    if (item.grantee == kPublicGrantee) {
      found_public = true;
      EXPECT_EQ(item.privs & AclMode::Execute, AclMode::Execute);
    }
  }
  EXPECT_TRUE(found_public);
}

// An empty stored ACL expands to the default; a non-empty one is returned
// as-is.
TEST(AclEffectiveTest, EmptyExpandsToDefault) {
  Acl empty;
  auto eff = AclEffective(empty, ObjectType::Table, kOwner);
  EXPECT_EQ(eff.size(), AclDefault(ObjectType::Table, kOwner).size());
}

TEST(AclCheckTest, OwnerDefaultGrantsOwnerEverything) {
  auto acl = AclDefault(ObjectType::Table, kOwner);
  EXPECT_TRUE(AclCheck(acl, Roles({kOwner}), AclMode::Select));
  EXPECT_TRUE(AclCheck(acl, Roles({kOwner}), AclMode::Insert));
  // A non-owner with no grant gets nothing from the default ACL.
  EXPECT_FALSE(AclCheck(acl, Roles({kAlice}), AclMode::Select));
}

TEST(AclCheckTest, PublicGranteeAppliesToEveryone) {
  Acl acl;
  AclGrant(acl, kPublicGrantee, kOwner, AclMode::Select);
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Select));
  EXPECT_TRUE(AclCheck(acl, Roles({kBob}), AclMode::Select));
  EXPECT_FALSE(AclCheck(acl, Roles({kAlice}), AclMode::Insert));
}

TEST(AclCheckTest, PrivilegesAccumulateAcrossItems) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclGrant(acl, kBob, kOwner, AclMode::Insert);
  // Alice inheriting from a role set that includes bob accumulates both.
  EXPECT_TRUE(
    AclCheck(acl, Roles({kAlice, kBob}), AclMode::Select | AclMode::Insert));
  // Alice alone has only Select.
  EXPECT_FALSE(
    AclCheck(acl, Roles({kAlice}), AclMode::Select | AclMode::Insert));
}

// REVOKE matches (grantee, grantor): a grant from a different grantor survives.
TEST(AclGrantRevokeTest, GrantorKeyedItems) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclGrant(acl, kAlice, kBob, AclMode::Select);
  EXPECT_EQ(acl.size(), 2u);

  AclRevoke(acl, kAlice, kOwner, AclMode::Select);
  // Bob's grant survives -> alice still has Select.
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Select));
}

TEST(AclGrantRevokeTest, RevokeNonMatchingGrantorIsNoOp) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclRevoke(acl, kAlice, kBob, AclMode::Select);  // bob never granted
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Select));
}

TEST(AclGrantRevokeTest, RevokingAllPrivilegesDropsItem) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert);
  AclRevoke(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert);
  EXPECT_TRUE(acl.empty());
}

TEST(AclGrantTest, RegrantMergesIntoSameItem) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclGrant(acl, kAlice, kOwner, AclMode::Insert);
  EXPECT_EQ(acl.size(), 1u);
  EXPECT_TRUE(
    AclCheck(acl, Roles({kAlice}), AclMode::Select | AclMode::Insert));
}

TEST(ParseAclModeTest, ParsesTablePrivilegeList) {
  auto mode = ParseAclMode("SELECT, INSERT", ObjectType::Table);
  EXPECT_EQ(mode, AclMode::Select | AclMode::Insert);
  EXPECT_EQ(ParseAclMode("select", ObjectType::Table), AclMode::Select);
}

TEST(ParseAclModeTest, RejectsUnknownPrivilege) {
  EXPECT_THROW(ParseAclMode("FLY", ObjectType::Table), sdb::basics::Exception);
}

TEST(ParseAclModeTest, RejectsClassInvalidPrivilege) {
  // EXECUTE is not a table privilege.
  EXPECT_THROW(ParseAclMode("EXECUTE", ObjectType::Table),
               sdb::basics::Exception);
  // EXECUTE is valid for a function.
  EXPECT_EQ(ParseAclMode("EXECUTE", ObjectType::PgSqlFunction),
            AclMode::Execute);
}

TEST(ParseAclModeTest, EmptyStringYieldsNoRights) {
  EXPECT_EQ(ParseAclMode("", ObjectType::Table), AclMode::NoRights);
  EXPECT_EQ(ParseAclMode("  ,  ", ObjectType::Table), AclMode::NoRights);
}

TEST(ParseAclModeTest, AllExpandsToEveryClassPrivilege) {
  // GRANT ALL / ALL PRIVILEGES -> every privilege valid for the object class.
  const auto all_table = AclMode::Select | AclMode::Insert | AclMode::Update |
                         AclMode::Delete | AclMode::Truncate |
                         AclMode::References | AclMode::Trigger |
                         AclMode::Maintain;
  EXPECT_EQ(ParseAclMode("ALL", ObjectType::Table), all_table);
  EXPECT_EQ(ParseAclMode("all", ObjectType::Table), all_table);
  // The class drives the expansion: a function's ALL is just EXECUTE.
  EXPECT_EQ(ParseAclMode("ALL", ObjectType::PgSqlFunction), AclMode::Execute);
  // ALL combined with an explicit name is still the class set (idempotent OR).
  EXPECT_EQ(ParseAclMode("ALL, SELECT", ObjectType::Table), all_table);
}

TEST(ParseAclModeTest, ToleratesWhitespaceAndCase) {
  EXPECT_EQ(ParseAclMode("  select ,  INSERT  ", ObjectType::Table),
            AclMode::Select | AclMode::Insert);
}

TEST(ParseAclModeTest, EveryTablePrivilege) {
  EXPECT_EQ(ParseAclMode("SELECT", ObjectType::Table), AclMode::Select);
  EXPECT_EQ(ParseAclMode("INSERT", ObjectType::Table), AclMode::Insert);
  EXPECT_EQ(ParseAclMode("UPDATE", ObjectType::Table), AclMode::Update);
  EXPECT_EQ(ParseAclMode("DELETE", ObjectType::Table), AclMode::Delete);
  EXPECT_EQ(ParseAclMode("TRUNCATE", ObjectType::Table), AclMode::Truncate);
  EXPECT_EQ(ParseAclMode("REFERENCES", ObjectType::Table), AclMode::References);
  EXPECT_EQ(ParseAclMode("TRIGGER", ObjectType::Table), AclMode::Trigger);
}

TEST(ParseAclModeTest, SequencePrivileges) {
  EXPECT_EQ(ParseAclMode("USAGE, SELECT, UPDATE", ObjectType::Sequence),
            AclMode::Usage | AclMode::Select | AclMode::Update);
  // INSERT is not a sequence privilege.
  EXPECT_THROW(ParseAclMode("INSERT", ObjectType::Sequence),
               sdb::basics::Exception);
}

TEST(ParseAclModeTest, DatabaseAndSchemaPrivileges) {
  EXPECT_EQ(ParseAclMode("CONNECT, TEMPORARY, CREATE", ObjectType::Database),
            AclMode::Connect | AclMode::CreateTemp | AclMode::Create);
  EXPECT_EQ(ParseAclMode("USAGE, CREATE", ObjectType::Schema),
            AclMode::Usage | AclMode::Create);
  // SELECT is meaningless for a schema.
  EXPECT_THROW(ParseAclMode("SELECT", ObjectType::Schema),
               sdb::basics::Exception);
}

// ---- OwnerPrivs / acldefault for every class ---------------------------

TEST(OwnerPrivsTest, MatchesClassPrivilegeSet) {
  // Owner holds every privilege defined for the class.
  EXPECT_EQ(OwnerPrivs(ObjectType::Table),
            AclMode::Select | AclMode::Insert | AclMode::Update |
              AclMode::Delete | AclMode::Truncate | AclMode::References |
              AclMode::Trigger | AclMode::Maintain);
  EXPECT_EQ(OwnerPrivs(ObjectType::Sequence),
            AclMode::Select | AclMode::Update | AclMode::Usage);
  EXPECT_EQ(OwnerPrivs(ObjectType::PgSqlFunction), AclMode::Execute);
  EXPECT_EQ(OwnerPrivs(ObjectType::PgSqlType), AclMode::Usage);
}

TEST(AclDefaultTest, OwnerSelfGrantStoresNoGrantOptionForEveryClass) {
  for (auto type :
       {ObjectType::Table, ObjectType::Sequence, ObjectType::Database,
        ObjectType::Schema, ObjectType::PgSqlFunction, ObjectType::PgSqlType}) {
    auto acl = AclDefault(type, kOwner);
    bool found_owner = false;
    for (const auto& item : acl) {
      if (item.grantee == kOwner) {
        found_owner = true;
        EXPECT_EQ(item.grantor, kOwner);
        // PG stores goptions = NO_RIGHTS; owner grantability is implicit.
        EXPECT_EQ(item.grant_option, AclMode::NoRights);
        EXPECT_EQ(item.privs, OwnerPrivs(type));
      }
    }
    EXPECT_TRUE(found_owner) << "owner self-grant missing for a class";
  }
}

TEST(AclDefaultTest, TypeGrantsPublicUsage) {
  auto acl = AclDefault(ObjectType::PgSqlType, kOwner);
  bool found = false;
  for (const auto& item : acl) {
    if (item.grantee == kPublicGrantee) {
      found = true;
      EXPECT_EQ(item.privs & AclMode::Usage, AclMode::Usage);
    }
  }
  EXPECT_TRUE(found);
}

TEST(AclDefaultTest, SequenceAndSchemaHaveNoPublicEntry) {
  for (auto type : {ObjectType::Sequence, ObjectType::Schema}) {
    auto acl = AclDefault(type, kOwner);
    for (const auto& item : acl) {
      EXPECT_NE(item.grantee, kPublicGrantee);
    }
  }
}

// PUBLIC's database CONNECT default lets any role connect via the default ACL.
TEST(AclCheckTest, DatabaseDefaultGrantsPublicConnect) {
  auto acl = AclDefault(ObjectType::Database, kOwner);
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Connect));
  EXPECT_TRUE(AclCheck(acl, Roles({kBob}), AclMode::CreateTemp));
  // But not CREATE -- that is owner-only by default.
  EXPECT_FALSE(AclCheck(acl, Roles({kAlice}), AclMode::Create));
}

TEST(AclCheckTest, FunctionDefaultGrantsPublicExecute) {
  auto acl = AclDefault(ObjectType::PgSqlFunction, kOwner);
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Execute));
}

TEST(AclCheckTest, EmptyAclAndEmptyRoleSet) {
  Acl empty;
  EXPECT_FALSE(AclCheck(empty, Roles({kAlice}), AclMode::Select));
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  EXPECT_FALSE(AclCheck(acl, RoleIdSet{}, AclMode::Select));
}

TEST(AclCheckTest, NoRightsNeverSatisfied) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  // Asking for NoRights is not a meaningful grant and returns false.
  EXPECT_FALSE(AclCheck(acl, Roles({kAlice}), AclMode::NoRights));
}

TEST(AclCheckTest, PublicAndDirectGrantCombine) {
  Acl acl;
  AclGrant(acl, kPublicGrantee, kOwner, AclMode::Select);
  AclGrant(acl, kAlice, kOwner, AclMode::Insert);
  // Alice gets Select from PUBLIC and Insert directly.
  EXPECT_TRUE(
    AclCheck(acl, Roles({kAlice}), AclMode::Select | AclMode::Insert));
  // Bob only has Select (from PUBLIC).
  EXPECT_TRUE(AclCheck(acl, Roles({kBob}), AclMode::Select));
  EXPECT_FALSE(AclCheck(acl, Roles({kBob}), AclMode::Insert));
}

// ---- Grant option ------------------------------------------------------

TEST(AclGrantTest, GrantOptionIsSubsetOfPrivs) {
  Acl acl;
  // Grant Select+Insert but only Select WITH GRANT OPTION.
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert,
           AclMode::Select);
  ASSERT_EQ(acl.size(), 1u);
  EXPECT_EQ(acl[0].grant_option, AclMode::Select);
}

TEST(AclGrantTest, GrantOptionClampedToGrantedPrivs) {
  Acl acl;
  // Asking grant option for Insert while only granting Select clamps it away.
  AclGrant(acl, kAlice, kOwner, AclMode::Select, AclMode::Insert);
  ASSERT_EQ(acl.size(), 1u);
  EXPECT_EQ(acl[0].grant_option & AclMode::Insert, AclMode::NoRights);
}

TEST(AclGrantTest, RegrantAccumulatesGrantOption) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclGrant(acl, kAlice, kOwner, AclMode::Select, AclMode::Select);
  ASSERT_EQ(acl.size(), 1u);
  EXPECT_EQ(acl[0].grant_option, AclMode::Select);
}

// ---- Revoke partial ----------------------------------------------------

TEST(AclRevokeTest, PartialRevokeKeepsRemaining) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert);
  AclRevoke(acl, kAlice, kOwner, AclMode::Insert);
  EXPECT_TRUE(AclCheck(acl, Roles({kAlice}), AclMode::Select));
  EXPECT_FALSE(AclCheck(acl, Roles({kAlice}), AclMode::Insert));
}

TEST(AclRevokeTest, RevokeClearsGrantOptionBits) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert,
           AclMode::Select | AclMode::Insert);
  AclRevoke(acl, kAlice, kOwner, AclMode::Select);
  ASSERT_EQ(acl.size(), 1u);
  EXPECT_EQ(acl[0].grant_option & AclMode::Select, AclMode::NoRights);
  EXPECT_EQ(acl[0].grant_option & AclMode::Insert, AclMode::Insert);
}

// ---- Serialization round-trip -----------------------------------------
//
// ACLs persist as a field of TableData/RoleData via the reflection serializer
// (basics::WriteTuple/ReadTuple over BinarySerializer). These tests confirm an
// Acl survives that path with every field intact -- the mechanism that actually
// persists grants on disk.

Acl SerializeRoundTrip(const Acl& acl) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, acl);
  }
  stream.Rewind();
  Acl out;
  duckdb::BinaryDeserializer source{stream};
  sdb::basics::ReadTuple(source, out);
  return out;
}

TEST(AclSerializeTest, RoundTripsAllFields) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert,
           AclMode::Select);
  AclGrant(acl, kBob, kAlice, AclMode::Update);
  AclGrant(acl, kPublicGrantee, kOwner, AclMode::Select);

  auto back = SerializeRoundTrip(acl);
  ASSERT_EQ(back.size(), acl.size());
  for (size_t i = 0; i < acl.size(); ++i) {
    EXPECT_EQ(back[i].grantee, acl[i].grantee);
    EXPECT_EQ(back[i].grantor, acl[i].grantor);
    EXPECT_EQ(back[i].privs, acl[i].privs);
    EXPECT_EQ(back[i].grant_option, acl[i].grant_option);
  }
}

TEST(AclSerializeTest, EmptyRoundTripsToEmpty) {
  Acl empty;
  EXPECT_TRUE(SerializeRoundTrip(empty).empty());
}

TEST(AclSerializeTest, DefaultAclRoundTrips) {
  // A materialized default ACL (owner + PUBLIC entries) survives a round-trip.
  auto acl = AclDefault(ObjectType::Database, kOwner);
  auto back = SerializeRoundTrip(acl);
  EXPECT_EQ(back.size(), acl.size());
  EXPECT_TRUE(AclCheck(back, Roles({kOwner}), AclMode::Create));
  EXPECT_TRUE(AclCheck(back, Roles({kAlice}), AclMode::Connect));
}

// ---- aclitem text synthesis (AclItemToText / AclToText) ----------------
//
// These render the typed ACL into PostgreSQL's aclitem text on the catalog
// read path ("grantee=privs/grantor", '*' = grant option, "" grantee =
// PUBLIC). The hot path never calls these; pg_class.relacl synthesis does.

// Name resolver mirroring pg_class.cpp: PUBLIC -> "", else a fixed map.
std::string_view TestName(ObjectId id) {
  if (id == kPublicGrantee) {
    return {};
  }
  if (id == kOwner) {
    return "owner";
  }
  if (id == kAlice) {
    return "alice";
  }
  if (id == kBob) {
    return "bob";
  }
  return "?";
}

TEST(AclItemToTextTest, DirectGrantNoGrantOption) {
  AclItem item{.grantee = kAlice, .grantor = kOwner, .privs = AclMode::Select};
  EXPECT_EQ(AclItemToText(item, TestName), "alice=r/owner");
}

TEST(AclItemToTextTest, GrantOptionRendersStar) {
  AclItem item{.grantee = kAlice,
               .grantor = kOwner,
               .privs = AclMode::Select | AclMode::Insert,
               .grant_option = AclMode::Select};
  // Canonical char order is a(INSERT) then r(SELECT); only SELECT has the star.
  EXPECT_EQ(AclItemToText(item, TestName), "alice=ar*/owner");
}

TEST(AclItemToTextTest, PublicGranteeRendersEmptyLeft) {
  AclItem item{
    .grantee = kPublicGrantee, .grantor = kOwner, .privs = AclMode::Select};
  EXPECT_EQ(AclItemToText(item, TestName), "=r/owner");
}

TEST(AclItemToTextTest, OwnerSelfGrantNoStars) {
  // Owner default: every table priv, NO stored grant option (PG acldefault
  // stores goptions = NO_RIGHTS) -> arwdDxtm with no stars.
  auto acl = AclDefault(ObjectType::Table, kOwner);
  ASSERT_EQ(acl.size(), 1u);  // table has no PUBLIC default
  EXPECT_EQ(AclItemToText(acl[0], TestName), "owner=arwdDxtm/owner");
}

TEST(AclToTextTest, OneStringPerItemInOrder) {
  Acl acl;
  AclGrant(acl, kOwner, kOwner, AclMode::Select, AclMode::Select);
  AclGrant(acl, kAlice, kOwner, AclMode::Insert);
  auto text = AclToText(acl, TestName);
  ASSERT_EQ(text.size(), 2u);
  EXPECT_EQ(text[0], "owner=r*/owner");
  EXPECT_EQ(text[1], "alice=a/owner");
}

TEST(AclToTextTest, EmptyAclYieldsEmptyVector) {
  EXPECT_TRUE(AclToText(Acl{}, TestName).empty());
}

// ---- AclCheckSorted (the DML hot-path test) ----------------------------
//
// Operates on the STORED acl (empty => acldefault inline, no materialization)
// against a SORTED role span (what SessionAclCache produces). `any_of` selects
// has_*_privilege semantics over enforcement's all-of.

std::vector<ObjectId> Sorted(std::initializer_list<ObjectId> ids) {
  std::vector<ObjectId> v{ids};
  std::ranges::sort(v);
  return v;
}

TEST(AclCheckSortedTest, EmptyAclOwnerGetsEverything) {
  auto roles = Sorted({kOwner});
  EXPECT_TRUE(AclCheckSorted({}, ObjectType::Table, kOwner, roles,
                             AclMode::Select | AclMode::Delete,
                             /*any_of=*/false));
}

TEST(AclCheckSortedTest, EmptyAclNonOwnerTableDenied) {
  auto roles = Sorted({kAlice});
  // Tables have no PUBLIC default, so a non-owner holds nothing.
  EXPECT_FALSE(AclCheckSorted({}, ObjectType::Table, kOwner, roles,
                              AclMode::Select, /*any_of=*/false));
}

TEST(AclCheckSortedTest, EmptyAclPublicDefaultFunctionExecute) {
  auto roles = Sorted({kAlice});
  // Functions grant EXECUTE to PUBLIC by default, so anyone passes.
  EXPECT_TRUE(AclCheckSorted({}, ObjectType::PgSqlFunction, kOwner, roles,
                             AclMode::Execute, /*any_of=*/false));
}

TEST(AclCheckSortedTest, StoredAclDirectGrant) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select | AclMode::Insert);
  auto roles = Sorted({kAlice});
  EXPECT_TRUE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                             AclMode::Select, /*any_of=*/false));
  EXPECT_FALSE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                              AclMode::Delete, /*any_of=*/false));
}

TEST(AclCheckSortedTest, AllOfRequiresEveryBit) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);  // only SELECT
  auto roles = Sorted({kAlice});
  EXPECT_FALSE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                              AclMode::Select | AclMode::Insert,
                              /*any_of=*/false));
  EXPECT_TRUE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                             AclMode::Select | AclMode::Insert,
                             /*any_of=*/true));  // any-of: SELECT alone passes
}

TEST(AclCheckSortedTest, PrivilegesAccumulateAcrossItems) {
  Acl acl;
  AclGrant(acl, kAlice, kOwner, AclMode::Select);
  AclGrant(acl, kBob, kOwner, AclMode::Insert);
  auto roles = Sorted({kAlice, kBob});  // a session in both roles
  EXPECT_TRUE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                             AclMode::Select | AclMode::Insert,
                             /*any_of=*/false));
}

TEST(AclCheckSortedTest, PublicGranteeMatchesAnyRole) {
  Acl acl;
  AclGrant(acl, kPublicGrantee, kOwner, AclMode::Select);
  auto roles = Sorted({kBob});
  EXPECT_TRUE(AclCheckSorted(acl, ObjectType::Table, kOwner, roles,
                             AclMode::Select, /*any_of=*/false));
}

TEST(AclCheckSortedTest, NoRightsNeverSatisfied) {
  auto roles = Sorted({kOwner});
  EXPECT_FALSE(AclCheckSorted({}, ObjectType::Table, kOwner, roles,
                              AclMode::NoRights, /*any_of=*/false));
  EXPECT_FALSE(AclCheckSorted({}, ObjectType::Table, kOwner, roles,
                              AclMode::NoRights, /*any_of=*/true));
}

TEST(AclCheckSortedTest, MatchesUnsortedAclCheckOnDefault) {
  // AclCheckSorted on an empty (default) ACL must agree with the legacy
  // AclEffective+AclCheck path for the same inputs.
  const auto type = ObjectType::Database;
  auto eff = AclEffective({}, type, kOwner);
  auto sorted = Sorted({kAlice});
  // PUBLIC gets CONNECT on a database by default.
  EXPECT_EQ(AclCheck(eff, Roles({kAlice}), AclMode::Connect),
            AclCheckSorted({}, type, kOwner, sorted, AclMode::Connect,
                           /*any_of=*/false));
}

}  // namespace
