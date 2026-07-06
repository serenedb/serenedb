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

#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "network/pg/hba.h"

using namespace sdb::network::pg::hba;

namespace {

std::array<uint8_t, 16> V4(std::string_view dotted) {
  std::array<uint8_t, 16> out{};
  EXPECT_EQ(inet_pton(AF_INET, std::string{dotted}.c_str(), out.data()), 1);
  return out;
}

std::array<uint8_t, 16> V6(std::string_view text) {
  std::array<uint8_t, 16> out{};
  EXPECT_EQ(inet_pton(AF_INET6, std::string{text}.c_str(), out.data()), 1);
  return out;
}

const MembershipFn kNoMembers = [](std::string_view, std::string_view) {
  return false;
};

Ruleset MustParse(std::string_view text) {
  ParseError err;
  auto rs = Parse(text, err);
  EXPECT_TRUE(rs.has_value()) << "line " << err.line << ": " << err.message;
  return rs.value_or(Ruleset{});
}

ParseError MustFail(std::string_view text) {
  ParseError err;
  auto rs = Parse(text, err);
  EXPECT_FALSE(rs.has_value()) << "expected parse failure";
  return err;
}

// A TCP+SSL client from `ip` as user/db.
ClientInfo Ssl(std::string_view ip, std::string_view user,
               std::string_view db) {
  return ClientInfo{.is_local = false,
                    .is_ssl = true,
                    .family = AF_INET,
                    .addr = V4(ip),
                    .user = user,
                    .database = db};
}

Decision::Kind KindOf(const Ruleset& rs, const ClientInfo& c,
                      const MembershipFn& m = kNoMembers) {
  return Decide(rs, c, m).kind;
}

}  // namespace

// --- method vocabulary ------------------------------------------------------

TEST(HbaMethod, ParseFullVocabulary) {
  EXPECT_EQ(ParseMethod("trust"), Method::Trust);
  EXPECT_EQ(ParseMethod("reject"), Method::Reject);
  EXPECT_EQ(ParseMethod("scram-sha-256"), Method::Scram);
  EXPECT_EQ(ParseMethod("md5"), Method::Md5);
  EXPECT_EQ(ParseMethod("password"), Method::Password);
  EXPECT_EQ(ParseMethod("peer"), Method::Peer);
  EXPECT_EQ(ParseMethod("ident"), Method::Ident);
  EXPECT_EQ(ParseMethod("ldap"), Method::Ldap);
  EXPECT_EQ(ParseMethod("cert"), Method::Cert);
  EXPECT_EQ(ParseMethod("gss"), Method::Gss);
  EXPECT_FALSE(ParseMethod("bogus").has_value());
  EXPECT_EQ(MethodExecutability(Method::Scram), MethodClass::Native);
  EXPECT_EQ(MethodExecutability(Method::Ldap), MethodClass::RejectAtConnect);
  EXPECT_EQ(MethodExecutability(Method::Peer), MethodClass::DeferredPeerIdent);
}

// --- parse: everything PG parses --------------------------------------------

TEST(HbaParse, UnsupportedMethodsParseNotError) {
  // ldap/gss/cert are real PG keywords: they must PARSE (reject-at-connect),
  // not fail parsing.
  MustParse("hostssl all all 0.0.0.0/0 ldap ldapserver=x\n");
  MustParse("host all all 0.0.0.0/0 gss\n");
  MustParse("hostssl all all 0.0.0.0/0 cert clientcert=verify-full\n");
}

TEST(HbaParse, GssConnTypesParse) {
  MustParse("hostgssenc all all 0.0.0.0/0 trust\n");
  MustParse("hostnogssenc all all 0.0.0.0/0 trust\n");
}

TEST(HbaParse, OptionFieldsParse) {
  MustParse("host all all 0.0.0.0/0 ldap ldapserver=a ldapport=389\n");
  auto err = MustFail("host all all 0.0.0.0/0 ldap noequals\n");
  EXPECT_EQ(err.line, 1u);
  EXPECT_NE(err.message.find("name=value"), std::string::npos);
}

TEST(HbaParse, QuotedSpacesAndCommas) {
  // A quoted role name containing a space is one token.
  auto rs = MustParse("host all \"role name\" 0.0.0.0/0 trust\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "role name", "d")),
            Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "role", "d")), Decision::Kind::Reject);
}

TEST(HbaParse, DoubledQuoteEscape) {
  // "a""b" => literal a"b
  auto rs = MustParse("host all \"a\"\"b\" 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "a\"b", "d")), Decision::Kind::Trust);
}

TEST(HbaParse, HashInQuoteIsLiteral) {
  auto rs = MustParse("host all \"a#b\" 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "a#b", "d")), Decision::Kind::Trust);
}

TEST(HbaParse, CommaListWithSpaces) {
  // Spaces around commas: still one role field with two tokens.
  auto rs = MustParse("host all alice , bob 0.0.0.0/0 trust\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "alice", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "bob", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "carol", "d")), Decision::Kind::Reject);
}

TEST(HbaParse, LineContinuation) {
  auto rs = MustParse("host all all \\\n0.0.0.0/0 trust\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "d")), Decision::Kind::Trust);
}

TEST(HbaParse, IncludeDirectiveNoOp) {
  // include is recognized (not a malformed rule) and expands to nothing.
  auto rs = MustParse("include other.conf\nhost all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(rs.rules.size(), 1u);
}

TEST(HbaParse, CaseSensitiveConnTypeAndMethod) {
  EXPECT_EQ(MustFail("HOST all all 0.0.0.0/0 trust\n").line, 1u);
  EXPECT_EQ(MustFail("host all all 0.0.0.0/0 TRUST\n").line, 1u);
}

TEST(HbaParse, RejectsBadRegex) {
  auto err = MustFail("host \"/[\" all 0.0.0.0/0 trust\n");  // db regex
  EXPECT_EQ(err.line, 1u);
  err = MustFail("host all /( 0.0.0.0/0 trust\n");  // role regex
  EXPECT_EQ(err.line, 1u);
}

TEST(HbaParse, FieldCountErrors) {
  EXPECT_NE(MustFail("host\n").message.find("database"), std::string::npos);
  EXPECT_NE(MustFail("host all\n").message.find("role"), std::string::npos);
  EXPECT_NE(MustFail("host all all\n").message.find("IP address"),
            std::string::npos);
  EXPECT_NE(MustFail("local all all\n").message.find("method"),
            std::string::npos);
}

// --- matching: native subset behaves as PG ----------------------------------

TEST(HbaMatch, ConnTypeSslDiscrimination) {
  auto rs = MustParse("hostssl all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "d")), Decision::Kind::Trust);
  ClientInfo nossl{.is_ssl = false,
                   .family = AF_INET,
                   .addr = V4("10.0.0.1"),
                   .user = "u",
                   .database = "d"};
  EXPECT_EQ(KindOf(rs, nossl), Decision::Kind::Reject);

  auto any = MustParse("host all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(any, nossl), Decision::Kind::Trust);  // host matches both
}

TEST(HbaMatch, LocalVsHost) {
  auto rs = MustParse("local all all trust\n");
  ClientInfo local{.is_local = true, .user = "u", .database = "d"};
  EXPECT_EQ(KindOf(rs, local), Decision::Kind::Trust);
  // A host rule never matches a local connection.
  auto host = MustParse("host all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(host, local), Decision::Kind::Reject);
}

TEST(HbaMatch, HostGssNeverMatches) {
  auto rs = MustParse("hostgssenc all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "d")), Decision::Kind::Reject);
  // hostnogssenc always passes the GSS axis for a never-GSS client.
  auto nogss = MustParse("hostnogssenc all all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(nogss, Ssl("10.0.0.1", "u", "d")), Decision::Kind::Trust);
}

TEST(HbaMatch, CidrV4Masks) {
  auto rs =
    MustParse("host all all 10.0.0.0/8 trust\nhost all all 0.0.0.0/0 reject\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.5.5.5", "u", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("11.0.0.1", "u", "d")), Decision::Kind::Reject);

  auto h32 = MustParse(
    "host all all 127.0.0.1/32 trust\nhost all all 0.0.0.0/0 reject\n");
  EXPECT_EQ(KindOf(h32, Ssl("127.0.0.1", "u", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(h32, Ssl("127.0.0.2", "u", "d")), Decision::Kind::Reject);

  auto h16 = MustParse(
    "host all all 127.0.0.0/16 trust\nhost all all 0.0.0.0/0 reject\n");
  EXPECT_EQ(KindOf(h16, Ssl("127.0.255.254", "u", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(h16, Ssl("127.1.0.1", "u", "d")), Decision::Kind::Reject);
}

TEST(HbaMatch, DottedNetmaskColumn) {
  auto rs = MustParse(
    "host all all 127.0.0.0 255.255.255.0 trust\nhost all all 0.0.0.0/0 "
    "reject\n");
  EXPECT_EQ(KindOf(rs, Ssl("127.0.0.9", "u", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("127.0.1.9", "u", "d")), Decision::Kind::Reject);
}

TEST(HbaMatch, NonContiguousNetmask) {
  // 255.0.255.0 -- non-contiguous; bytewise AND must handle it.
  auto rs = MustParse(
    "host all all 127.0.7.0 255.0.255.0 trust\nhost all all 0.0.0.0/0 "
    "reject\n");
  EXPECT_EQ(KindOf(rs, Ssl("127.99.7.99", "u", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("127.99.8.99", "u", "d")), Decision::Kind::Reject);
}

TEST(HbaMatch, CidrV6AndFamilyStrict) {
  auto rs = MustParse("host all all 2001:db8::/32 trust\n");
  ClientInfo in6{.is_ssl = true,
                 .family = AF_INET6,
                 .addr = V6("2001:db8::1"),
                 .user = "u",
                 .database = "d"};
  EXPECT_EQ(KindOf(rs, in6), Decision::Kind::Trust);
  // v4 client never matches a v6 rule.
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "d")), Decision::Kind::Reject);
}

TEST(HbaMatch, FirstMatchWins) {
  auto rs = MustParse(
    "host all all 127.0.0.5/32 reject\nhost all all 127.0.0.0/8 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("127.0.0.5", "u", "d")), Decision::Kind::Reject);
  EXPECT_EQ(KindOf(rs, Ssl("127.0.0.6", "u", "d")), Decision::Kind::Trust);
}

TEST(HbaMatch, ImplicitReject) {
  auto rs = MustParse("host all all 10.0.0.0/8 trust\n");
  auto d = Decide(rs, Ssl("127.0.0.1", "u", "d"), kNoMembers);
  EXPECT_EQ(d.kind, Decision::Kind::Reject);
  EXPECT_EQ(d.rule, nullptr);  // implicit (no rule matched)
}

TEST(HbaMatch, DatabaseKeywords) {
  EXPECT_EQ(KindOf(MustParse("host sameuser all 0.0.0.0/0 trust\n"),
                   Ssl("10.0.0.1", "alice", "alice")),
            Decision::Kind::Trust);
  EXPECT_EQ(KindOf(MustParse("host sameuser all 0.0.0.0/0 trust\n"),
                   Ssl("10.0.0.1", "alice", "postgres")),
            Decision::Kind::Reject);

  // samerole: login role is a member of a role named like the database.
  MembershipFn m = [](std::string_view u, std::string_view g) {
    return u == "alice" && g == "analysts";
  };
  EXPECT_EQ(KindOf(MustParse("host samerole all 0.0.0.0/0 trust\n"),
                   Ssl("10.0.0.1", "alice", "analysts"), m),
            Decision::Kind::Trust);

  // replication keyword never matches a normal connection.
  EXPECT_EQ(KindOf(MustParse("host replication all 0.0.0.0/0 trust\n"),
                   Ssl("10.0.0.1", "u", "replication")),
            Decision::Kind::Reject);
}

TEST(HbaMatch, DatabaseRegex) {
  auto rs = MustParse("host \"/^db_.*\" all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "db_sales")),
            Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "payroll")),
            Decision::Kind::Reject);
}

TEST(HbaMatch, RoleGroupAndRegex) {
  MembershipFn m = [](std::string_view u, std::string_view g) {
    return g == "analysts" && (u == "alice" || u == "bob");
  };
  auto rs = MustParse("host all +analysts 0.0.0.0/0 scram-sha-256\n");
  EXPECT_EQ(Decide(rs, Ssl("10.0.0.1", "alice", "d"), m).kind,
            Decision::Kind::Method);
  EXPECT_EQ(Decide(rs, Ssl("10.0.0.1", "alice", "d"), m).method, Method::Scram);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "carol", "d"), m),
            Decision::Kind::Reject);

  auto re = MustParse("host all /^svc_ 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(re, Ssl("10.0.0.1", "svc_etl", "d")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(re, Ssl("10.0.0.1", "human", "d")), Decision::Kind::Reject);
}

TEST(HbaMatch, QuotedKeywordIsLiteral) {
  // "all" quoted => a literal database named all, not the wildcard.
  auto rs = MustParse("host \"all\" all 0.0.0.0/0 trust\n");
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "all")), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "other")), Decision::Kind::Reject);
}

// --- reject-at-connect + deferred outcomes ----------------------------------

TEST(HbaDecision, UnsupportedMethod) {
  auto rs = MustParse("hostssl all all 0.0.0.0/0 ldap ldapserver=x\n");
  auto d = Decide(rs, Ssl("10.0.0.1", "u", "d"), kNoMembers);
  EXPECT_EQ(d.kind, Decision::Kind::Unsupported);
  EXPECT_NE(d.reason.find("ldap"), std::string::npos);
}

TEST(HbaDecision, PeerIsDeferred) {
  auto rs = MustParse("local all all peer\n");
  ClientInfo local{.is_local = true, .user = "u", .database = "d"};
  EXPECT_EQ(Decide(rs, local, kNoMembers).kind,
            Decision::Kind::DeferredPeerIdent);
}

TEST(HbaDecision, LocalIdentRewritesToPeer) {
  // `local ... ident` becomes peer at parse (PG behavior).
  auto rs = MustParse("local all all ident\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  EXPECT_EQ(rs.rules[0].method, Method::Peer);
}

TEST(HbaDecision, HostnameDeferred) {
  auto rs = MustParse("host all all db.example.com trust\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  EXPECT_EQ(rs.rules[0].address.kind, AddrMatcher::Kind::Hostname);
  EXPECT_EQ(KindOf(rs, Ssl("10.0.0.1", "u", "d")),
            Decision::Kind::MatchedHostnameDeferred);
}

TEST(HbaDecision, PeerOnNonLocalIsParseError) {
  EXPECT_EQ(MustFail("host all all 0.0.0.0/0 peer\n").line, 1u);
}

TEST(HbaDecision, CertOnNonSslIsParseError) {
  EXPECT_EQ(MustFail("host all all 0.0.0.0/0 cert\n").line, 1u);
}

TEST(HbaParse, LocalDashIsLiteralDatabase) {
  // No more `-` placeholder: `local - all trust` means database literally '-'.
  auto rs = MustParse("local - all trust\n");
  ASSERT_EQ(rs.rules.size(), 1u);
  ClientInfo local_dash{.is_local = true, .user = "u", .database = "-"};
  ClientInfo local_d{.is_local = true, .user = "u", .database = "d"};
  EXPECT_EQ(KindOf(rs, local_dash), Decision::Kind::Trust);
  EXPECT_EQ(KindOf(rs, local_d), Decision::Kind::Reject);
}

// Only the built-in zero-config default carries is_default=true. Authenticate()
// keys the trust-a-password-less-role fallback off this flag, so an explicitly
// configured ruleset never silently downgrades a password method to trust.
TEST(HbaDefault, IsDefaultOnlyForBuiltIn) {
  EXPECT_TRUE(DefaultRuleset().is_default);
  EXPECT_FALSE(MustParse("host all all 0.0.0.0/0 scram-sha-256\n").is_default);

  // SetHbaFromText produces a non-default (admin-authored) live ruleset; an
  // empty ("reset") ruleset is likewise non-default, not the built-in default.
  ASSERT_FALSE(SetHbaFromText("host all all 0.0.0.0/0 scram-sha-256\n"));
  EXPECT_FALSE(GetHbaRuleset()->is_default);
  ASSERT_FALSE(SetHbaFromText(""));
  EXPECT_FALSE(GetHbaRuleset()->is_default);
}
