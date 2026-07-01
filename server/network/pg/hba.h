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

#include <re2/re2.h>

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::network::pg::hba {

// PG conntype (parse_hba_line, hba.c:1362-1418). An enum, not a bitmask: PG
// matches with a short-circuit switch over the connection's SSL/GSS state, not
// an AND.
enum class ConnType : uint8_t {
  Local,      // unix socket
  Host,       // TCP, any SSL/GSS state
  HostSsl,    // TCP + TLS active
  HostNoSsl,  // TCP, no TLS
  HostGss,    // TCP + GSSAPI encryption (never matches -- SDB has no GSS)
  HostNoGss,  // TCP, no GSSAPI
};

// The full PG UserAuth method set (UserAuthName[], hba.c:102-119) plus an
// ImplicitReject sentinel for "no rule matched".
enum class Method : uint8_t {
  ImplicitReject,
  Trust,
  Reject,
  Password,
  Md5,
  Scram,
  Ident,
  Peer,
  Gss,
  Sspi,
  Pam,
  Bsd,
  Ldap,
  Cert,
  Oauth,
};

// How SerenedB treats a matched rule's method.
//  Native            -- SDB evaluates it (trust/reject/password/md5/scram).
//  DeferredPeerIdent -- parses; enforcement not yet built (unix SO_PEERCRED).
//  RejectAtConnect   -- parses; a matched connection is refused (SDB can't do
//  it).
enum class MethodClass : uint8_t { Native, DeferredPeerIdent, RejectAtConnect };

MethodClass MethodExecutability(Method method);

std::optional<Method> ParseMethod(std::string_view name);
std::string_view MethodName(Method method);

// Resolves group membership for `+group`/`samerole`/`samegroup`. Injected so
// the matcher stays free of the catalog. Contract: NOSUPER semantics -- a
// superuser is NOT an implicit member (is_member_of_role_nosuper,
// hba.c:920-939) -- and returns false for a nonexistent target group
// (missing_ok).
using MembershipFn =
  std::function<bool(std::string_view user, std::string_view group)>;

// One parsed token in a name field. Carries quoted-ness (suppresses keyword /
// +group / @file meaning, token_is_keyword hba.c:71) and an optional compiled
// regex (non-null iff value began with '/', regardless of quoted). shared_ptr
// so a Rule/Ruleset copies cheaply and an in-flight match keeps the regex alive
// across a COW ruleset swap.
struct AuthToken {
  std::string value;  // dequoted; for regex, without the '/'
  bool quoted = false;
  std::shared_ptr<const re2::RE2> regex;
};

// The connection facts a rule is matched against. Filled by the P2 call site.
struct ClientInfo {
  bool is_local = false;           // AF_UNIX socket; family/addr must be 0
  bool is_ssl = false;             // TLS active on THIS socket right now
  bool is_gss = false;             // always false for SDB
  int family = 0;                  // AF_INET / AF_INET6; 0 when is_local
  std::array<uint8_t, 16> addr{};  // client IP, network order, left-aligned
  bool is_replication = false;  // physical walsender; false unless SDB adds it
  std::string_view user;
  std::string_view database;
};

// A name-field matcher (database or role column): the comma-list of tokens,
// evaluated in order (first token to match wins), with per-token semantics
// decided at match time exactly as PG's check_db / check_role do.
struct NameMatcher {
  enum class Field : uint8_t { Database, Role };
  Field field = Field::Database;
  std::vector<AuthToken> tokens;

  bool Matches(const ClientInfo& client, const MembershipFn& is_member) const;
};

// An address-field matcher. CIDR / bare-IP+netmask store raw addr + raw mask
// (mask may be non-contiguous); matching ANDs both sides. samehost/samenet/
// hostname parse but are not evaluated (reject-at-connect).
struct AddrMatcher {
  enum class Kind : uint8_t { All, Mask, SameHost, SameNet, Hostname };
  Kind kind = Kind::All;
  int family = 0;                  // AF_INET / AF_INET6 for Mask
  std::array<uint8_t, 16> addr{};  // raw (NOT pre-masked)
  std::array<uint8_t, 16> mask{};  // raw mask bytes
  std::string hostname;  // Kind::Hostname; leading '.' => suffix match

  // Only meaningful for All / Mask. For the deferred kinds the caller checks
  // `kind` and produces a reject-at-connect Decision.
  bool Matches(int client_family,
               const std::array<uint8_t, 16>& client_addr) const;
};

// A parsed name=value auth option (map=, clientcert=, ldap*, ...). Carried
// opaquely; inert for the methods SDB executes natively.
struct MethodOption {
  std::string name;
  std::string value;
};

struct Rule {
  uint32_t seq = 0;  // authoring order == precedence
  ConnType conntype = ConnType::Host;
  AddrMatcher address;  // meaningful only when conntype != Local
  NameMatcher database;
  NameMatcher role;
  Method method = Method::ImplicitReject;
  std::vector<MethodOption> options;
  std::string raw;  // verbatim authored line, for the view
};

struct Ruleset {
  std::vector<Rule> rules;  // first-match-wins, in seq order
  uint64_t version = 0;
};

// The outcome of matching a connection against the ruleset.
struct Decision {
  enum class Kind : uint8_t {
    Trust,              // matched a trust rule -- skip the password exchange
    Reject,             // matched a reject rule OR no rule matched (see rule)
    Method,             // matched a native password method (Password/Md5/Scram)
    DeferredPeerIdent,  // matched peer/ident -- not yet supported
    Unsupported,        // matched a method SDB can't perform (ldap/gss/...)
    MatchedHostnameDeferred,  // matched a samehost/samenet/hostname rule
  };
  Kind kind = Kind::Reject;
  Method method = Method::ImplicitReject;  // valid when kind == Method
  std::vector<MethodOption> options;       // threaded to the auth layer
  std::string reason;          // client-facing text for the deferred kinds
  const Rule* rule = nullptr;  // matched rule; nullptr => implicit reject
};

// First-match-wins scan (check_hba, hba.c:2337-2438). No match => implicit
// reject (Decision{Reject, rule=nullptr}).
Decision Decide(const Ruleset& ruleset, const ClientInfo& client,
                const MembershipFn& is_member);

// --- Authoring ---------------------------------------------------------------

struct ParseError {
  size_t line = 0;  // 1-based; 0 == not line-specific
  std::string message;
};

// Parse an authored ruleset (PG pg_hba.conf grammar). Returns the ruleset, or
// the first error. Pure: never injects a safety/default rule -- that is the
// caller's job (see SetHbaFromText). @file / include directives are recognized
// but expand to nothing unless a file-reader hook is installed.
std::optional<Ruleset> Parse(std::string_view text, ParseError& error);

// --- Live ruleset holder (process-global, COW) -------------------------------
//
// The single source of truth the pg-wire session consults and the `hba` GUC
// mutates. Held behind an atomic shared_ptr so a session reads a stable
// snapshot for the whole match while a concurrent SET publishes a new pointer
// -- no lock, no tear, and any in-flight regex stays alive through the swap.

// The current ruleset (never null once the server has initialized it).
std::shared_ptr<const Ruleset> GetHbaRuleset();

// Publish a new ruleset. The caller is expected to have already run the
// safety-rule prepend (SetHbaFromText does this).
void SetHbaRuleset(std::shared_ptr<const Ruleset> ruleset);

// The default ruleset used when the configured text is empty: a local +
// loopback escape hatch so a fresh/blank config never locks everyone out
// (CockroachDB DefaultHBAConfig). Also the basis for the mandatory prepend.
Ruleset DefaultRuleset();

// Parse `text`, force-prepend the un-removable safety rule if the ruleset does
// not already begin with it, and -- on success -- atomically publish it as the
// live ruleset. On a parse error the live ruleset is left untouched and the
// error is returned (keep-last-good). An empty/all-comment text installs
// DefaultRuleset(). This is the GUC set-handler's engine entry point.
std::optional<ParseError> SetHbaFromText(std::string_view text);

// Same as SetHbaFromText but returns a formatted "line N: <message>" string on
// error (nullopt on success). The string-typed boundary lets the query-layer
// GUC handler call in without sharing the ParseError type across a header seam.
std::optional<std::string> SetHbaFromTextString(std::string_view text);

// A view-ready projection of one live rule, shaped for pg_hba_file_rules: the
// conn-type / address / netmask rendered back to text, the database/role token
// lists and options as string arrays. Backs the pg_hba_file_rules() system
// table (P4). `address`/`netmask` are empty for `local` and keyword addresses.
struct RenderedRule {
  uint32_t rule_number = 0;
  std::string type;  // local / host / hostssl / ...
  std::vector<std::string>
    databases;                     // database[] tokens (raw, incl. keywords)
  std::vector<std::string> roles;  // user_name[] tokens
  std::string address;             // CIDR base / keyword / hostname; "" if none
  std::string netmask;             // dotted/hex mask; "" unless a Mask rule
  std::string auth_method;         // trust / scram-sha-256 / ...
  std::vector<std::string> options;  // "name=value" entries
};

// Project the current live ruleset into view rows (in first-match order).
std::vector<RenderedRule> RenderHbaRules();

}  // namespace sdb::network::pg::hba
