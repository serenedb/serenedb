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

#include "network/pg/hba.h"

#include <absl/strings/str_cat.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <exception>
#include <filesystem>
#include <iterator>
#include <memory>
#include <ranges>

#include "basics/file_utils.h"
#include "basics/log.h"
#include "basics/static_strings.h"

namespace sdb::network::pg::hba {
namespace {

// ---------------------------------------------------------------------------
// Tokenizer -- a faithful port of PG next_token / next_field_expand
// (hba.c:182-414). A line is a list of FIELDS; a field is a comma-list of
// tokens. Quotes suppress blank/comma/# and keyword meaning; "" inside a quote
// is a literal quote.
// ---------------------------------------------------------------------------

// pg_isblank (hba.c:140-145): ASCII space or tab only.
bool IsBlank(char c) { return c == ' ' || c == '\t'; }

struct Token {
  std::string value;
  bool quoted = false;  // initial_quote: token started with a quote
};

// Reads one token starting at `pos` in `line`. Sets terminating_comma when the
// token ended on an unquoted comma. Advances `pos` past the token (leaving a
// terminating comma for the next call's leading-skip, hba.c:246).
Token NextToken(std::string_view line, size_t& pos, bool& terminating_comma) {
  Token tok;
  terminating_comma = false;

  // Skip leading blanks and commas (hba.c:197-198).
  while (pos < line.size() && (IsBlank(line[pos]) || line[pos] == ',')) {
    ++pos;
  }

  bool in_quote = false;
  bool was_quote = false;
  bool saw_quote = false;

  while (pos < line.size()) {
    const char c = line[pos];
    if (!in_quote && IsBlank(c)) {
      // Token ends on a blank. PG peeks past trailing blanks: if the next
      // non-blank is a comma, the field continues (hba.c:240-245).
      size_t peek = pos;
      while (peek < line.size() && IsBlank(line[peek])) {
        ++peek;
      }
      if (peek < line.size() && line[peek] == ',') {
        terminating_comma = true;
      }
      break;
    }
    if (c == '#' && !in_quote) {
      pos = line.size();  // comment: consume rest of line (hba.c:208-213)
      break;
    }
    if (c == ',' && !in_quote) {
      terminating_comma =
        true;  // leave the comma for the next call (hba.c:216-220)
      break;
    }
    // Append unless it's a quote char that isn't the second of a doubled pair
    // (hba.c:222-223).
    if (c != '"' || was_quote) {
      tok.value.push_back(c);
    }
    // Doubled-quote tracking (hba.c:226-229).
    if (in_quote && c == '"') {
      was_quote = !was_quote;
    } else {
      was_quote = false;
    }
    // Quote toggle + initial-quote flag (hba.c:231-237).
    if (c == '"') {
      if (!saw_quote && tok.value.empty()) {
        tok.quoted = true;
      }
      saw_quote = true;
      in_quote = !in_quote;
    }
    ++pos;
  }
  return tok;
}

// The seam for @file / include expansion. Default drops the reference (returns
// nothing) so a GUC string parses without touching the filesystem; a real
// reader can be installed later.
using FileReader = std::function<std::optional<std::string>(std::string_view)>;

// Builds one FIELD: a comma-list of tokens (hba.c:376-414). An unquoted token
// beginning with '@' (len>1) is a file reference; with no reader it expands to
// nothing.
std::vector<Token> NextField(std::string_view line, size_t& pos,
                             const FileReader& read_file) {
  std::vector<Token> field;
  bool comma = false;
  do {
    Token t = NextToken(line, pos, comma);
    if (t.value.empty() && !comma && t.quoted == false && pos >= line.size() &&
        field.empty()) {
      // Nothing more on the line.
      break;
    }
    if (!t.quoted && t.value.size() > 1 && t.value.front() == '@') {
      // @file: expand its tokens into this field, or drop if no reader.
      if (read_file) {
        if (auto contents = read_file(std::string_view{t.value}.substr(1))) {
          size_t fp = 0;
          bool fc = false;
          while (fp < contents->size()) {
            Token ft = NextToken(*contents, fp, fc);
            if (!ft.value.empty() || ft.quoted) {
              field.push_back(std::move(ft));
            }
          }
        }
      }
      // else: recognized, expands to nothing (documented divergence).
    } else if (!t.value.empty() || t.quoted) {
      field.push_back(std::move(t));
    }
  } while (comma);
  return field;
}

// Splits a logical line into fields. Empty result => blank/comment line.
std::vector<std::vector<Token>> TokenizeLine(std::string_view line,
                                             const FileReader& read_file) {
  std::vector<std::vector<Token>> fields;
  size_t pos = 0;
  while (pos < line.size()) {
    const size_t before = pos;
    std::vector<Token> field = NextField(line, pos, read_file);
    if (!field.empty()) {
      fields.push_back(std::move(field));
    }
    if (pos == before) {
      break;  // no progress (trailing blanks / comment)
    }
  }
  return fields;
}

// Joins physical lines on a single trailing backslash (hba.c:731-756) and
// yields logical lines with their starting 1-based line number.
struct LogicalLine {
  std::string text;
  size_t line_no;  // 1-based line where this logical line began
};

std::vector<LogicalLine> SpliceLines(std::string_view text) {
  std::vector<LogicalLine> out;
  size_t i = 0;
  size_t phys = 0;  // physical lines consumed so far
  while (i <= text.size()) {
    size_t nl = text.find('\n', i);
    const bool last = nl == std::string_view::npos;
    std::string_view raw = text.substr(i, last ? text.size() - i : nl - i);
    if (!raw.empty() && raw.back() == '\r') {
      raw.remove_suffix(1);  // strip CR (pg_strip_crlf)
    }
    ++phys;
    const size_t start_line = phys;

    std::string joined{raw};
    // Continue while the (accumulated) buffer ends in a lone trailing
    // backslash.
    while (!last && !joined.empty() && joined.back() == '\\' &&
           !(joined.size() >= 2 && joined[joined.size() - 2] == '\\')) {
      joined.pop_back();  // drop the continuation backslash
      i = nl + 1;
      nl = text.find('\n', i);
      std::string_view cont =
        text.substr(i, nl == std::string_view::npos ? text.size() - i : nl - i);
      if (!cont.empty() && cont.back() == '\r') {
        cont.remove_suffix(1);
      }
      ++phys;
      joined.append(cont);
      if (nl == std::string_view::npos) {
        break;
      }
    }

    out.push_back({std::move(joined), start_line});
    if (last || nl == std::string_view::npos) {
      break;
    }
    i = nl + 1;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Regex (hba.c:298-336). A '/'-prefixed token compiles to RE2 at parse time.
// ---------------------------------------------------------------------------

std::shared_ptr<const re2::RE2> CompileRegex(std::string_view pattern,
                                             std::string& why) {
  re2::RE2::Options opts;
  opts.set_log_errors(false);
  opts.set_case_sensitive(true);  // PG compiles with C collation
  auto re = std::make_shared<re2::RE2>(pattern, opts);
  if (!re->ok()) {
    why = absl::StrCat("invalid regular expression \"", pattern,
                       "\": ", re->error());
    return nullptr;
  }
  return re;
}

// ---------------------------------------------------------------------------
// Address parsing (hba.c:1509-1666).
// ---------------------------------------------------------------------------

bool PtonAny(const std::string& s, int& family, std::array<uint8_t, 16>& out) {
  if (s.find(':') != std::string::npos) {
    if (inet_pton(AF_INET6, s.c_str(), out.data()) == 1) {
      family = AF_INET6;
      return true;
    }
    return false;
  }
  if (inet_pton(AF_INET, s.c_str(), out.data()) == 1) {
    family = AF_INET;
    return true;
  }
  return false;
}

int FamilyWidth(int family) { return family == AF_INET6 ? 128 : 32; }

void MaskFromPrefix(unsigned prefix, std::array<uint8_t, 16>& mask) {
  mask.fill(0);
  for (unsigned bit = 0; bit < prefix; ++bit) {
    mask[bit / 8] |= static_cast<uint8_t>(0x80u >> (bit % 8));
  }
}

// ---------------------------------------------------------------------------
// Field parsers.
// ---------------------------------------------------------------------------

bool ParseConnType(std::string_view tok, ConnType& out) {
  if (tok == "local") {
    out = ConnType::Local;
  } else if (tok == "host") {
    out = ConnType::Host;
  } else if (tok == "hostssl") {
    out = ConnType::HostSsl;
  } else if (tok == "hostnossl") {
    out = ConnType::HostNoSsl;
  } else if (tok == "hostgssenc") {
    out = ConnType::HostGss;
  } else if (tok == "hostnogssenc") {
    out = ConnType::HostNoGss;
  } else {
    return false;
  }
  return true;
}

// Builds a NameMatcher from a field's token list, compiling regexes. Keyword
// meaning is resolved at match time, so nothing is collapsed here.
bool BuildNameMatcher(const std::vector<Token>& toks, NameMatcher::Field field,
                      NameMatcher& out, std::string& why) {
  out.field = field;
  for (const auto& t : toks) {
    AuthToken at;
    at.value = t.value;
    at.quoted = t.quoted;
    if (!t.value.empty() &&
        t.value.front() == '/') {  // regex, regardless of quoted
      at.regex = CompileRegex(std::string_view{t.value}.substr(1), why);
      if (!at.regex) {
        return false;
      }
    }
    out.tokens.push_back(std::move(at));
  }
  return true;
}

// ---------------------------------------------------------------------------
// Line parser -- parse_hba_line field walk (hba.c:1323-1990).
// ---------------------------------------------------------------------------

bool IsIncludeDirective(const std::vector<std::vector<Token>>& fields) {
  if (fields.size() != 2 || fields[0].size() != 1 || fields[0][0].quoted) {
    return false;
  }
  const std::string& kw = fields[0][0].value;
  return kw == "include" || kw == "include_dir" || kw == "include_if_exists";
}

std::optional<Rule> ParseLine(const std::vector<std::vector<Token>>& fields,
                              uint32_t seq, ParseError& err, size_t line_no) {
  Rule rule;
  rule.seq = seq;

  auto fail = [&](std::string msg) -> std::optional<Rule> {
    err = {line_no, std::move(msg)};
    return std::nullopt;
  };
  auto single = [&](size_t idx, const char* multi_msg,
                    std::string_view& out) -> bool {
    if (fields[idx].size() != 1) {
      err = {line_no, multi_msg};
      return false;
    }
    out = fields[idx][0].value;
    return true;
  };

  // Field 0: conn type (one token, case-sensitive).
  std::string_view conn;
  if (!single(0, "multiple values specified for connection type", conn)) {
    return std::nullopt;
  }
  if (!ParseConnType(conn, rule.conntype)) {
    return fail(absl::StrCat("invalid connection type \"", conn, "\""));
  }
  const bool is_local = rule.conntype == ConnType::Local;

  // Field 1: database.
  if (fields.size() < 2) {
    return fail("end-of-line before database specification");
  }
  std::string why;
  if (!BuildNameMatcher(fields[1], NameMatcher::Field::Database, rule.database,
                        why)) {
    return fail(absl::StrCat("database field: ", why));
  }

  // Field 2: role.
  if (fields.size() < 3) {
    return fail("end-of-line before role specification");
  }
  if (!BuildNameMatcher(fields[2], NameMatcher::Field::Role, rule.role, why)) {
    return fail(absl::StrCat("role field: ", why));
  }

  size_t idx = 3;

  // Address + optional netmask column (host* only).
  if (!is_local) {
    if (fields.size() <= idx) {
      return fail("end-of-line before IP address specification");
    }
    std::string_view addr_tok;
    if (!single(idx, "multiple values specified for host address", addr_tok)) {
      return std::nullopt;
    }
    ++idx;

    if (addr_tok == "all") {
      rule.address.kind = AddrMatcher::Kind::All;
    } else if (addr_tok == "samehost") {
      rule.address.kind = AddrMatcher::Kind::SameHost;
    } else if (addr_tok == "samenet") {
      rule.address.kind = AddrMatcher::Kind::SameNet;
    } else {
      const auto slash = addr_tok.find('/');
      const std::string left{addr_tok.substr(
        0, slash == std::string_view::npos ? addr_tok.size() : slash)};
      int fam = 0;
      std::array<uint8_t, 16> raw{};
      const bool numeric = PtonAny(left, fam, raw);

      if (!numeric) {
        // Hostname (leading '.' => suffix match). CIDR on a hostname is
        // invalid.
        if (slash != std::string_view::npos) {
          return fail("specifying both host name and CIDR mask is invalid");
        }
        rule.address.kind = AddrMatcher::Kind::Hostname;
        rule.address.hostname = left;
      } else if (slash != std::string_view::npos) {
        // CIDR.
        const std::string_view pfx = addr_tok.substr(slash + 1);
        unsigned prefix = 0;
        const auto [p, ec] =
          std::from_chars(pfx.data(), pfx.data() + pfx.size(), prefix);
        if (ec != std::errc{} || p != pfx.data() + pfx.size() ||
            prefix > static_cast<unsigned>(FamilyWidth(fam))) {
          return fail("invalid CIDR mask in address");
        }
        rule.address.kind = AddrMatcher::Kind::Mask;
        rule.address.family = fam;
        rule.address.addr = raw;
        MaskFromPrefix(prefix, rule.address.mask);
      } else {
        // Bare IP: read a separate netmask column.
        if (fields.size() <= idx) {
          return fail("end-of-line before netmask specification");
        }
        std::string_view mask_tok;
        if (!single(idx, "multiple values specified for netmask", mask_tok)) {
          return std::nullopt;
        }
        ++idx;
        int mfam = 0;
        std::array<uint8_t, 16> mraw{};
        if (!PtonAny(std::string{mask_tok}, mfam, mraw) || mfam != fam) {
          return fail("IP address and mask do not match");
        }
        rule.address.kind = AddrMatcher::Kind::Mask;
        rule.address.family = fam;
        rule.address.addr = raw;
        rule.address.mask = mraw;  // raw bytes: may be non-contiguous
      }
    }
  }

  // Method (one token, case-sensitive).
  if (fields.size() <= idx) {
    return fail("end-of-line before authentication method");
  }
  std::string_view method_tok;
  if (!single(idx, "multiple values specified for authentication type",
              method_tok)) {
    return std::nullopt;
  }
  ++idx;
  const auto method = ParseMethod(method_tok);
  if (!method) {
    return fail(
      absl::StrCat("invalid authentication method \"", method_tok, "\""));
  }
  rule.method = *method;

  // Post-method fixups (hba.c:1774-1823).
  if (is_local && rule.method == Method::Ident) {
    rule.method = Method::Peer;  // local ident => peer
  }
  if (is_local && rule.method == Method::Gss) {
    return fail("gssapi authentication is not supported on local sockets");
  }
  if (!is_local && rule.method == Method::Peer) {
    return fail("peer authentication is only supported on local sockets");
  }
  if (rule.conntype != ConnType::HostSsl && rule.method == Method::Cert) {
    return fail("cert authentication is only supported on hostssl connections");
  }

  // Options: remaining fields, each token name=value.
  for (; idx < fields.size(); ++idx) {
    for (const auto& t : fields[idx]) {
      const auto eq = t.value.find('=');
      if (eq == std::string::npos) {
        return fail(absl::StrCat(
          "authentication option not in name=value format: ", t.value));
      }
      rule.options.push_back({t.value.substr(0, eq), t.value.substr(eq + 1)});
    }
  }

  rule.raw.clear();  // set by caller from the source line
  return rule;
}

}  // namespace

MethodClass MethodExecutability(Method method) {
  switch (method) {
    case Method::ImplicitReject:
    case Method::Trust:
    case Method::Reject:
    case Method::Password:
    case Method::Md5:
    case Method::Scram:
      return MethodClass::Native;
    case Method::Ident:
    case Method::Peer:
      return MethodClass::DeferredPeerIdent;
    case Method::Cert:
      return MethodClass::DeferredCert;
    case Method::Gss:
    case Method::Sspi:
    case Method::Pam:
    case Method::Bsd:
    case Method::Ldap:
    case Method::Oauth:
      return MethodClass::RejectAtConnect;
  }
  return MethodClass::RejectAtConnect;
}

std::optional<Method> ParseMethod(std::string_view name) {
  if (name == "trust") {
    return Method::Trust;
  }
  if (name == "reject") {
    return Method::Reject;
  }
  if (name == "password") {
    return Method::Password;
  }
  if (name == "md5") {
    return Method::Md5;
  }
  if (name == "scram-sha-256") {
    return Method::Scram;
  }
  if (name == "ident") {
    return Method::Ident;
  }
  if (name == "peer") {
    return Method::Peer;
  }
  if (name == "gss") {
    return Method::Gss;
  }
  if (name == "sspi") {
    return Method::Sspi;
  }
  if (name == "pam") {
    return Method::Pam;
  }
  if (name == "bsd") {
    return Method::Bsd;
  }
  if (name == "ldap") {
    return Method::Ldap;
  }
  if (name == "cert") {
    return Method::Cert;
  }
  if (name == "oauth") {
    return Method::Oauth;
  }
  return std::nullopt;
}

std::string_view MethodName(Method method) {
  switch (method) {
    case Method::ImplicitReject:
      return "reject";
    case Method::Trust:
      return "trust";
    case Method::Reject:
      return "reject";
    case Method::Password:
      return "password";
    case Method::Md5:
      return "md5";
    case Method::Scram:
      return "scram-sha-256";
    case Method::Ident:
      return "ident";
    case Method::Peer:
      return "peer";
    case Method::Gss:
      return "gss";
    case Method::Sspi:
      return "sspi";
    case Method::Pam:
      return "pam";
    case Method::Bsd:
      return "bsd";
    case Method::Ldap:
      return "ldap";
    case Method::Cert:
      return "cert";
    case Method::Oauth:
      return "oauth";
  }
  return "reject";
}

// check_db / check_role (hba.c:949-1030), branched on field.
bool NameMatcher::Matches(const ClientInfo& client,
                          const MembershipFn& is_member) const {
  const std::string_view subject =
    field == Field::Database ? client.database : client.user;

  for (const auto& t : tokens) {
    // Regex first (before keyword-vs-literal), regardless of quoted.
    if (t.regex) {
      if (re2::RE2::PartialMatch(subject, *t.regex)) {
        return true;
      }
      continue;
    }
    if (field == Field::Database) {
      if (client.is_replication) {
        // A physical-replication conn matches only the `replication` keyword.
        if (!t.quoted && t.value == "replication") {
          return true;
        }
        continue;
      }
      if (!t.quoted && t.value == "all") {
        return true;
      }
      if (!t.quoted && t.value == "sameuser") {
        if (client.database == client.user) {
          return true;
        }
        continue;
      }
      if (!t.quoted && (t.value == "samerole" || t.value == "samegroup")) {
        if (is_member && is_member(client.user, client.database)) {
          return true;
        }
        continue;
      }
      if (!t.quoted && t.value == "replication") {
        continue;  // never matches a normal connection
      }
      if (t.value == subject) {
        return true;  // exact literal
      }
    } else {  // Role field
      if (!t.quoted && t.value == "all") {
        return true;
      }
      if (!t.quoted && t.value.size() > 1 && t.value.front() == '+') {
        if (is_member && is_member(client.user, t.value.substr(1))) {
          return true;
        }
        continue;
      }
      if (t.value == subject) {
        return true;  // exact literal
      }
    }
  }
  return false;
}

bool AddrMatcher::Contains(const std::array<uint8_t, 16>& candidate) const {
  for (int i = 0; i < 16; ++i) {
    if ((candidate[i] & mask[i]) != (addr[i] & mask[i])) {
      return false;
    }
  }
  return true;
}

namespace {

// The IPv4-mapped IPv6 form of a client's v4 address (::ffff:a.b.c.d): 10 zero
// bytes, 0xff, 0xff, then the 4 v4 bytes (RFC 4291). Lets a v4 client be tested
// against an IPv6 rule, matching PostgreSQL's check_ip promotion.
std::array<uint8_t, 16> V4Mapped(const std::array<uint8_t, 16>& v4) {
  std::array<uint8_t, 16> mapped{};
  mapped[10] = 0xff;
  mapped[11] = 0xff;
  std::copy_n(v4.begin(), 4, mapped.begin() + 12);
  return mapped;
}

}  // namespace

bool AddrMatcher::Matches(int client_family,
                          const std::array<uint8_t, 16>& client_addr) const {
  if (kind == Kind::All) {
    return true;
  }
  if (kind != Kind::Mask) {
    return false;  // SameHost/SameNet/Hostname are handled by Decide, not here
  }
  if (client_family == family) {
    return Contains(client_addr);
  }

  if (client_family == AF_INET && family == AF_INET6) {
    return Contains(V4Mapped(client_addr));
  }

  return false;
}

Decision Decide(const Ruleset& ruleset, const ClientInfo& client,
                const MembershipFn& is_member) {
  for (const auto& rule : ruleset.rules) {
    // 1. Conn-type + SSL/GSS (hba.c:2352-2387).
    if (rule.conntype == ConnType::Local) {
      if (!client.is_local) {
        continue;
      }
    } else {
      if (client.is_local) {
        continue;
      }
      if (client.is_ssl && rule.conntype == ConnType::HostNoSsl) {
        continue;
      }
      if (!client.is_ssl && rule.conntype == ConnType::HostSsl) {
        continue;
      }
      // GSS: SDB is_gss is always false, so HostGss never matches.
      if (rule.conntype == ConnType::HostGss && !client.is_gss) {
        continue;
      }
      // HostNoGss always passes the GSS axis when !is_gss.
    }

    // 2. Address (non-local). Deferred kinds can't be evaluated cheaply; run
    // db/role first and only then declare a deferred verdict, so a later native
    // rule still gets its chance.
    const bool deferred_addr =
      !client.is_local && (rule.address.kind == AddrMatcher::Kind::SameHost ||
                           rule.address.kind == AddrMatcher::Kind::SameNet ||
                           rule.address.kind == AddrMatcher::Kind::Hostname);
    if (!client.is_local && !deferred_addr &&
        !rule.address.Matches(client.family, client.addr)) {
      continue;
    }

    // 3. Database, 4. Role.
    if (!rule.database.Matches(client, is_member)) {
      continue;
    }
    if (!rule.role.Matches(client, is_member)) {
      continue;
    }

    // This rule is the match.
    if (deferred_addr) {
      return {Decision::Kind::MatchedHostnameDeferred,
              Method::ImplicitReject,
              {},
              "hostname/samehost/samenet matching is not supported",
              &rule};
    }
    switch (MethodExecutability(rule.method)) {
      case MethodClass::Native:
        if (rule.method == Method::Trust) {
          return {Decision::Kind::Trust, rule.method, {}, {}, &rule};
        }
        if (rule.method == Method::Reject) {
          return {Decision::Kind::Reject, rule.method, {}, {}, &rule};
        }
        return {Decision::Kind::Method, rule.method, rule.options, {}, &rule};
      case MethodClass::DeferredPeerIdent:
        // peer executes at the session (SO_PEERCRED, unix sockets only);
        // ident (TCP) is rejected there with this reason.
        return {Decision::Kind::DeferredPeerIdent,
                rule.method,
                {},
                absl::StrCat("\"", MethodName(rule.method),
                             "\" authentication is not supported"),
                &rule};
      case MethodClass::DeferredCert:
        // cert executes at the session (verified client-cert CN, hostssl only).
        return {
          Decision::Kind::DeferredCert, rule.method, rule.options, {}, &rule};
      case MethodClass::RejectAtConnect:
        return {Decision::Kind::Unsupported,
                rule.method,
                {},
                absl::StrCat("\"", MethodName(rule.method),
                             "\" authentication is not supported"),
                &rule};
    }
  }
  return {Decision::Kind::Reject, Method::ImplicitReject, {}, {}, nullptr};
}

std::optional<Ruleset> Parse(std::string_view text, ParseError& error) {
  Ruleset ruleset;
  uint32_t seq = 0;
  const FileReader no_reader;  // null => @file/include expand to nothing

  for (const auto& logical : SpliceLines(text)) {
    auto fields = TokenizeLine(logical.text, no_reader);
    if (fields.empty()) {
      continue;  // blank or comment line
    }
    if (IsIncludeDirective(fields)) {
      continue;  // recognized; no-op without a file reader
    }
    auto rule = ParseLine(fields, seq, error, logical.line_no);
    if (!rule) {
      return std::nullopt;
    }
    // Verbatim line for the view (strip a trailing comment for readability).
    std::string_view raw = logical.text;
    rule->raw = std::string{raw};
    ruleset.rules.push_back(std::move(*rule));
    ++seq;
  }
  return ruleset;
}

// --- Live ruleset holder -----------------------------------------------------

namespace {

// The un-removable safety net, force-prepended onto every ruleset (the built-in
// default and any user-authored one) so a bad ruleset can never lock the admin
// out (CockroachDB's mandatory-root-rule shape). Scoped to the BOOTSTRAP
// SUPERUSER: it can always authenticate locally (unix socket + loopback),
// while every other role falls through to the rest of the ruleset -- so
// loopback can be password-gated or rejected for non-superusers. Built at
// runtime because it embeds the superuser's name; it flows through the same
// parser as any authored ruleset.
std::string SafetyRules() {
  const std::string_view super = StaticStrings::kDefaultUser;
  return absl::StrCat("local all ", super, " trust\n", "host all ", super,
                      " 127.0.0.1/32 trust\n", "host all ", super,
                      " ::1/128 trust\n");
}

// The default ruleset when no `hba` is configured. Every role authenticates via
// SCRAM against its stored credential, locally and remotely alike, so a role
// with no password cannot log in (as in PostgreSQL). The one exception is the
// bootstrap superuser's passwordless local access, which comes from the
// force-prepended SafetyRules -- not from this text -- so the zero-config local
// launch stays friction-free for the superuser while everyone else fails
// closed.
constexpr std::string_view kDefaultRules =
  "local all all scram-sha-256\n"
  "host all all 0.0.0.0/0 scram-sha-256\n"
  "host all all ::0/0 scram-sha-256\n";

// Renumber a ruleset's rule seqs to their index (used after prepend/merge).
void Renumber(Ruleset& rs) {
  for (uint32_t i = 0; i < rs.rules.size(); ++i) {
    rs.rules[i].seq = i;
  }
}

Ruleset ParseTrusted(std::string_view text) {
  ParseError err;
  auto rs = Parse(text, err);
  // The safety text is a compile-time constant; a parse failure is a bug.
  return rs.value_or(Ruleset{});
}

// The live ruleset, accessed via std::atomic_load/store free functions (the
// codebase idiom for atomic shared_ptr -- see catalog.cpp / inverted_index_
// storage.h; the C++20 std::atomic<shared_ptr> specialization is unavailable in
// this libc++). Lazily initialized to the default on first access so a session
// before any SET never sees a null ruleset.
std::shared_ptr<const Ruleset>& LiveRuleset() {
  static std::shared_ptr<const Ruleset> gRuleset =
    std::make_shared<const Ruleset>(DefaultRuleset());
  return gRuleset;
}

}  // namespace

Ruleset DefaultRuleset() {
  return ParseTrusted(absl::StrCat(SafetyRules(), kDefaultRules));
}

std::shared_ptr<const Ruleset> GetHbaRuleset() {
  return std::atomic_load(&LiveRuleset());
}

void SetHbaRuleset(std::shared_ptr<const Ruleset> ruleset) {
  std::atomic_store(&LiveRuleset(), std::move(ruleset));
}

std::optional<ParseError> SetHbaFromText(std::string_view text) {
  ParseError err;
  auto parsed = Parse(text, err);
  if (!parsed) {
    return err;  // keep-last-good: live ruleset untouched
  }
  Ruleset combined = ParseTrusted(SafetyRules());
  // Force-prepend the safety rules unless the user's ruleset already begins
  // with the exact same escape hatch (compare the leading lines verbatim), so
  // we don't duplicate them.
  const bool already_safe =
    parsed->rules.size() >= combined.rules.size() &&
    std::ranges::equal(combined.rules,
                       parsed->rules | std::views::take(combined.rules.size()),
                       {}, &Rule::raw, &Rule::raw);
  if (already_safe) {
    combined = std::move(*parsed);
  } else {
    std::ranges::move(parsed->rules, std::back_inserter(combined.rules));
  }
  Renumber(combined);
  SetHbaRuleset(std::make_shared<const Ruleset>(std::move(combined)));
  return std::nullopt;
}

namespace {

// The resolved path of the HBA config file, and a lock serializing the
// persist-then-swap pair so concurrent SETs cannot leave the file and the live
// ruleset disagreeing. The path is set once at startup, before any connection.
std::string& ConfigPath() {
  static std::string gPath;
  return gPath;
}

absl::Mutex& PersistMutex() {
  static absl::Mutex gMutex;
  return gMutex;
}

// Atomically replace the config file with `text`: write a fsync'd sibling temp
// file and rename it over the target, so a crash mid-write never leaves a torn
// config. Returns an error string on failure, nullopt on success or when no
// path is configured (nothing to persist).
std::optional<std::string> WriteConfigFile(std::string_view text) {
  const std::string& path = ConfigPath();
  if (path.empty()) {
    return std::nullopt;  // no data directory (e.g. a unit test): live-only
  }
  namespace fs = std::filesystem;
  const fs::path target{path};
  std::error_code dir_ec;
  if (const auto parent = target.parent_path();
      !parent.empty() && !fs::exists(parent, dir_ec)) {
    fs::create_directories(parent, dir_ec);
    if (dir_ec) {
      return absl::StrCat("could not create hba config directory '",
                          parent.string(), "': ", dir_ec.message());
    }
  }
  const std::string tmp = target.string() + ".tmp";
  try {
    basics::file_utils::Spit(tmp, text, /*sync=*/true);
  } catch (const std::exception& e) {
    return absl::StrCat("could not write hba config file '", tmp,
                        "': ", e.what());
  }
  std::error_code ec;
  fs::rename(tmp, target, ec);
  if (ec) {
    fs::remove(tmp, ec);
    return absl::StrCat("could not install hba config file '", target.string(),
                        "': ", ec.message());
  }
  return std::nullopt;
}

}  // namespace

void SetHbaConfig(std::string path) { ConfigPath() = std::move(path); }

std::string_view ConfigFilePath() { return ConfigPath(); }

std::optional<std::string> SetHbaFromTextString(std::string_view text) {
  // Validate the ruleset without mutating the live one yet.
  ParseError perr;
  if (!Parse(text, perr)) {
    if (perr.line > 0) {
      return absl::StrCat("line ", perr.line, ": ", perr.message);
    }
    return perr.message;
  }
  // Persist first, then publish, under one lock: on a write failure we have not
  // swapped, so the SET fails cleanly and the file and live ruleset never
  // diverge; on restart the on-disk file is always what is running.
  absl::MutexLock lock{&PersistMutex()};
  if (auto werr = WriteConfigFile(text)) {
    return werr;
  }
  SetHbaFromText(text);  // re-parses + prepends safety rules; cannot fail here
  return std::nullopt;
}

namespace {

std::string_view ConnTypeName(ConnType t) {
  switch (t) {
    case ConnType::Local:
      return "local";
    case ConnType::Host:
      return "host";
    case ConnType::HostSsl:
      return "hostssl";
    case ConnType::HostNoSsl:
      return "hostnossl";
    case ConnType::HostGss:
      return "hostgssenc";
    case ConnType::HostNoGss:
      return "hostnogssenc";
  }
  return "host";
}

// Render raw address bytes back to text (inverse of inet_pton at parse).
std::string AddrToText(int family, const std::array<uint8_t, 16>& bytes) {
  char buf[INET6_ADDRSTRLEN] = {};
  if (inet_ntop(family, bytes.data(), buf, sizeof(buf))) {
    return buf;
  }
  return {};
}

// Render the CIDR prefix length back to text, e.g. "127.0.0.0/8". A
// non-contiguous mask (from the separate-netmask column) has no prefix form, so
// the caller falls back to emitting the netmask separately.
std::optional<int> ContiguousPrefix(const std::array<uint8_t, 16>& mask,
                                    int family) {
  const int width = family == AF_INET6 ? 16 : 4;
  int bits = 0;
  bool seen_zero = false;
  for (int i = 0; i < width; ++i) {
    for (int b = 7; b >= 0; --b) {
      const bool set = (mask[i] >> b) & 1;
      if (set) {
        if (seen_zero) {
          return std::nullopt;  // 1 after a 0 => non-contiguous
        }
        ++bits;
      } else {
        seen_zero = true;
      }
    }
  }
  return bits;
}

}  // namespace

std::vector<RenderedRule> RenderHbaRules() {
  const auto ruleset = GetHbaRuleset();
  std::vector<RenderedRule> out;
  if (!ruleset) {
    return out;
  }
  const std::string_view file = ConfigFilePath();
  for (const auto& rule : ruleset->rules) {
    RenderedRule r;
    r.rule_number = rule.seq + 1;  // 1-based, like PG
    r.file_name = file;
    r.type = ConnTypeName(rule.conntype);
    std::ranges::transform(rule.database.tokens,
                           std::back_inserter(r.databases), &AuthToken::value);
    std::ranges::transform(rule.role.tokens, std::back_inserter(r.roles),
                           &AuthToken::value);
    r.auth_method = MethodName(rule.method);
    std::ranges::transform(
      rule.options, std::back_inserter(r.options),
      [](const MethodOption& o) { return absl::StrCat(o.name, "=", o.value); });
    switch (rule.address.kind) {
      case AddrMatcher::Kind::All:
        if (rule.conntype != ConnType::Local) {
          r.address = "all";
        }
        break;
      case AddrMatcher::Kind::SameHost:
        r.address = "samehost";
        break;
      case AddrMatcher::Kind::SameNet:
        r.address = "samenet";
        break;
      case AddrMatcher::Kind::Hostname:
        r.address = rule.address.hostname;
        break;
      case AddrMatcher::Kind::Mask: {
        const int fam = rule.address.family;
        const auto base = AddrToText(fam, rule.address.addr);
        if (const auto prefix = ContiguousPrefix(rule.address.mask, fam)) {
          r.address = absl::StrCat(base, "/", *prefix);  // CIDR form
        } else {
          r.address = base;  // non-contiguous: address + separate netmask
          r.netmask = AddrToText(fam, rule.address.mask);
        }
        break;
      }
    }
    out.push_back(std::move(r));
  }
  return out;
}

void LoadPersistedHba() {
  namespace fs = std::filesystem;
  const std::string& path = ConfigPath();
  if (path.empty()) {
    return;  // no data directory configured -> keep the built-in default
  }
  std::error_code ec;
  if (!fs::exists(fs::path{path}, ec) || ec) {
    return;  // no config file -> keep the built-in default
  }
  std::string text;
  try {
    text = basics::file_utils::Slurp(path);
  } catch (const std::exception& e) {
    SDB_ERROR(GENERAL, "hba config file '", path,
              "' could not be read: ", e.what(), " -- keeping default");
    return;
  }
  if (auto err = SetHbaFromText(text)) {
    SDB_ERROR(GENERAL, "hba config file '", path, "' failed to parse (line ",
              err->line, "): ", err->message, " -- keeping default");
  }
}

}  // namespace sdb::network::pg::hba
