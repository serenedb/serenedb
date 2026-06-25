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

#include "network/listen_spec.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>
#include <ada.h>
#include <fast_float/fast_float.h>

#include <charconv>
#include <set>

#include "basics/log.h"
#include "basics/string_utils.h"

namespace sdb::network {
namespace {

std::string PercentDecode(std::string_view s) {
  return ada::unicode::percent_decode(s, s.find('%'));
}

bool ParseBoolParam(std::string_view v, std::string_view key,
                    std::string_view url) {
  if (const auto parsed = basics::ParseBool(v)) {
    return *parsed;
  }
  SDB_FATAL(GENERAL, "invalid boolean for '", key, "' in endpoint '", url,
            "': '", v, "'");
}

uint32_t ParseUintParam(std::string_view v, std::string_view key,
                        std::string_view url, int base = 10) {
  uint32_t value = 0;
  const auto* end = v.data() + v.size();
  const auto [ptr, ec] = fast_float::from_chars(v.data(), end, value, base);
  if (ec != std::errc{} || ptr != end) {
    SDB_FATAL(GENERAL, "invalid number for '", key, "' in endpoint '", url,
              "': '", v, "'");
  }
  return value;
}

uint16_t ParsePort(std::string_view port_str, std::string_view url) {
  uint16_t port = 0;
  const auto* end = port_str.data() + port_str.size();
  const auto [ptr, ec] = fast_float::from_chars(port_str.data(), end, port);
  if (ec != std::errc{} || ptr != end || port == 0) {
    SDB_FATAL(GENERAL, "missing or invalid port in endpoint '", url, "'");
  }
  return port;
}

SslMode ParseSslMode(std::string_view v, std::string_view url) {
  if (v == "disable") {
    return SslMode::Disable;
  }
  if (v == "allow") {
    return SslMode::Allow;
  }
  if (v == "prefer") {
    return SslMode::Prefer;
  }
  if (v == "require") {
    return SslMode::Require;
  }
  if (v == "verify-ca") {
    return SslMode::VerifyCa;
  }
  if (v == "verify-full") {
    return SslMode::VerifyFull;
  }
  SDB_FATAL(GENERAL, "invalid sslmode in endpoint '", url, "': '", v, "'");
}

void ApplyParam(ListenSpec& spec, std::string_view key,
                std::string_view value) {
  const std::string_view url = spec.url;
  const bool is_http = spec.protocol == ListenProtocol::Http;
  const bool is_unix = spec.transport == ListenTransport::Unix;
  if (key == "sslmode") {
    if (is_http) {
      SDB_FATAL(GENERAL, "'sslmode' is not valid on an http endpoint '", url,
                "' (http TLS is the scheme: use https://)");
    }
    if (is_unix) {
      SDB_FATAL(GENERAL, "'sslmode' is not valid on a unix endpoint '", url,
                "' (unix sockets never use TLS)");
    }
    spec.sslmode = ParseSslMode(value, url);
  } else if (key == "cert" || key == "key" || key == "ca") {
    if (is_unix) {
      SDB_FATAL(GENERAL, "TLS material is not valid on a unix endpoint '", url,
                "'");
    }
    if (is_http && !spec.https) {
      SDB_FATAL(GENERAL,
                "TLS material is not valid on a plaintext http endpoint '", url,
                "' (use https://)");
    }
    if (key == "cert") {
      spec.cert = PercentDecode(value);
    } else if (key == "key") {
      spec.key = PercentDecode(value);
    } else {
      spec.ca = PercentDecode(value);
    }
  } else if (key == "api") {
    if (!is_http) {
      SDB_FATAL(GENERAL, "'api' is only valid on an http endpoint '", url, "'");
    }
    for (std::string_view a : absl::StrSplit(value, ',', absl::SkipEmpty())) {
      if (a != "es" && a != "test") {
        SDB_FATAL(GENERAL, "unknown api '", a, "' in endpoint '", url,
                  "' (known: es, test)");
      }
      spec.apis.emplace_back(a);
    }
  } else if (key == "mode") {
    if (!is_unix) {
      SDB_FATAL(GENERAL, "'mode' is only valid on a unix endpoint '", url, "'");
    }
    spec.unix_mode = ParseUintParam(value, key, url, 8);
  } else if (key == "group") {
    if (!is_unix) {
      SDB_FATAL(GENERAL, "'group' is only valid on a unix endpoint '", url,
                "'");
    }
    spec.unix_group = PercentDecode(value);
  } else if (key == "port") {
    if (!is_unix) {
      SDB_FATAL(GENERAL,
                "'port' query is only for a unix pg socket (tcp uses "
                "host:port) in endpoint '",
                url, "'");
    }
    if (is_http) {
      SDB_FATAL(GENERAL, "'port' is not valid on a unix http endpoint '", url,
                "'");
    }
    spec.unix_port = static_cast<uint16_t>(ParseUintParam(value, key, url));
  } else if (key == "backlog") {
    spec.backlog = static_cast<int>(ParseUintParam(value, key, url));
  } else if (key == "reuseport") {
    if (is_unix) {
      SDB_FATAL(GENERAL, "'reuseport' is not valid on a unix endpoint '", url,
                "'");
    }
    spec.reuseport = ParseBoolParam(value, key, url);
  } else if (key == "keepalive") {
    spec.keepalive = ParseBoolParam(value, key, url);
  } else if (key == "keepidle") {
    spec.keepidle = ParseUintParam(value, key, url);
  } else if (key == "keepintvl") {
    spec.keepintvl = ParseUintParam(value, key, url);
  } else if (key == "keepcnt") {
    spec.keepcnt = ParseUintParam(value, key, url);
  } else if (key == "max_connections") {
    spec.max_connections = ParseUintParam(value, key, url);
  } else if (key == "proxy_protocol") {
    if (value == "off") {
      spec.proxy = ProxyMode::Off;
    } else if (value == "optional") {
      spec.proxy = ProxyMode::Optional;
    } else if (value == "require") {
      spec.proxy = ProxyMode::Require;
    } else {
      SDB_FATAL(GENERAL, "invalid proxy_protocol in endpoint '", url, "': '",
                value, "' (off|optional|require)");
    }
  } else {
    SDB_FATAL(GENERAL, "unknown parameter '", key, "' in endpoint '", url, "'");
  }
}

std::vector<asio_ns::ip::tcp::endpoint> ResolveTcp(
  std::string_view host, uint16_t port, std::string_view url,
  asio_ns::io_context& ctx, bool& v6_only_out, bool wildcard) {
  std::vector<asio_ns::ip::tcp::endpoint> eps;
  if (wildcard) {
    eps.emplace_back(asio_ns::ip::make_address("0.0.0.0"), port);
    eps.emplace_back(asio_ns::ip::make_address("::"), port);
    v6_only_out = true;
    return eps;
  }
  asio_ns::error_code ec;
  const auto addr = asio_ns::ip::make_address(std::string{host}, ec);
  if (!ec) {
    eps.emplace_back(addr, port);
    v6_only_out = addr.is_v6();
    return eps;
  }
  asio_ns::ip::tcp::resolver resolver{ctx};
  const auto results = resolver.resolve(
    host, absl::StrCat(port), asio_ns::ip::tcp::resolver::passive, ec);
  if (ec) {
    SDB_FATAL(GENERAL, "cannot resolve host '", host, "' in endpoint '", url,
              "': ", ec.message());
  }
  containers::FlatHashSet<std::string> seen;
  for (const auto& entry : results) {
    auto ep = entry.endpoint();
    if (seen.emplace(ep.address().to_string()).second) {
      eps.push_back(ep);
    }
  }
  if (eps.empty()) {
    SDB_FATAL(GENERAL, "host '", host, "' resolved to nothing in endpoint '",
              url, "'");
  }
  v6_only_out = true;
  return eps;
}

void ParseOne(std::string_view url, asio_ns::io_context& resolve_ctx,
              std::vector<ListenSpec>& out) {
  // Determine the scheme textually: http/https are WHATWG "special" schemes
  // that ada parses differently for the empty-authority (unix) form, so we
  // classify transport from the raw string and only hand the TCP authority case
  // to ada.
  const auto scheme_end = url.find("://");
  if (scheme_end == std::string_view::npos) {
    SDB_FATAL(GENERAL, "invalid network endpoint '", url,
              "' (expected scheme://...)");
  }
  const std::string scheme = absl::AsciiStrToLower(url.substr(0, scheme_end));

  ListenSpec base;
  base.url = url;
  if (scheme == "postgres" || scheme == "postgresql") {
    base.protocol = ListenProtocol::Pg;
  } else if (scheme == "http") {
    base.protocol = ListenProtocol::Http;
  } else if (scheme == "https") {
    base.protocol = ListenProtocol::Http;
    base.https = true;
  } else {
    SDB_FATAL(GENERAL, "unsupported scheme '", scheme, "' in endpoint '", url,
              "' (postgres, postgresql, http, https)");
  }

  std::string_view rest = url.substr(scheme_end + 3);
  const auto qpos = rest.find('?');
  const std::string_view authority =
    qpos == std::string_view::npos ? rest : rest.substr(0, qpos);
  const std::string_view query =
    qpos == std::string_view::npos ? std::string_view{} : rest.substr(qpos + 1);

  const auto parse_query = [&](std::string_view search) {
    for (std::string_view pair :
         absl::StrSplit(search, '&', absl::SkipEmpty())) {
      const auto eq = pair.find('=');
      if (eq == std::string_view::npos) {
        SDB_FATAL(GENERAL, "malformed parameter '", pair, "' in endpoint '",
                  url, "'");
      }
      ApplyParam(base, PercentDecode(pair.substr(0, eq)), pair.substr(eq + 1));
    }
  };

  // Empty authority (`scheme:///path`) => unix-domain socket.
  if (authority.starts_with('/')) {
    base.transport = ListenTransport::Unix;
    if (base.https) {
      SDB_FATAL(GENERAL, "TLS (https) is not supported on a unix endpoint '",
                url, "'");
    }
    parse_query(query);
    if (base.protocol == ListenProtocol::Http && base.apis.empty()) {
      SDB_FATAL(GENERAL, "http endpoint '", url,
                "' requires ?api= (e.g. ?api=es); there is no default api");
    }
    std::string p = PercentDecode(authority);
    if (p.size() >= 2 && p[0] == '/' && p[1] == '@') {
      base.unix_abstract = true;
      base.unix_path = p.substr(2);
    } else {
      base.unix_path = std::move(p);
    }
    out.push_back(std::move(base));
    return;
  }

  base.transport = ListenTransport::Tcp;
  const auto parsed = ada::parse(url);
  if (!parsed) {
    SDB_FATAL(GENERAL, "invalid network endpoint '", url, "'");
  }
  std::string_view host = parsed->get_hostname();
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  const std::string_view port_str = parsed->get_port();

  parse_query(query);
  if (base.protocol == ListenProtocol::Http && base.apis.empty()) {
    SDB_FATAL(GENERAL, "http endpoint '", url,
              "' requires ?api= (e.g. ?api=es); there is no default api");
  }

  const bool wildcard = host == "*";
  const uint16_t port = ParsePort(port_str, url);
  bool v6_only = false;
  const auto eps = ResolveTcp(host, port, url, resolve_ctx, v6_only, wildcard);
  for (const auto& ep : eps) {
    ListenSpec spec = base;
    spec.endpoint = ep;
    spec.v6_only = ep.address().is_v6();
    out.push_back(std::move(spec));
  }
}

}  // namespace

std::vector<ListenSpec> ParseListenSpecs(
  const std::vector<std::string>& urls_in) {
  std::vector<std::string> urls;
  for (const auto& u : urls_in) {
    for (std::string_view part : absl::StrSplit(u, ',', absl::SkipEmpty())) {
      std::string_view t = absl::StripAsciiWhitespace(part);
      if (!t.empty()) {
        urls.emplace_back(t);
      }
    }
  }
  if (urls.empty()) {
    urls.emplace_back("postgres://127.0.0.1:7890");
  }
  std::vector<ListenSpec> out;
  asio_ns::io_context resolve_ctx;
  for (const auto& url : urls) {
    ParseOne(url, resolve_ctx, out);
  }
  if (out.empty()) {
    SDB_FATAL(GENERAL, "no listeners configured");
  }
  return out;
}

}  // namespace sdb::network
