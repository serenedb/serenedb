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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <ada.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <charconv>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/string_utils.hpp>

#include "network/listen_protocol.h"

namespace sdb::network {
namespace {

uint16_t ParsePort(std::string_view port_str, std::string_view url) {
  uint16_t port = 0;
  const auto* end = port_str.data() + port_str.size();
  const auto [ptr, ec] = fast_float::from_chars(port_str.data(), end, port);
  if (ec != std::errc{} || ptr != end || port == 0) {
    SDB_FATAL(GENERAL, "missing or invalid port in endpoint '", url, "'");
  }
  return port;
}

void ApplyParam(ListenSpec& spec, std::string_view key, std::string_view value,
                std::vector<ListenParam>& own) {
  const std::string_view url = spec.url;
  const bool is_unix = spec.transport == ListenTransport::Unix;
  if (key == "cert" || key == "key" || key == "ca") {
    if (is_unix) {
      SDB_FATAL(GENERAL, "TLS material is not valid on a unix endpoint '", url,
                "'");
    }
    if (key == "cert") {
      spec.cert = PercentDecode(value);
    } else if (key == "key") {
      spec.key = PercentDecode(value);
    } else {
      spec.ca = PercentDecode(value);
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
    own.push_back({.key = std::string{key}, .value = value});
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
  const auto addr = asio_ns::ip::make_address(host, ec);
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
  irs::containers::FlatHashSet<std::string> seen;
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
  const auto parsed = ada::parse<ada::url_aggregator>(url);
  if (!parsed || !parsed->has_hostname()) {
    SDB_FATAL(GENERAL, "invalid network endpoint '", url,
              "' (expected scheme://...)");
  }
  if (parsed->is_special() && url.find('\\') != std::string_view::npos) {
    SDB_FATAL(GENERAL, "invalid network endpoint '", url,
              "': '\\' is not allowed");
  }
  std::string_view scheme = parsed->get_protocol();
  scheme.remove_suffix(1);

  ListenSpec base;
  base.url = url;
  base.protocol = Registry<ListenProtocol>::Find(scheme);
  if (!base.protocol) {
    SDB_FATAL(GENERAL, "unsupported scheme '", scheme, "' in endpoint '", url,
              "' (", absl::StrJoin(Registry<ListenProtocol>::Names(), ", "),
              ")");
  }

  const auto parse_query = [&] {
    std::string_view search = parsed->get_search();
    search.remove_prefix(std::min<size_t>(search.size(), 1));
    std::vector<ListenParam> own;
    for (std::string_view pair :
         absl::StrSplit(search, '&', absl::SkipEmpty())) {
      const auto eq = pair.find('=');
      if (eq == std::string_view::npos) {
        SDB_FATAL(GENERAL, "malformed parameter '", pair, "' in endpoint '",
                  url, "'");
      }
      ApplyParam(base, PercentDecode(pair.substr(0, eq)), pair.substr(eq + 1),
                 own);
    }
    base.options = base.protocol->parse(base, own);
  };

  if (parsed->has_empty_hostname()) {
    base.transport = ListenTransport::Unix;
    parse_query();
    std::string path = PercentDecode(parsed->get_pathname());
    if (path.size() >= 2 && path[0] == '/' && path[1] == '@') {
      base.unix_abstract = true;
      base.unix_path = path.substr(2);
    } else {
      base.unix_path = std::move(path);
    }
    out.push_back(std::move(base));
    return;
  }

  base.transport = ListenTransport::Tcp;
  std::string_view host = parsed->get_hostname();
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  parse_query();

  const bool wildcard = host == "*";
  const std::string_view port_str = parsed->get_port();
  const uint16_t port = port_str.empty() && parsed->scheme_default_port() != 0
                          ? parsed->scheme_default_port()
                          : ParsePort(port_str, url);
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

std::string PercentDecode(std::string_view value) {
  return ada::unicode::percent_decode(value, value.find('%'));
}

bool ParseBoolParam(std::string_view value, std::string_view key,
                    std::string_view url) {
  if (const auto parsed = irs::utils::ParseBool(value)) {
    return *parsed;
  }
  SDB_FATAL(GENERAL, "invalid boolean for '", key, "' in endpoint '", url,
            "': '", value, "'");
}

uint32_t ParseUintParam(std::string_view value, std::string_view key,
                        std::string_view url, int base) {
  uint32_t parsed = 0;
  const auto* end = value.data() + value.size();
  const auto [ptr, ec] =
    fast_float::from_chars(value.data(), end, parsed, base);
  if (ec != std::errc{} || ptr != end) {
    SDB_FATAL(GENERAL, "invalid number for '", key, "' in endpoint '", url,
              "': '", value, "'");
  }
  return parsed;
}

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
