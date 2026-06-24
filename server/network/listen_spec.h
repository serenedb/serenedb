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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "basics/asio_ns.h"

namespace sdb::network {

enum class ListenProtocol : uint8_t { Pg, Http };

enum class ListenTransport : uint8_t { Tcp, Unix };

enum class SslMode : uint8_t {
  Disable,
  Allow,
  Prefer,
  Require,
  VerifyCa,
  VerifyFull,
};

enum class ProxyMode : uint8_t { Off, Optional, Require };

struct ListenSpec {
  std::string url;
  ListenProtocol protocol = ListenProtocol::Pg;
  ListenTransport transport = ListenTransport::Tcp;

  asio_ns::ip::tcp::endpoint endpoint;
  bool v6_only = false;

  std::string unix_path;
  bool unix_abstract = false;
  std::optional<uint32_t> unix_mode;
  std::string unix_group;
  // For a pg unix listener: when set, the socket file is named
  // <unix_path>/.s.PGSQL.<unix_port> (libpq-compatible; connect with
  // host=<unix_path> port=<unix_port>). Unset => the literal unix_path is used.
  std::optional<uint16_t> unix_port;

  SslMode sslmode = SslMode::Prefer;
  bool https = false;
  std::string cert;
  std::string key;
  std::string ca;

  std::vector<std::string> apis;

  std::optional<int> backlog;
  std::optional<bool> reuseport;
  std::optional<bool> keepalive;
  std::optional<uint32_t> keepidle;
  std::optional<uint32_t> keepintvl;
  std::optional<uint32_t> keepcnt;
  std::optional<uint32_t> max_connections;
  ProxyMode proxy = ProxyMode::Off;

  bool TlsCapable() const {
    if (protocol == ListenProtocol::Http) {
      return https;
    }
    return transport == ListenTransport::Tcp && sslmode != SslMode::Disable;
  }
  bool RequireTls() const {
    return protocol == ListenProtocol::Pg &&
           (sslmode == SslMode::Require || sslmode == SslMode::VerifyCa ||
            sslmode == SslMode::VerifyFull);
  }
  bool RequireClientCert() const {
    return sslmode == SslMode::VerifyCa || sslmode == SslMode::VerifyFull;
  }
};

// Parse the --listen flag value list into concrete listener specs: ada URL
// parse (strict; fatal on any malformed/inapplicable input), DNS/wildcard
// resolution into one spec per bound address, and the historical default when
// empty. Fatal (SDB_FATAL) on any error -- this runs once at startup.
std::vector<ListenSpec> ParseListenSpecs(const std::vector<std::string>& urls);

}  // namespace sdb::network
