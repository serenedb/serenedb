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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "network/proxy_protocol.h"
#include "server/utils/asio_ns.h"

namespace sdb::network {

enum class ListenTransport : uint8_t {
  Tcp,
  Unix,
};

struct ListenOptions {
  virtual ~ListenOptions() = default;
};

struct ListenParam {
  std::string key;
  std::string_view value;
};

struct ListenProtocol;

struct ListenSpec {
  std::string url;
  const ListenProtocol* protocol = nullptr;
  ListenTransport transport = ListenTransport::Tcp;

  asio_ns::ip::tcp::endpoint endpoint;
  bool v6_only = false;

  std::string unix_path;
  bool unix_abstract = false;
  std::optional<uint32_t> unix_mode;
  std::string unix_group;

  std::string cert;
  std::string key;
  std::string ca;

  std::optional<int> backlog;
  std::optional<bool> reuseport;
  std::optional<bool> keepalive;
  std::optional<uint32_t> keepidle;
  std::optional<uint32_t> keepintvl;
  std::optional<uint32_t> keepcnt;
  std::optional<uint32_t> max_connections;
  ProxyMode proxy = ProxyMode::Off;

  std::shared_ptr<const ListenOptions> options;
};

// Parse the --listen flag value list into concrete listener specs: ada URL
// parse (strict; fatal on any malformed/inapplicable input), DNS/wildcard
// resolution into one spec per bound address, and the historical default when
// empty. Fatal (SDB_FATAL) on any error -- this runs once at startup.
std::vector<ListenSpec> ParseListenSpecs(const std::vector<std::string>& urls);

std::string PercentDecode(std::string_view value);

bool ParseBoolParam(std::string_view value, std::string_view key,
                    std::string_view url);

uint32_t ParseUintParam(std::string_view value, std::string_view key,
                        std::string_view url, int base = 10);

}  // namespace sdb::network
