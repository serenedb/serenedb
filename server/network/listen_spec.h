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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/algo/wait_group.hpp>

#include "network/acceptor.h"
#include "network/cancel_registry.h"
#include "network/credentials.h"
#include "network/http/auth.h"
#include "network/io_context.h"
#include "network/proxy_protocol.h"
#include "network/tls_context.h"
#include "server/utils/asio_ns.h"
#include "server/utils/registry.h"

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

struct ListenContext {
  IoThreadPool& pool;
  CancelRegistry& cancel;
  std::atomic<uint32_t>& active;
  yaclib::WaitGroup<>& sessions;
  const CredentialProvider* credentials = nullptr;
  const http::ApiKeyValidator* api_keys = nullptr;
  const http::BearerValidator* bearer = nullptr;
  const TlsOptions& tls;
  uint32_t max_connections = 0;
  uint32_t max_message_bytes = 0;
  std::chrono::milliseconds auth_timeout{0};
  std::string_view cors_origins;
};

struct ListenProtocol {
  static constexpr std::string_view kKind = "listen protocol";

  std::shared_ptr<const ListenOptions> (*parse)(
    const ListenSpec& spec, std::span<const ListenParam> params);
  std::unique_ptr<AcceptorBase> (*listen)(const ListenContext& context,
                                          const ListenSpec& spec);
};

struct ListenTls {
  bool required = false;
  bool client_cert = false;
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

AcceptorOptions MakeAcceptorOptions(const ListenSpec& spec);

std::unique_ptr<asio_ns::ssl::context> MakeTls(const ListenContext& context,
                                               const ListenSpec& spec,
                                               ListenTls tls);

void LogListening(const ListenSpec& spec, bool tls);

}  // namespace sdb::network

#define SDB_LISTEN_PROTOCOL \
  const ::sdb::Registry<::sdb::network::ListenProtocol>::Register
