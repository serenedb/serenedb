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
#include <string>
#include <vector>
#include <yaclib/algo/wait_group.hpp>

#include "network/acceptor.h"
#include "network/cancel_registry.h"
#include "network/credentials.h"
#include "network/http/auth.h"
#include "network/io_context.h"
#include "network/listen_spec.h"
#include "network/tls_context.h"

namespace sdb {

struct ServerConfig {
  std::vector<network::ListenSpec> listeners;
  network::TlsOptions tls;
  std::string api_key;
  std::string cors_origins;
  std::uint32_t max_message = 0;
  std::uint32_t max_connections = 0;
  std::chrono::milliseconds auth_timeout{0};
  std::chrono::milliseconds idle_timeout{0};
  std::chrono::milliseconds body_timeout{0};
};

ServerConfig ReadServerConfig();

class Server final {
 public:
  Server(network::IoThreadPool& pool, ServerConfig config);
  ~Server();

 private:
  void StartListeners();
  void SetupAuth();

  network::IoThreadPool& _pool;
  const ServerConfig _config;

  network::CancelRegistry _cancel;
  std::atomic<std::uint32_t> _active{0};
  // Every accepted session's Run() future; the slot is taken by the acceptor
  // at spawn and the initial hold is released by ~Server().
  yaclib::WaitGroup<> _sessions{1};
  // Shared (cross-listener) auth sources; a future RBAC layer replaces them.
  std::unique_ptr<network::CredentialProvider> _credentials;
  std::unique_ptr<network::http::ApiKeyValidator> _api_key_validator;
  std::vector<std::unique_ptr<network::http::BearerValidator>>
    _bearer_validators;

  std::vector<std::unique_ptr<network::AcceptorBase>> _listeners;
};

}  // namespace sdb
