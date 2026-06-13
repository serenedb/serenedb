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
#include <vector>

#include "basics/asio_ns.h"
#include "network/acceptor.h"
#include "network/http/router.h"
#include "network/http/session.h"
#include "network/io_context.h"
#include "network/pg/pg_wire_session.h"

namespace sdb {

class Server final {
 public:
  inline static Server* gInstance = nullptr;
  static Server& instance() noexcept { return *gInstance; }

  Server();
  ~Server();

  void start();
  void stop();

  // The io worker pool (sockets + reused for background timers); null until
  // start() and only when at least one endpoint is configured.
  network::IoThreadPool* IoPool() noexcept { return _pool.get(); }

 private:
  std::string _endpoint;
  std::string _pg_endpoint;
  std::string _tls_cert;
  std::string _tls_key;
  std::string _tls_ca;
  std::string _auth_password;
  std::string _auth_user;
  std::string _api_key;
  std::string _bearer_token;
  bool _auth_cleartext;
  bool _allow_cleartext_without_tls;
  bool _http_test_api;
  std::uint32_t _io_threads;
  network::HttpRouter _router;
  network::HttpServerContext _http_context{_router};
  network::pg::PgServerContext _pg_context;
  // Declared before _pool/_acceptors so it outlives the sessions that hold a
  // pointer to it via _pg_context.cancel.
  network::pg::CancelRegistry _cancel;
  std::optional<asio_ns::ssl::context> _ssl;
  // Temporary config-based auth source (RBAC will replace it via the seam).
  std::unique_ptr<network::pg::CredentialProvider> _credentials;
  std::unique_ptr<network::http::ApiKeyValidator> _api_key_validator;
  std::unique_ptr<network::http::BearerValidator> _bearer_validator;
  std::unique_ptr<network::IoThreadPool> _pool;
  std::vector<std::shared_ptr<network::AcceptorBase>> _acceptors;
};

}  // namespace sdb
