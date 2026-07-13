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
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "basics/asio_ns.h"
#include "network/acceptor.h"
#include "network/http/router.h"
#include "network/http/session.h"
#include "network/io_context.h"
#include "network/listen_spec.h"
#include "network/pg/pg_wire_session.h"
#include "network/tls_context.h"

namespace sdb {

class Server final {
 public:
  inline static Server* gInstance = nullptr;
  static Server& instance() noexcept { return *gInstance; }

  Server();
  ~Server();

  // Two-phase start: bring the io worker pool up first (StartIoPool) so the
  // background scheduler's Delay() has a timer host, then -- after the rest of
  // the engine (search indexes) is ready -- begin accepting connections
  // (StartListeners). Splitting them keeps the search maintenance loops from
  // busy-spinning on an instant Delay() during startup.
  void StartIoPool();
  void StartListeners();
  // Two-phase stop, mirroring start. StopAccepting() closes the listeners so no
  // new connection is taken; it runs EARLY, before the rest of the engine comes
  // down. stop() joins the DuckDB workers and then tears the io pool down; it
  // must run LAST -- after every subsystem that submits DuckDB work (search,
  // background, store, catalog) is down -- because a session query pipeline
  // runs on a DuckDB worker and pushes results up into the io pool, so the pool
  // can only be freed once no worker is left to touch it.
  void StopAccepting() noexcept;
  void stop();

  // The io worker pool (sockets + reused for background timers); null until
  // StartIoPool().
  network::IoThreadPool* IoPool() noexcept { return _pool.get(); }

 private:
  void SetupAuth();
  void AddListener(const network::ListenSpec& spec);
  void AddUnixListener(const network::ListenSpec& spec);
  // Per-listener TLS context (null when the listener is plaintext); built from
  // the listener's own cert/key/ca or the global --tls_* defaults.
  asio_ns::ssl::context* BuildTls(const network::ListenSpec& spec);
  // Per-listener HTTP router assembled from the listener's ?api= module set.
  network::HttpRouter& BuildRouter(const network::ListenSpec& spec);

  std::vector<std::string> _listen;
  std::string _tls_cert;
  std::string _tls_key;
  std::string _tls_ca;
  std::string _tls_ciphers;
  std::string _tls_groups;
  network::TlsMinVersion _tls_min_version = network::TlsMinVersion::Tls12;
  std::string _api_key;
  std::string _bearer_token;
  std::string _cors_origins;
  std::uint32_t _io_threads = 0;
  std::uint32_t _max_message = 0;
  std::uint32_t _max_connections = 0;
  std::chrono::milliseconds _auth_timeout{0};

  // Declared before the per-listener deps + pool/acceptors so they outlive the
  // sessions that point at them via the per-listener contexts.
  network::pg::CancelRegistry _cancel;
  std::atomic<std::uint32_t> _active{0};
  // Shared (cross-listener) auth sources; a future RBAC layer replaces them.
  std::unique_ptr<network::CredentialProvider> _credentials;
  std::unique_ptr<network::http::ApiKeyValidator> _api_key_validator;
  std::unique_ptr<network::http::BearerValidator> _bearer_validator;

  // Stable per-listener storage: acceptors hold references/pointers into these,
  // so deque (never relocates existing elements) not vector.
  std::deque<network::HttpRouter> _routers;
  std::deque<asio_ns::ssl::context> _ssl_ctxs;
  std::deque<network::pg::PgServerContext> _pg_ctxs;
  std::deque<network::HttpServerContext> _http_ctxs;

  std::unique_ptr<network::IoThreadPool> _pool;
  std::vector<std::shared_ptr<network::AcceptorBase>> _acceptors;
};

}  // namespace sdb
