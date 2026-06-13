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

#include "network/server.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <ada.h>

#include <algorithm>
#include <charconv>

#include "basics/log.h"
#include "basics/number_of_cores.h"
#include "network/http/es/handlers.h"
#include "network/http/test/handlers.h"
#include "network/pg/auth.h"
#include "network/tls_context.h"

ABSL_FLAG(uint64_t, server_io_threads, 0,
          "IO threads for HTTP and pg-wire connections "
          "(0 = max(1, cpu_count / 4)).");

ABSL_FLAG(std::string, network_http_endpoint, "",
          "URL the network HTTP server listens on, e.g. "
          "http://0.0.0.0:9200 (empty disables it).");

ABSL_FLAG(std::string, network_pg_endpoint, "",
          "URL the network pg-wire server listens on, e.g. "
          "pgsql://0.0.0.0:5433 (empty disables it).");

ABSL_FLAG(std::string, network_tls_cert, "",
          "PEM server certificate chain. When set, TLS is enabled: the pg "
          "endpoint upgrades in-band on SSLRequest and the HTTP endpoint "
          "serves HTTPS.");

ABSL_FLAG(std::string, network_tls_key, "",
          "PEM private key for --network_tls_cert.");

ABSL_FLAG(std::string, network_tls_ca, "",
          "Optional PEM CA bundle to verify client certificates (empty "
          "disables client-cert verification).");

ABSL_FLAG(std::string, network_auth_password, "",
          "Temporary single-user password for the new pg-wire server (empty = "
          "trust). Placeholder until RBAC; SCRAM-SHA-256 by default.");

ABSL_FLAG(std::string, network_auth_user, "postgres",
          "User the --network_auth_password applies to.");

ABSL_FLAG(std::string, network_api_key, "",
          "Static HTTP ApiKey credential as id:key (empty = ApiKey scheme "
          "rejected). Authenticates as --network_auth_user. Placeholder until "
          "a key store exists.");

ABSL_FLAG(std::string, network_bearer_token, "",
          "Static HTTP Bearer token (empty = Bearer scheme rejected). "
          "Authenticates as --network_auth_user. Placeholder until a "
          "service-token store exists.");

ABSL_FLAG(bool, network_auth_cleartext, false,
          "Use cleartext password auth instead of SCRAM-SHA-256 (still "
          "requires TLS unless --network_allow_cleartext_without_tls).");

ABSL_FLAG(bool, network_allow_cleartext_without_tls, false,
          "Permit cleartext password auth on plaintext (non-TLS) connections.");

ABSL_FLAG(bool, network_http_test_api, false,
          "Register test-only HTTP endpoints under /_test/ (echo, ping, bytes, "
          "fuzz, status). For tests/benchmarks only -- never in production.");

ABSL_FLAG(uint64_t, network_pg_max_message_bytes,
          sdb::network::kDefaultMaxMessageBytes,
          "Maximum size of a single pg-wire message (statement text / bound "
          "parameter value). Bulk data should use COPY, which streams and is "
          "not bounded by this. Over-cap messages get a clean error, not a "
          "silent drop.");

namespace sdb {
namespace {

// Throwaway credential source: one user from the config flags. The colleague's
// RBAC will provide a real CredentialProvider via the same seam.
class ConfigCredentialProvider final : public network::pg::CredentialProvider {
 public:
  ConfigCredentialProvider(std::string user, network::pg::Credential credential)
    : _user{std::move(user)}, _credential{std::move(credential)} {}

  std::optional<network::pg::Credential> LookupCredential(
    std::string_view username) const override {
    if (username == _user) {
      return _credential;
    }
    return std::nullopt;
  }

 private:
  std::string _user;
  network::pg::Credential _credential;
};

asio_ns::ip::tcp::endpoint ParseEndpoint(const std::string& url) {
  const auto parsed = ada::parse(url);
  if (!parsed) {
    SDB_FATAL(GENERAL, "invalid network endpoint '", url, "'");
  }
  std::string_view host = parsed->get_hostname();
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  asio_ns::error_code ec;
  const auto address = asio_ns::ip::make_address(std::string{host}, ec);
  if (ec) {
    SDB_FATAL(GENERAL, "invalid bind host '", host, "' in endpoint '", url,
              "'");
  }
  const std::string_view port_str = parsed->get_port();
  std::uint16_t port = 0;
  const auto [ptr, perr] =
    std::from_chars(port_str.data(), port_str.data() + port_str.size(), port);
  if (perr != std::errc{} || ptr != port_str.data() + port_str.size() ||
      port == 0) {
    SDB_FATAL(GENERAL, "missing or invalid port in endpoint '", url, "'");
  }
  return {address, port};
}

}  // namespace

Server::Server()
  : _endpoint{absl::GetFlag(FLAGS_network_http_endpoint)},
    _pg_endpoint{absl::GetFlag(FLAGS_network_pg_endpoint)},
    _tls_cert{absl::GetFlag(FLAGS_network_tls_cert)},
    _tls_key{absl::GetFlag(FLAGS_network_tls_key)},
    _tls_ca{absl::GetFlag(FLAGS_network_tls_ca)},
    _auth_password{absl::GetFlag(FLAGS_network_auth_password)},
    _auth_user{absl::GetFlag(FLAGS_network_auth_user)},
    _api_key{absl::GetFlag(FLAGS_network_api_key)},
    _bearer_token{absl::GetFlag(FLAGS_network_bearer_token)},
    _auth_cleartext{absl::GetFlag(FLAGS_network_auth_cleartext)},
    _allow_cleartext_without_tls{
      absl::GetFlag(FLAGS_network_allow_cleartext_without_tls)},
    _http_test_api{absl::GetFlag(FLAGS_network_http_test_api)},
    _io_threads{
      static_cast<std::uint32_t>(absl::GetFlag(FLAGS_server_io_threads))} {
  if (_io_threads == 0) {
    _io_threads = std::max<std::uint32_t>(1, number_of_cores::GetValue() / 4);
  }
  gInstance = this;
}

Server::~Server() { gInstance = nullptr; }

void Server::start() {
  if (_endpoint.empty() && _pg_endpoint.empty()) {
    return;
  }

  const bool tls = !_tls_cert.empty();
  if (tls) {
    _ssl.emplace(network::BuildServerTlsContext(_tls_cert, _tls_key, _tls_ca));
    _http_context.ssl = &*_ssl;
    _pg_context.ssl = &*_ssl;
  }

  if (!_auth_password.empty()) {
    network::pg::Credential credential;
    credential.cleartext = _auth_password;
    if (!_auth_cleartext) {
      credential.scram = network::pg::BuildScramVerifier(_auth_password);
      if (!credential.scram) {
        SDB_FATAL(GENERAL, "could not build SCRAM verifier for auth");
      }
    }
    _credentials = std::make_unique<ConfigCredentialProvider>(
      _auth_user, std::move(credential));
    _pg_context.credentials = _credentials.get();
    _pg_context.allow_cleartext_without_tls = _allow_cleartext_without_tls;
    // One user store for both protocols: HTTP Basic validates against the
    // same provider (cleartext compare or scram-verifier derivation).
    _http_context.credentials = _credentials.get();
    SDB_INFO(GENERAL, "network pg-wire + http auth enabled for user '",
             _auth_user, "' (", _auth_cleartext ? "cleartext" : "scram-sha-256",
             ")");
  }

  // HTTP ApiKey / Bearer are independent of Basic: each scheme is enabled by
  // its own flag, validated against one static credential, authenticating as
  // _auth_user. Unset => the scheme stays rejected (null validator -> 401).
  if (!_api_key.empty()) {
    const auto colon = _api_key.find(':');
    if (colon == std::string::npos) {
      SDB_FATAL(GENERAL, "--network_api_key must be 'id:key'");
    }
    _api_key_validator = std::make_unique<network::http::FlagApiKeyValidator>(
      _api_key.substr(0, colon), _api_key.substr(colon + 1), _auth_user);
    _http_context.api_keys = _api_key_validator.get();
    SDB_INFO(GENERAL, "network http ApiKey auth enabled");
  }
  if (!_bearer_token.empty()) {
    _bearer_validator = std::make_unique<network::http::FlagBearerValidator>(
      _bearer_token, _auth_user);
    _http_context.bearer = _bearer_validator.get();
    SDB_INFO(GENERAL, "network http Bearer auth enabled");
  }

  _pg_context.cancel = &_cancel;
  _pg_context.max_message_bytes =
    static_cast<std::uint32_t>(std::min<std::uint64_t>(
      absl::GetFlag(FLAGS_network_pg_max_message_bytes), 0xFFFFFFFFull));

  _pool = std::make_unique<network::IoThreadPool>(_io_threads);
  _pool->Start();

  if (!_endpoint.empty()) {
    const auto bind = ParseEndpoint(_endpoint);
    network::http::es::Register(_router);
    if (_http_test_api) {
      network::http::test::Register(_router);
      SDB_INFO(GENERAL, "network HTTP test API enabled under /_test/");
    }
    std::shared_ptr<network::AcceptorBase> acceptor;
    if (tls) {
      acceptor = std::make_shared<
        network::Acceptor<network::HttpSession<network::SocketKind::Ssl>>>(
        *_pool, bind, _http_context);
    } else {
      acceptor = std::make_shared<
        network::Acceptor<network::HttpSession<network::SocketKind::Tcp>>>(
        *_pool, bind, _http_context);
    }
    acceptor->Start();
    _acceptors.push_back(std::move(acceptor));
    SDB_INFO(GENERAL, "network HTTP", tls ? "S" : "", " server listening on ",
             _endpoint);
  }

  if (!_pg_endpoint.empty()) {
    const auto bind = ParseEndpoint(_pg_endpoint);
    std::shared_ptr<network::AcceptorBase> acceptor;
    if (tls) {
      acceptor = std::make_shared<network::Acceptor<
        network::pg::PgWireSession<network::SocketKind::MaybeTls>>>(
        *_pool, bind, _pg_context);
    } else {
      acceptor = std::make_shared<network::Acceptor<
        network::pg::PgWireSession<network::SocketKind::Tcp>>>(*_pool, bind,
                                                               _pg_context);
    }
    acceptor->Start();
    _acceptors.push_back(std::move(acceptor));
    SDB_INFO(GENERAL, "network pg-wire server listening on ", _pg_endpoint,
             tls ? " (in-band TLS available)" : "");
  }
}

void Server::stop() {
  for (auto& acceptor : _acceptors) {
    acceptor->Stop();
  }
  if (_pool) {
    _pool->Stop();
    _pool.reset();
  }
  _acceptors.clear();
}

}  // namespace sdb
