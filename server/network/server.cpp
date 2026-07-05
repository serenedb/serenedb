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
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

#include "basics/log.h"
#include "basics/number_of_cores.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "network/connection.h"
#include "network/credentials.h"
#include "network/http/es/handlers.h"
#include "network/http/test/handlers.h"
#include "network/pg/hba.h"
#include "network/socket.h"
#include "network/tls_context.h"

ABSL_FLAG(
  std::vector<std::string>, listen, {"postgres://127.0.0.1:7890"},
  "Listener URL(s), comma-separated or repeated (last-wins). Each is one "
  "listener: postgres://host:port[?sslmode=...], http(s)://host:port?api=es, "
  "postgres:///path/to.sock (unix).");

ABSL_FLAG(std::string, tls_cert, "",
          "Default PEM server certificate chain for TLS listeners (a listener "
          "may override with ?cert=).");

ABSL_FLAG(std::string, tls_key, "", "Default PEM private key for --tls_cert.");

ABSL_FLAG(std::string, tls_ca, "",
          "Default PEM CA bundle to verify client certificates "
          "(sslmode=verify-ca/verify-full).");

ABSL_FLAG(std::string, tls_min_version, "1.2",
          "Minimum TLS version: 1.2 or 1.3 (legacy TLS is not supported).");

ABSL_FLAG(std::string, tls_ciphers,
          "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
          "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
          "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305",
          "OpenSSL TLS 1.2 cipher list (forward-secret AEAD, Mozilla "
          "intermediate); TLS 1.3 suites are fixed by OpenSSL.");

ABSL_FLAG(std::string, tls_groups, "X25519:P-256",
          "TLS key-exchange groups, colon-separated.");

ABSL_FLAG(std::string, auth_user, "postgres",
          "User the temporary --auth_password / token credentials apply to.");

ABSL_FLAG(
  std::string, auth_password, "",
  "Temporary single-user password (empty = trust). Placeholder until "
  "RBAC; SCRAM-SHA-256 by default. Cleartext password auth requires TLS.");

ABSL_FLAG(std::string, auth_api_key, "",
          "Static HTTP ApiKey credential as id:key (empty = ApiKey rejected).");

ABSL_FLAG(std::string, auth_bearer_token, "",
          "Static HTTP Bearer token (empty = Bearer rejected).");

ABSL_FLAG(std::string, auth_method, "scram",
          "Password auth method for --auth_password: scram (SCRAM-SHA-256, "
          "default), md5, or password (cleartext; requires TLS).");

ABSL_FLAG(uint64_t, pg_max_message_bytes, sdb::network::kDefaultMaxMessageBytes,
          "Maximum size of a single pg-wire message (statement text / bound "
          "parameter value). Bulk data should use COPY, which streams.");

ABSL_FLAG(uint64_t, io_threads, 0,
          "IO threads for HTTP and pg-wire connections (0 = max(1, "
          "cpu_count / 4)).");

ABSL_FLAG(uint64_t, max_connections, 0,
          "Max concurrent client connections across all listeners (0 = "
          "unlimited). Over-cap connections get 53300 (pg) / 503 (http) then "
          "close. Per-listener override via ?max_connections=.");

ABSL_FLAG(std::string, http_cors_origins, "",
          "CORS allow-origin list for the HTTP/ES API: comma-separated origins "
          "or '*' (empty disables CORS). Echoes an allowed Origin and answers "
          "preflight OPTIONS.");

ABSL_FLAG(
  absl::Duration, auth_timeout, absl::Seconds(30),
  "Max time from connect to authenticated, per connection (0 = off). "
  "pg: covers TLS+startup+auth; http: the request-header read deadline.");

ABSL_FLAG(
  absl::Duration, idle_session_timeout, absl::Seconds(75),
  "Close a session idle (between requests) longer than this. Drives the "
  "HTTP keep-alive idle timeout.");

ABSL_FLAG(absl::Duration, http_body_timeout, absl::Seconds(30),
          "Max time to read one HTTP request body.");

namespace sdb {
namespace {

const char* AuthMethodName(network::pg::AuthMethod method) {
  switch (method) {
    case network::pg::AuthMethod::Scram:
      return "scram-sha-256";
    case network::pg::AuthMethod::Md5:
      return "md5";
    case network::pg::AuthMethod::Cleartext:
      return "password";
  }
  return "password";
}

// Throwaway credential source: one user from the config flags. RBAC will
// provide a real CredentialProvider via the same seam.
class ConfigCredentialProvider final : public network::CredentialProvider {
 public:
  ConfigCredentialProvider(std::string user, network::Credential credential)
    : _user{std::move(user)}, _credential{std::move(credential)} {}

  std::optional<network::Credential> LookupCredential(
    std::string_view username) const override {
    if (username == _user) {
      return _credential;
    }
    return std::nullopt;
  }

 private:
  std::string _user;
  network::Credential _credential;
};

// Per-role credential source: looks the connecting user up in the catalog and,
// if the role carries a stored SCRAM verifier (rolpassword), returns it so the
// connection must authenticate. A role with no stored password returns nullopt
// -> the pg-wire/http layer treats that as trust (logs in without a password),
// matching PostgreSQL's "no password set" behaviour.
class CatalogCredentialProvider final : public network::CredentialProvider {
 public:
  std::optional<network::Credential> LookupCredential(
    std::string_view username) const override {
    auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
    auto role = snapshot->GetRole(username);
    if (!role) {
      return std::nullopt;
    }
    const auto stored = role->PasswordVerifier();
    if (stored.empty()) {
      return std::nullopt;
    }
    network::Credential credential;
    if (auto verifier = network::ParseScramVerifier(stored)) {
      credential.scram = std::move(*verifier);
    } else if (network::IsMd5Verifier(stored)) {
      credential.md5 = std::string{stored};
    } else {
      return std::nullopt;
    }
    return credential;
  }
};

}  // namespace

Server::Server()
  : _listen{absl::GetFlag(FLAGS_listen)},
    _tls_cert{absl::GetFlag(FLAGS_tls_cert)},
    _tls_key{absl::GetFlag(FLAGS_tls_key)},
    _tls_ca{absl::GetFlag(FLAGS_tls_ca)},
    _tls_ciphers{absl::GetFlag(FLAGS_tls_ciphers)},
    _tls_groups{absl::GetFlag(FLAGS_tls_groups)},
    _auth_user{absl::GetFlag(FLAGS_auth_user)},
    _auth_password{absl::GetFlag(FLAGS_auth_password)},
    _api_key{absl::GetFlag(FLAGS_auth_api_key)},
    _bearer_token{absl::GetFlag(FLAGS_auth_bearer_token)},
    _cors_origins{absl::GetFlag(FLAGS_http_cors_origins)},
    _io_threads{static_cast<std::uint32_t>(absl::GetFlag(FLAGS_io_threads))} {
  if (_io_threads == 0) {
    // One independent single-threaded asio reactor per PHYSICAL core: the
    // reactors are round-robin pinned with no shared queue (no contention, so
    // no ceiling) and idle ones just park in epoll_wait, but hyper-thread
    // siblings add nothing to epoll-bound reactors -- so physical, not logical,
    // cores.
    _io_threads = static_cast<std::uint32_t>(CountPhysicalCores());
  }
  absl::SetFlag(&FLAGS_io_threads, _io_threads);
  const auto min_version = absl::GetFlag(FLAGS_tls_min_version);
  if (min_version == "1.2") {
    _tls_min_version = network::TlsMinVersion::Tls12;
  } else if (min_version == "1.3") {
    _tls_min_version = network::TlsMinVersion::Tls13;
  } else {
    SDB_FATAL(GENERAL,
              "--tls_min_version must be 1.2 or 1.3 (legacy TLS is "
              "not supported), got '",
              min_version, "'");
  }
  const auto method = absl::GetFlag(FLAGS_auth_method);
  if (method == "scram") {
    _auth_method = network::pg::AuthMethod::Scram;
  } else if (method == "md5") {
    _auth_method = network::pg::AuthMethod::Md5;
  } else if (method == "password" || method == "cleartext") {
    _auth_method = network::pg::AuthMethod::Cleartext;
  } else {
    SDB_FATAL(GENERAL, "--auth_method must be scram, md5, or password, got '",
              method, "'");
  }
  _max_message = static_cast<std::uint32_t>(std::min<std::uint64_t>(
    absl::GetFlag(FLAGS_pg_max_message_bytes), 0xFFFFFFFFull));
  _max_connections = static_cast<std::uint32_t>(std::min<std::uint64_t>(
    absl::GetFlag(FLAGS_max_connections), 0xFFFFFFFFull));
  gInstance = this;
}

Server::~Server() { gInstance = nullptr; }

void Server::SetupAuth() {
  if (!_auth_password.empty()) {
    network::Credential credential;
    credential.cleartext = _auth_password;
    if (_auth_method == network::pg::AuthMethod::Scram) {
      credential.scram = network::BuildScramVerifier(_auth_password);
      if (!credential.scram) {
        SDB_FATAL(GENERAL, "could not build SCRAM verifier for auth");
      }
    }
    _credentials = std::make_unique<ConfigCredentialProvider>(
      _auth_user, std::move(credential));
    SDB_INFO(GENERAL, "network auth enabled for user '", _auth_user, "' (",
             AuthMethodName(_auth_method), ")");
  } else {
    _credentials = std::make_unique<CatalogCredentialProvider>();
    SDB_INFO(GENERAL, "network auth using per-role catalog passwords (",
             AuthMethodName(_auth_method), ")");
  }
  if (!_api_key.empty()) {
    const auto colon = _api_key.find(':');
    if (colon == std::string::npos) {
      SDB_FATAL(GENERAL, "--auth_api_key must be 'id:key'");
    }
    _api_key_validator = std::make_unique<network::http::FlagApiKeyValidator>(
      _api_key.substr(0, colon), _api_key.substr(colon + 1), _auth_user);
    SDB_INFO(GENERAL, "network http ApiKey auth enabled");
  }
  if (!_bearer_token.empty()) {
    _bearer_validator = std::make_unique<network::http::FlagBearerValidator>(
      _bearer_token, _auth_user);
    SDB_INFO(GENERAL, "network http Bearer auth enabled");
  }

  network::pg::hba::LoadPersistedHba();
}

asio_ns::ssl::context* Server::BuildTls(const network::ListenSpec& spec) {
  if (!spec.TlsCapable()) {
    return nullptr;
  }
  std::string cert = spec.cert.empty() ? _tls_cert : spec.cert;
  std::string key = spec.key.empty() ? _tls_key : spec.key;
  std::string ca = spec.ca.empty() ? _tls_ca : spec.ca;
  if (cert.empty() || key.empty()) {
    if (spec.protocol == network::ListenProtocol::Http || spec.RequireTls()) {
      SDB_FATAL(GENERAL, "endpoint '", spec.url,
                "' requires TLS but no certificate is configured (set "
                "--tls_cert/--tls_key or ?cert=&key=)");
    }
    return nullptr;
  }
  if (spec.RequireClientCert() && ca.empty()) {
    SDB_FATAL(GENERAL, "endpoint '", spec.url,
              "' uses sslmode=verify-* but no client-cert CA is configured "
              "(set --tls_ca or ?ca=)");
  }
  network::TlsOptions options;
  options.cert_file = std::move(cert);
  options.key_file = std::move(key);
  options.ca_file = std::move(ca);
  options.require_client_cert = spec.RequireClientCert();
  options.min_version = _tls_min_version;
  options.ciphers = _tls_ciphers;
  options.groups = _tls_groups;
  _ssl_ctxs.emplace_back(network::BuildServerTlsContext(options));
  return &_ssl_ctxs.back();
}

network::HttpRouter& Server::BuildRouter(const network::ListenSpec& spec) {
  network::HttpRouter& router = _routers.emplace_back();
  for (const auto& api : spec.apis) {
    if (api == "es") {
      network::http::es::Register(router);
    } else if (api == "test") {
      network::http::test::Register(router);
    }
  }
  return router;
}

void Server::AddUnixListener(const network::ListenSpec& spec) {
  network::AcceptorOptions opts;
  opts.backlog = spec.backlog;
  opts.unix_mode = spec.unix_mode;
  opts.unix_group = spec.unix_group;
  std::string path;
  if (spec.unix_abstract) {
    path = std::string(1, '\0') + spec.unix_path;
  } else if (spec.unix_port) {
    path = spec.unix_path + "/.s.PGSQL." + absl::StrCat(*spec.unix_port);
  } else {
    path = spec.unix_path;
  }
  const asio_ns::local::stream_protocol::endpoint ep{path};

  std::shared_ptr<network::AcceptorBase> acceptor;
  if (spec.protocol == network::ListenProtocol::Pg) {
    network::pg::PgServerContext& deps = _pg_ctxs.emplace_back();
    deps.credentials = _credentials.get();
    deps.allow_cleartext_without_tls = true;
    deps.cancel = &_cancel;
    deps.max_message_bytes = _max_message;
    deps.auth_method = _auth_method;
    deps.active = &_active;
    deps.max_connections = spec.max_connections.value_or(_max_connections);
    deps.auth_timeout = _auth_timeout;
    deps.proxy = spec.proxy;
    acceptor = std::make_shared<
      network::Acceptor<network::pg::PgWireSession<network::SocketKind::Unix>>>(
      *_pool, ep, deps, opts);
  } else {
    network::HttpRouter& router = BuildRouter(spec);
    network::HttpServerContext& deps =
      _http_ctxs.emplace_back(network::HttpServerContext{router});
    deps.credentials = _credentials.get();
    deps.api_keys = _api_key_validator.get();
    deps.bearer = _bearer_validator.get();
    deps.active = &_active;
    deps.max_connections = spec.max_connections.value_or(_max_connections);
    deps.cors_origins = _cors_origins;
    deps.proxy = spec.proxy;
    acceptor = std::make_shared<
      network::Acceptor<network::HttpSession<network::SocketKind::Unix>>>(
      *_pool, ep, deps, opts);
  }
  acceptor->Start();
  _acceptors.push_back(std::move(acceptor));
  SDB_INFO(GENERAL, "network listening on ", spec.url,
           " (unix:", spec.unix_abstract ? "@" + spec.unix_path : path, ")");
}

void Server::AddListener(const network::ListenSpec& spec) {
  if (spec.transport == network::ListenTransport::Unix) {
    AddUnixListener(spec);
    return;
  }
  if (spec.https && spec.proxy != network::ProxyMode::Off) {
    SDB_FATAL(GENERAL, "proxy_protocol is not supported on an https listener '",
              spec.url, "' (the PROXY header precedes the TLS handshake)");
  }
  asio_ns::ssl::context* ssl = BuildTls(spec);
  network::AcceptorOptions opts;
  opts.v6_only = spec.v6_only;
  opts.reuseport = spec.reuseport;
  opts.backlog = spec.backlog;
  opts.keepalive = spec.keepalive;
  opts.keepidle = spec.keepidle;
  opts.keepintvl = spec.keepintvl;
  opts.keepcnt = spec.keepcnt;

  std::shared_ptr<network::AcceptorBase> acceptor;
  if (spec.protocol == network::ListenProtocol::Pg) {
    network::pg::PgServerContext& deps = _pg_ctxs.emplace_back();
    deps.ssl = ssl;
    deps.credentials = _credentials.get();
    deps.allow_cleartext_without_tls = false;
    deps.require_tls = spec.RequireTls() && ssl != nullptr;
    deps.cancel = &_cancel;
    deps.max_message_bytes = _max_message;
    deps.auth_method = _auth_method;
    deps.active = &_active;
    deps.max_connections = spec.max_connections.value_or(_max_connections);
    deps.auth_timeout = _auth_timeout;
    deps.proxy = spec.proxy;
    if (ssl != nullptr) {
      acceptor = std::make_shared<network::Acceptor<
        network::pg::PgWireSession<network::SocketKind::MaybeTls>>>(
        *_pool, spec.endpoint, deps, opts);
    } else {
      acceptor = std::make_shared<network::Acceptor<
        network::pg::PgWireSession<network::SocketKind::Tcp>>>(
        *_pool, spec.endpoint, deps, opts);
    }
  } else {
    network::HttpRouter& router = BuildRouter(spec);
    network::HttpServerContext& deps =
      _http_ctxs.emplace_back(network::HttpServerContext{router});
    deps.ssl = ssl;
    deps.credentials = _credentials.get();
    deps.api_keys = _api_key_validator.get();
    deps.bearer = _bearer_validator.get();
    deps.active = &_active;
    deps.max_connections = spec.max_connections.value_or(_max_connections);
    deps.cors_origins = _cors_origins;
    deps.proxy = spec.proxy;
    if (ssl != nullptr) {
      acceptor = std::make_shared<
        network::Acceptor<network::HttpSession<network::SocketKind::Ssl>>>(
        *_pool, spec.endpoint, deps, opts);
    } else {
      acceptor = std::make_shared<
        network::Acceptor<network::HttpSession<network::SocketKind::Tcp>>>(
        *_pool, spec.endpoint, deps, opts);
    }
  }
  acceptor->Start();
  _acceptors.push_back(std::move(acceptor));
  SDB_INFO(GENERAL, "network listening on ", spec.url, " (",
           spec.endpoint.address().to_string(), ":", spec.endpoint.port(),
           ssl != nullptr ? ", tls" : "", ")");
}

void Server::StartIoPool() {
  // Validate the listen specs up front so a bad --listen fails before anything
  // else comes up, even though the listeners themselves start later.
  network::ParseListenSpecs(_listen);
  _auth_timeout = absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_auth_timeout));
  if (_auth_timeout > std::chrono::milliseconds::zero()) {
    network::gHttpHeaderTimeout = _auth_timeout;
  }
  if (const auto idle =
        absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_idle_session_timeout));
      idle > std::chrono::milliseconds::zero()) {
    network::gHttpKeepAliveTimeout = idle;
  }
  if (const auto body =
        absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_http_body_timeout));
      body > std::chrono::milliseconds::zero()) {
    network::gHttpBodyTimeout = body;
  }
  _pool = std::make_unique<network::IoThreadPool>(_io_threads);
  _pool->Start();
}

void Server::StartListeners() {
  const auto specs = network::ParseListenSpecs(_listen);
  SetupAuth();
  for (const auto& spec : specs) {
    AddListener(spec);
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
