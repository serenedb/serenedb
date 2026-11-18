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
#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <utility>

#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "network/connection.h"
#include "network/credentials.h"
#include "network/pg/hba.h"
#include "network/tls_context.h"

ABSL_FLAG(
  std::vector<std::string>, listen, {"postgres://127.0.0.1:7890"},
  "Listener URL(s), comma-separated or repeated (last-wins). Each is one "
  "listener: postgres://host:port[?sslmode=...], http(s)://host:port?api=es, "
  "postgres:///path/to.sock (unix). A postgres listener needs an explicit "
  "port, http(s) defaults to 80/443; a path after the port is ignored.");

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

ABSL_FLAG(std::string, auth_api_key, "",
          "Static HTTP ApiKey credential as id:key (empty = ApiKey rejected).");

ABSL_FLAG(std::string, auth_bearer_token, "",
          "Static HTTP Bearer token (empty = Bearer rejected).");

ABSL_FLAG(uint64_t, pg_max_message_bytes, sdb::network::kDefaultMaxMessageBytes,
          "Maximum size of a single pg-wire message (statement text / bound "
          "parameter value). Bulk data should use COPY, which streams.");

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

// Per-role credential source: looks the connecting user up in the catalog and,
// if the role carries a stored SCRAM verifier (rolpassword), returns it so the
// connection must authenticate. A role with no stored password returns nullopt
// -> the pg-wire/http layer treats that as trust (logs in without a password),
// matching PostgreSQL's "no password set" behaviour.
class CatalogCredentialProvider final : public network::CredentialProvider {
 public:
  std::optional<network::Credential> LookupCredential(
    std::string_view username) const final {
    std::string stored;
    catalog::ReadRole(username, [&](const catalog::RoleCatalogEntry& role) {
      stored = role.Password();
    });
    if (stored.empty()) {
      return std::nullopt;
    }
    network::Credential credential;
    if (auto verifier = network::ParseScramVerifier(stored)) {
      credential.scram = std::move(*verifier);
    } else if (network::IsMd5Verifier(stored)) {
      credential.md5.emplace(stored);
    } else {
      return std::nullopt;
    }
    return credential;
  }

  network::ScramVerifier MockScram(std::string_view username) const final {
    return network::MockScramVerifier(
      username, backup::PositionSource::Instance().MockNonce());
  }
};

}  // namespace

ServerConfig ReadServerConfig() {
  ServerConfig config{
    .listeners = network::ParseListenSpecs(absl::GetFlag(FLAGS_listen)),
    .tls =
      {
        .cert_file = absl::GetFlag(FLAGS_tls_cert),
        .key_file = absl::GetFlag(FLAGS_tls_key),
        .ca_file = absl::GetFlag(FLAGS_tls_ca),
        .ciphers = absl::GetFlag(FLAGS_tls_ciphers),
        .groups = absl::GetFlag(FLAGS_tls_groups),
      },
    .api_key = absl::GetFlag(FLAGS_auth_api_key),
    .bearer_token = absl::GetFlag(FLAGS_auth_bearer_token),
    .cors_origins = absl::GetFlag(FLAGS_http_cors_origins),
    .max_message = static_cast<std::uint32_t>(std::min<std::uint64_t>(
      absl::GetFlag(FLAGS_pg_max_message_bytes), 0xFFFFFFFFull)),
    .max_connections = static_cast<std::uint32_t>(std::min<std::uint64_t>(
      absl::GetFlag(FLAGS_max_connections), 0xFFFFFFFFull)),
    .auth_timeout =
      absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_auth_timeout)),
    .idle_timeout =
      absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_idle_session_timeout)),
    .body_timeout =
      absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_http_body_timeout)),
  };
  const auto min_version = absl::GetFlag(FLAGS_tls_min_version);
  if (min_version == "1.2") {
    config.tls.min_version = network::TlsMinVersion::Tls12;
  } else if (min_version == "1.3") {
    config.tls.min_version = network::TlsMinVersion::Tls13;
  } else {
    SDB_FATAL(GENERAL,
              "--tls_min_version must be 1.2 or 1.3 (legacy TLS is "
              "not supported), got '",
              min_version, "'");
  }
  return config;
}

Server::Server(network::IoThreadPool& pool, ServerConfig config)
  : _pool{pool}, _config{std::move(config)} {
  if (_config.auth_timeout > std::chrono::milliseconds::zero()) {
    network::gHttpHeaderTimeout = _config.auth_timeout;
  }
  if (_config.idle_timeout > std::chrono::milliseconds::zero()) {
    network::gHttpKeepAliveTimeout = _config.idle_timeout;
  }
  if (_config.body_timeout > std::chrono::milliseconds::zero()) {
    network::gHttpBodyTimeout = _config.body_timeout;
  }
  StartListeners();
}

Server::~Server() {
  for (auto& listener : _listeners) {
    listener->Stop();
  }
  _cancel.TerminateAll();
  _sessions.Done();
  _sessions.Wait();
  backup::BackupSession::Instance().End();
}

void Server::SetupAuth() {
  // Auth resolves every connection through the catalog's per-role credentials
  // (RBAC). A role with no stored password is trusted only over loopback (see
  // the HBA loopback-gated fallback in pg_wire_session / http auth).
  _credentials = std::make_unique<CatalogCredentialProvider>();
  SDB_DEBUG(GENERAL, "network auth using per-role catalog passwords");

  // The static HTTP ApiKey / Bearer credentials have no catalog store yet, so
  // they authenticate as the bootstrap superuser.
  const std::string token_user{irs::StaticStrings::kDefaultUser};
  if (const auto& api_key = _config.api_key; !api_key.empty()) {
    const auto colon = api_key.find(':');
    if (colon == std::string::npos) {
      SDB_FATAL(GENERAL, "--auth_api_key must be 'id:key'");
    }
    _api_key_validator = std::make_unique<network::http::FlagApiKeyValidator>(
      api_key.substr(0, colon), api_key.substr(colon + 1), token_user);
    SDB_DEBUG(GENERAL, "network http ApiKey auth enabled");
  }
  if (!_config.bearer_token.empty()) {
    _bearer_validator = std::make_unique<network::http::FlagBearerValidator>(
      _config.bearer_token, token_user);
    SDB_DEBUG(GENERAL, "network http Bearer auth enabled");
  }

  if (auto text = network::pg::hba::LoadPersistedHba(); !text.empty()) {
    duckdb::DBConfig::GetConfig(irs::DuckDBEngine::Instance().instance())
      .SetOptionByName("hba", duckdb::Value{std::move(text)});
  }
}

void Server::StartListeners() {
  SetupAuth();
  const network::ListenContext context{
    .pool = _pool,
    .cancel = _cancel,
    .active = _active,
    .sessions = _sessions,
    .credentials = _credentials.get(),
    .api_keys = _api_key_validator.get(),
    .bearer = _bearer_validator.get(),
    .tls = _config.tls,
    .max_connections = _config.max_connections,
    .max_message_bytes = _config.max_message,
    .auth_timeout = _config.auth_timeout,
    .cors_origins = _config.cors_origins,
  };
  for (const auto& spec : _config.listeners) {
    _listeners.push_back(spec.protocol->listen(context, spec));
  }
  SDB_IF_FAILURE("listeners_start_throws") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  for (const auto& listener : _listeners) {
    listener->Start();
  }
}

}  // namespace sdb
