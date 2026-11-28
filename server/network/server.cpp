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

#include <algorithm>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <utility>

#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "network/credentials.h"
#include "network/listen_protocol.h"
#include "network/pg/hba.h"
#include "network/tls_context.h"

ABSL_FLAG(
  std::vector<std::string>, listen, {"postgres://127.0.0.1:7890"},
  "Listener URL(s), comma-separated or repeated (last-wins). Each is one "
  "listener: postgres://host:port[?sslmode=...], "
  "http(s)://host:port?api=mcp&api=otel (es needs a listener of its own), "
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

ABSL_FLAG(uint64_t, max_connections, 0,
          "Max concurrent client connections across all listeners (0 = "
          "unlimited). Over-cap connections get 53300 (pg) / 503 (http) then "
          "close. Per-listener override via ?max_connections=.");

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
    catalog::ReadEntry<catalog::RoleCatalogEntry>(
      username,
      [&](const catalog::RoleCatalogEntry& role) { stored = role.Password(); });
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
      username, backup::PositionSource::Instance().GetMockNonce());
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
    .max_connections = static_cast<std::uint32_t>(std::min<std::uint64_t>(
      absl::GetFlag(FLAGS_max_connections), 0xFFFFFFFFull)),
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
    .tls = _config.tls,
    .max_connections = _config.max_connections,
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
