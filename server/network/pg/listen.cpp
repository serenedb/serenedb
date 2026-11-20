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

#include "network/pg/listen.h"

#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <string>

#include "network/connection.h"
#include "network/listen_protocol.h"
#include "network/pg/pg_wire_session.h"

ABSL_FLAG(uint64_t, pg_max_message_bytes, sdb::network::kDefaultMaxMessageBytes,
          "Maximum size of a single pg-wire message (statement text / bound "
          "parameter value). Bulk data should use COPY, which streams.");

namespace sdb::network {
namespace {

SslMode ParseSslMode(std::string_view value, std::string_view url) {
  if (value == "disable") {
    return SslMode::Disable;
  }
  if (value == "allow") {
    return SslMode::Allow;
  }
  if (value == "prefer") {
    return SslMode::Prefer;
  }
  if (value == "require") {
    return SslMode::Require;
  }
  if (value == "verify-ca") {
    return SslMode::VerifyCa;
  }
  if (value == "verify-full") {
    return SslMode::VerifyFull;
  }
  SDB_FATAL(GENERAL, "invalid sslmode in endpoint '", url, "': '", value, "'");
}

std::shared_ptr<const ListenOptions> ParsePg(
  const ListenSpec& spec, std::span<const ListenParam> params) {
  auto options = std::make_shared<PgListenOptions>();
  const bool is_unix = spec.transport == ListenTransport::Unix;
  for (const auto& [key, value] : params) {
    if (key == "sslmode") {
      if (is_unix) {
        SDB_FATAL(GENERAL, "'sslmode' is not valid on a unix endpoint '",
                  spec.url, "' (unix sockets never use TLS)");
      }
      options->sslmode = ParseSslMode(value, spec.url);
    } else if (key == "port") {
      if (!is_unix) {
        SDB_FATAL(GENERAL,
                  "'port' query is only for a unix pg socket (tcp uses "
                  "host:port) in endpoint '",
                  spec.url, "'");
      }
      options->unix_port =
        static_cast<uint16_t>(ParseUintParam(value, key, spec.url));
    } else {
      SDB_FATAL(GENERAL, "unknown parameter '", key, "' in endpoint '",
                spec.url, "'");
    }
  }
  return options;
}

class PgListener final : public AcceptorBase {
 public:
  PgListener(const ListenContext& context, const ListenSpec& spec) {
    _deps.credentials = context.credentials;
    _deps.cancel = &context.cancel;
    _deps.max_message_bytes = static_cast<uint32_t>(std::min<uint64_t>(
      absl::GetFlag(FLAGS_pg_max_message_bytes), 0xFFFFFFFFull));
    _deps.active = &context.active;
    _deps.sessions = &context.sessions;
    _deps.max_connections =
      spec.max_connections.value_or(context.max_connections);
    _deps.auth_timeout = AuthTimeout();
    _deps.proxy = spec.proxy;
    if (spec.transport == ListenTransport::Unix) {
      ListenOnUnix(context, spec);
    } else {
      ListenOnTcp(context, spec);
    }
  }

  void Start() final { _acceptor->Start(); }

  void Stop() noexcept final { _acceptor->Stop(); }

 private:
  void ListenOnUnix(const ListenContext& context, const ListenSpec& spec) {
    const auto& options = PgOptions(spec);
    std::string path;
    if (spec.unix_abstract) {
      path = std::string(1, '\0') + spec.unix_path;
    } else if (options.unix_port) {
      path = absl::StrCat(spec.unix_path, "/.s.PGSQL.", *options.unix_port);
    } else {
      path = spec.unix_path;
    }
    _deps.allow_cleartext_without_tls = true;
    _acceptor = std::make_shared<Acceptor<pg::PgWireSession<SocketKind::Unix>>>(
      context.pool, asio_ns::local::stream_protocol::endpoint{path}, _deps,
      MakeAcceptorOptions(spec));
    SDB_INFO(STARTUP, "listening on ", spec.url,
             " (unix:", spec.unix_abstract ? "@" + spec.unix_path : path, ")");
  }

  void ListenOnTcp(const ListenContext& context, const ListenSpec& spec) {
    const auto sslmode = PgOptions(spec).sslmode;
    if (sslmode != SslMode::Disable) {
      _ssl = MakeTls(context, spec,
                     {
                       .required = RequireTls(sslmode),
                       .client_cert = RequireClientCert(sslmode),
                     });
    }
    _deps.ssl = _ssl.get();
    _deps.require_tls = RequireTls(sslmode) && _ssl;
    if (_ssl) {
      _acceptor =
        std::make_shared<Acceptor<pg::PgWireSession<SocketKind::MaybeTls>>>(
          context.pool, spec.endpoint, _deps, MakeAcceptorOptions(spec));
    } else {
      _acceptor =
        std::make_shared<Acceptor<pg::PgWireSession<SocketKind::Tcp>>>(
          context.pool, spec.endpoint, _deps, MakeAcceptorOptions(spec));
    }
    LogListening(spec, static_cast<bool>(_ssl));
  }

  std::unique_ptr<asio_ns::ssl::context> _ssl;
  pg::PgServerContext _deps;
  std::shared_ptr<AcceptorBase> _acceptor;
};

std::unique_ptr<AcceptorBase> ListenPg(const ListenContext& context,
                                       const ListenSpec& spec) {
  return std::make_unique<PgListener>(context, spec);
}

SDB_LISTEN_PROTOCOL kPostgres{"postgres", &ParsePg, &ListenPg};
SDB_LISTEN_PROTOCOL kPostgresql{"postgresql", &ParsePg, &ListenPg};

}  // namespace

const PgListenOptions& PgOptions(const ListenSpec& spec) {
  return irs::utils::downCast<PgListenOptions>(*spec.options);
}

bool RequireTls(SslMode mode) {
  return mode == SslMode::Require || mode == SslMode::VerifyCa ||
         mode == SslMode::VerifyFull;
}

bool RequireClientCert(SslMode mode) {
  return mode == SslMode::VerifyCa || mode == SslMode::VerifyFull;
}

}  // namespace sdb::network
