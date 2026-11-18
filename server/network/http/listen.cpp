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

#include "network/http/listen.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>

#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string_view>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "network/http/router.h"
#include "network/http/routes.h"
#include "network/http/session.h"

namespace sdb::network {
namespace {

std::shared_ptr<const ListenOptions> Parse(const ListenSpec& spec,
                                           std::span<const ListenParam> params,
                                           bool https) {
  const std::string_view url = spec.url;
  auto options = std::make_shared<HttpListenOptions>();
  options->https = https;
  for (const auto& [key, value] : params) {
    if (key == "api") {
      const auto* api = Registry<http::Api>::Find(value);
      if (!api) {
        SDB_FATAL(
          GENERAL, "unknown api '", value, "' in endpoint '", url,
          "' (known: ", absl::StrJoin(Registry<http::Api>::Names(), ", "), ")");
      }
      if (!absl::c_linear_search(options->apis, api)) {
        options->apis.push_back(api);
      }
    } else if (key == "db") {
      options->database = PercentDecode(value);
      if (options->database.empty()) {
        SDB_FATAL(GENERAL, "empty 'db' in endpoint '", url, "'");
      }
    } else if (key == "schema") {
      options->schema = PercentDecode(value);
      if (options->schema.empty()) {
        SDB_FATAL(GENERAL, "empty 'schema' in endpoint '", url, "'");
      }
    } else if (key == "create_schema") {
      options->create_schema = ParseBoolParam(value, key, url);
    } else if (key == "sslmode") {
      SDB_FATAL(GENERAL, "'sslmode' is not valid on an http endpoint '", url,
                "' (http TLS is the scheme: use https://)");
    } else {
      SDB_FATAL(GENERAL, "unknown parameter '", key, "' in endpoint '", url,
                "'");
    }
  }
  if (!https && (!spec.cert.empty() || !spec.key.empty() || !spec.ca.empty())) {
    SDB_FATAL(GENERAL,
              "TLS material is not valid on a plaintext http endpoint '", url,
              "' (use https://)");
  }
  if (https && spec.proxy != ProxyMode::Off) {
    SDB_FATAL(GENERAL, "proxy_protocol is not supported on an https listener '",
              url, "' (the PROXY header precedes the TLS handshake)");
  }
  if (options->apis.empty()) {
    SDB_FATAL(GENERAL, "http endpoint '", url,
              "' requires ?api= (e.g. ?api=es); there is no default api");
  }
  return options;
}

std::shared_ptr<const ListenOptions> ParseHttp(
  const ListenSpec& spec, std::span<const ListenParam> params) {
  return Parse(spec, params, false);
}

std::shared_ptr<const ListenOptions> ParseHttps(
  const ListenSpec& spec, std::span<const ListenParam> params) {
  return Parse(spec, params, true);
}

class HttpListener final : public AcceptorBase {
 public:
  HttpListener(const ListenContext& context, const ListenSpec& spec)
    : _ssl{HttpOptions(spec).https ? MakeTls(context, spec, {.required = true})
                                   : nullptr},
      _deps{.router = _router} {
    const auto& options = HttpOptions(spec);
    http::AddRoutes(_router, options.apis);
    _deps.ssl = _ssl.get();
    _deps.credentials = context.credentials;
    _deps.api_keys = context.api_keys;
    _deps.bearer = context.bearer;
    _deps.active = &context.active;
    _deps.max_connections =
      spec.max_connections.value_or(context.max_connections);
    _deps.max_body_bytes = http::MaxBodyBytes(options.apis);
    _deps.cancel = &context.cancel;
    _deps.sessions = &context.sessions;
    _deps.cors_origins = context.cors_origins;
    _deps.database = options.database;
    _deps.schema = options.schema;
    _deps.proxy = spec.proxy;
    if (_ssl) {
      _acceptor = std::make_shared<Acceptor<HttpSession<SocketKind::Ssl>>>(
        context.pool, spec.endpoint, _deps, MakeAcceptorOptions(spec));
    } else {
      _acceptor = std::make_shared<Acceptor<HttpSession<SocketKind::Tcp>>>(
        context.pool, spec.endpoint, _deps, MakeAcceptorOptions(spec));
    }
    LogListening(spec, static_cast<bool>(_ssl));
  }

  void Start() final { _acceptor->Start(); }

  void Stop() noexcept final { _acceptor->Stop(); }

 private:
  HttpRouter _router;
  std::unique_ptr<asio_ns::ssl::context> _ssl;
  HttpServerContext _deps;
  std::shared_ptr<AcceptorBase> _acceptor;
};

std::unique_ptr<AcceptorBase> ListenHttp(const ListenContext& context,
                                         const ListenSpec& spec) {
  const auto& options = HttpOptions(spec);
  const std::string_view database = options.database.empty()
                                      ? irs::StaticStrings::kDefaultDatabase
                                      : std::string_view{options.database};
  if (!catalog::ReadDatabase(database,
                             [](const catalog::DatabaseCatalogEntry&) {})) {
    SDB_FATAL(GENERAL, "endpoint '", spec.url, "': database '", database,
              "' does not exist");
  }
  for (const auto* api : options.apis) {
    if (api->start) {
      api->start(spec);
    }
  }
  return std::make_unique<HttpListener>(context, spec);
}

SDB_LISTEN_PROTOCOL kHttp{"http", &ParseHttp, &ListenHttp};
SDB_LISTEN_PROTOCOL kHttps{"https", &ParseHttps, &ListenHttp};

}  // namespace

const HttpListenOptions& HttpOptions(const ListenSpec& spec) {
  SDB_ASSERT(spec.protocol->listen == &ListenHttp);
  return static_cast<const HttpListenOptions&>(*spec.options);
}

}  // namespace sdb::network
