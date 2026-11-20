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
#include <absl/flags/flag.h>
#include <absl/strings/str_join.h>
#include <absl/time/time.h>

#include <chrono>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "network/connection.h"
#include "network/http/auth.h"
#include "network/http/router.h"
#include "network/http/routes.h"
#include "network/http/session.h"
#include "network/listen_protocol.h"

ABSL_FLAG(std::string, auth_api_key, "",
          "Static HTTP ApiKey credential as id:key (empty = ApiKey rejected).");

ABSL_FLAG(std::string, auth_bearer_token, "",
          "Static HTTP Bearer token (empty = Bearer rejected).");

ABSL_FLAG(std::string, http_cors_origins, "",
          "CORS allow-origin list for the HTTP/ES API: comma-separated origins "
          "or '*' (empty disables CORS). Echoes an allowed Origin and answers "
          "preflight OPTIONS.");

ABSL_FLAG(
  absl::Duration, idle_session_timeout, absl::Seconds(75),
  "Close a session idle (between requests) longer than this. Drives the "
  "HTTP keep-alive idle timeout.");

ABSL_FLAG(absl::Duration, http_body_timeout, absl::Seconds(30),
          "Max time to read one HTTP request body.");

namespace sdb::network {
namespace {

std::unique_ptr<http::ApiKeyValidator> MakeApiKeyValidator() {
  const auto api_key = absl::GetFlag(FLAGS_auth_api_key);
  if (api_key.empty()) {
    return nullptr;
  }
  const auto colon = api_key.find(':');
  if (colon == std::string::npos) {
    SDB_FATAL(GENERAL, "--auth_api_key must be 'id:key'");
  }
  return std::make_unique<http::FlagApiKeyValidator>(api_key.substr(0, colon),
                                                     api_key.substr(colon + 1));
}

std::unique_ptr<http::BearerValidator> MakeBearerValidator() {
  auto token = absl::GetFlag(FLAGS_auth_bearer_token);
  if (token.empty()) {
    return nullptr;
  }
  return std::make_unique<http::FlagBearerValidator>(std::move(token));
}

void SetTimeouts() {
  static constexpr std::chrono::milliseconds kOff{0};
  if (const auto timeout = AuthTimeout(); timeout > kOff) {
    gHttpHeaderTimeout = timeout;
  }
  if (const auto timeout =
        absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_idle_session_timeout));
      timeout > kOff) {
    gHttpKeepAliveTimeout = timeout;
  }
  if (const auto timeout =
        absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_http_body_timeout));
      timeout > kOff) {
    gHttpBodyTimeout = timeout;
  }
}

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
      _api_keys{MakeApiKeyValidator()},
      _bearer{MakeBearerValidator()},
      _cors_origins{absl::GetFlag(FLAGS_http_cors_origins)},
      _deps{.router = _router} {
    const auto& options = HttpOptions(spec);
    http::AddRoutes(_router, options.apis);
    _deps.ssl = _ssl.get();
    _deps.credentials = context.credentials;
    _deps.api_keys = _api_keys.get();
    _deps.bearer = _bearer.get();
    _deps.active = &context.active;
    _deps.max_connections =
      spec.max_connections.value_or(context.max_connections);
    _deps.max_body_bytes = http::MaxBodyBytes(options.apis);
    _deps.cancel = &context.cancel;
    _deps.sessions = &context.sessions;
    _deps.cors_origins = _cors_origins;
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
  std::unique_ptr<http::ApiKeyValidator> _api_keys;
  std::unique_ptr<http::BearerValidator> _bearer;
  std::string _cors_origins;
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
  SetTimeouts();
  return std::make_unique<HttpListener>(context, spec);
}

SDB_LISTEN_PROTOCOL kHttp{"http", &ParseHttp, &ListenHttp};
SDB_LISTEN_PROTOCOL kHttps{"https", &ParseHttps, &ListenHttp};

}  // namespace

const HttpListenOptions& HttpOptions(const ListenSpec& spec) {
  return irs::utils::downCast<HttpListenOptions>(*spec.options);
}

}  // namespace sdb::network
