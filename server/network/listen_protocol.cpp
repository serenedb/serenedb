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

#include "network/listen_protocol.h"

#include <absl/flags/flag.h>
#include <absl/time/time.h>

#include <iresearch/utils/log.hpp>

ABSL_FLAG(
  absl::Duration, auth_timeout, absl::Seconds(30),
  "Max time from connect to authenticated, per connection (0 = off). "
  "pg: covers TLS+startup+auth; http: the request-header read deadline.");

namespace sdb::network {

AcceptorOptions MakeAcceptorOptions(const ListenSpec& spec) {
  return {
    .v6_only = spec.v6_only,
    .reuseport = spec.reuseport,
    .backlog = spec.backlog,
    .keepalive = spec.keepalive,
    .keepidle = spec.keepidle,
    .keepintvl = spec.keepintvl,
    .keepcnt = spec.keepcnt,
    .unix_mode = spec.unix_mode,
    .unix_group = spec.unix_group,
  };
}

std::unique_ptr<asio_ns::ssl::context> MakeTls(const ListenContext& context,
                                               const ListenSpec& spec,
                                               ListenTls tls) {
  TlsOptions options = context.tls;
  if (!spec.cert.empty()) {
    options.cert_file = spec.cert;
  }
  if (!spec.key.empty()) {
    options.key_file = spec.key;
  }
  if (!spec.ca.empty()) {
    options.ca_file = spec.ca;
  }
  if (options.cert_file.empty() || options.key_file.empty()) {
    if (tls.required) {
      SDB_FATAL(GENERAL, "endpoint '", spec.url,
                "' requires TLS but no certificate is configured (set "
                "--tls_cert/--tls_key or ?cert=&key=)");
    }
    return nullptr;
  }
  if (tls.client_cert && options.ca_file.empty()) {
    SDB_FATAL(GENERAL, "endpoint '", spec.url,
              "' uses sslmode=verify-* but no client-cert CA is configured "
              "(set --tls_ca or ?ca=)");
  }
  options.require_client_cert = tls.client_cert;
  return std::make_unique<asio_ns::ssl::context>(
    BuildServerTlsContext(options));
}

void LogListening(const ListenSpec& spec, bool tls) {
  SDB_INFO(STARTUP, "listening on ", spec.url, " (",
           spec.endpoint.address().to_string(), ":", spec.endpoint.port(),
           tls ? ", tls" : "", ")");
}

std::chrono::milliseconds AuthTimeout() {
  return absl::ToChronoMilliseconds(absl::GetFlag(FLAGS_auth_timeout));
}

}  // namespace sdb::network
