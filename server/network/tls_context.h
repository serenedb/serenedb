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
#include <string>

#include "basics/asio_ns.h"

namespace sdb::network {

enum class TlsMinVersion : uint8_t { Tls12, Tls13 };

struct TlsOptions {
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
  // Require + verify a client certificate (sslmode=verify-ca / verify-full).
  // When false a presented client cert is still verified, but absence is OK.
  bool require_client_cert = false;
  TlsMinVersion min_version = TlsMinVersion::Tls12;
  // Empty => the built-in modern defaults.
  std::string ciphers;
  std::string groups;
};

// Builds a server-side TLS context for the new network stack. Modern only
// (TLS 1.2+, ECDHE, no renegotiation) and ALPN-capable (advertises http/1.1
// today, ready for h2 / pg direct-SSL). Owns its config; deliberately not
// SslServerFeature. Throws on a missing/invalid cert or key.
asio_ns::ssl::context BuildServerTlsContext(const TlsOptions& options);

}  // namespace sdb::network
