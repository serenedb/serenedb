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

#include <string>

#include "basics/asio_ns.h"

namespace sdb::network {

// Builds a server-side TLS context for the new network stack from a PEM cert
// chain + private key (+ optional client-cert CA). Modern only (TLS 1.2+,
// ECDHE, no renegotiation) and ALPN-capable (advertises http/1.1 today, ready
// for h2 / pg direct-SSL). Owns its config; deliberately not SslServerFeature.
// Throws on a missing/invalid cert or key.
asio_ns::ssl::context BuildServerTlsContext(const std::string& cert_file,
                                            const std::string& key_file,
                                            const std::string& ca_file);

}  // namespace sdb::network
