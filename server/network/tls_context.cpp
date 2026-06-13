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

#include "network/tls_context.h"

#include <openssl/ssl.h>

namespace sdb::network {
namespace {

// ALPN selection callback. The server's supported protocols in preference
// order, in ALPN wire format (1-byte length prefix per protocol). When an H2
// codec lands, prepend "\x02h2"; for pg direct-SSL, add "\x0apostgresql".
int AlpnSelect(SSL* /*ssl*/, const unsigned char** out, unsigned char* outlen,
               const unsigned char* in, unsigned int inlen, void* /*arg*/) {
  static constexpr unsigned char kProtocols[] = {8,   'h', 't', 't', 'p',
                                                 '/', '1', '.', '1'};
  unsigned char* selected = nullptr;
  if (SSL_select_next_proto(&selected, outlen, kProtocols, sizeof(kProtocols),
                            in, inlen) != OPENSSL_NPN_NEGOTIATED) {
    // No protocol in common: proceed without ALPN (the client falls back to
    // its default, e.g. HTTP/1.1).
    return SSL_TLSEXT_ERR_NOACK;
  }
  *out = selected;
  return SSL_TLSEXT_ERR_OK;
}

}  // namespace

asio_ns::ssl::context BuildServerTlsContext(const std::string& cert_file,
                                            const std::string& key_file,
                                            const std::string& ca_file) {
  asio_ns::ssl::context ctx{asio_ns::ssl::context::tls_server};
  ctx.set_options(asio_ns::ssl::context::default_workarounds |
                  asio_ns::ssl::context::single_dh_use);

  auto* native = ctx.native_handle();
  // TLS 1.2 floor (1.3 preferred); explicit min/max beats stacking no_tlsvN.
  SSL_CTX_set_min_proto_version(native, TLS1_2_VERSION);
  SSL_CTX_set_max_proto_version(native, TLS1_3_VERSION);
  // Server picks the cipher; kill renegotiation (DoS) and compression (CRIME).
  SSL_CTX_set_options(native, SSL_OP_CIPHER_SERVER_PREFERENCE |
                                SSL_OP_NO_RENEGOTIATION |
                                SSL_OP_NO_COMPRESSION);
  // TLS 1.2 suites: forward-secret AEAD only (Mozilla "intermediate"). TLS 1.3
  // suites are fixed by OpenSSL and always on.
  SSL_CTX_set_cipher_list(
    native,
    "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305");
  SSL_CTX_set1_groups_list(native, "X25519:P-256");

  ctx.use_certificate_chain_file(cert_file);
  ctx.use_private_key_file(key_file, asio_ns::ssl::context::pem);
  if (!ca_file.empty()) {
    ctx.load_verify_file(ca_file);
    // Request + verify a client cert if presented, but don't require one
    // (password auth must still work without a client cert).
    ctx.set_verify_mode(asio_ns::ssl::verify_peer);
  } else {
    ctx.set_verify_mode(asio_ns::ssl::verify_none);
  }

  SSL_CTX_set_alpn_select_cb(native, AlpnSelect, nullptr);
  return ctx;
}

}  // namespace sdb::network
