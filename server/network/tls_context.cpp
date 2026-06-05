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
  ctx.set_options(
    asio_ns::ssl::context::default_workarounds |
    asio_ns::ssl::context::single_dh_use | asio_ns::ssl::context::no_sslv2 |
    asio_ns::ssl::context::no_sslv3 | asio_ns::ssl::context::no_tlsv1 |
    asio_ns::ssl::context::no_tlsv1_1);
  ctx.use_certificate_chain_file(cert_file);
  ctx.use_private_key_file(key_file, asio_ns::ssl::context::pem);
  if (!ca_file.empty()) {
    ctx.load_verify_file(ca_file);
    // Request a client cert and verify it if presented, but don't require one
    // (password auth must still work).
    ctx.set_verify_mode(asio_ns::ssl::verify_peer);
  } else {
    ctx.set_verify_mode(asio_ns::ssl::verify_none);
  }

  auto* native = ctx.native_handle();
  SSL_CTX_set_options(native, SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set1_groups_list(native, "X25519:P-256");
  SSL_CTX_set_alpn_select_cb(native, AlpnSelect, nullptr);
  return ctx;
}

}  // namespace sdb::network
