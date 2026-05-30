////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "ssl_helper.h"

#include <openssl/err.h>
#include <openssl/opensslconf.h>
#include <string.h>

#include <algorithm>
#include <boost/asio/ssl/context_base.hpp>
#include <boost/asio/ssl/impl/context.ipp>
#include <boost/system/error_code.hpp>
#include <cstdint>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"

using namespace sdb;

#ifndef OPENSSL_NO_SSL3_METHOD
extern "C" const SSL_METHOD* SSLv3_method(void);
#endif

////////////////////////////////////////////////////////////////////////////////
/// creates an SSL context
////////////////////////////////////////////////////////////////////////////////

asio_ns::ssl::context sdb::SslContext(SslProtocol protocol,
                                      const std::string& keyfile) {
  // create our context

  asio_ns::ssl::context::method meth;

  switch (protocol) {
    case kSslV2:
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "support for SSLv2 has been dropped");

#ifndef OPENSSL_NO_SSL3_METHOD
    case SSL_V3:
      meth = asio_ns::ssl::context::method::sslv3;
      break;
#endif
    case kSslV23:
      meth = asio_ns::ssl::context::method::sslv23;
      break;

    case kTlsV1:
      meth = asio_ns::ssl::context::method::tlsv1_server;
      break;

    case kTlsV12:
      meth = asio_ns::ssl::context::method::tlsv12_server;
      break;

    case kTlsV13:
      meth = asio_ns::ssl::context::method::tlsv13_server;
      break;

    case kTlsGeneric:
      meth = asio_ns::ssl::context::method::tls_server;
      break;

    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "unknown SSL protocol method");
  }

  asio_ns::ssl::context sslctx(meth);

  if (sslctx.native_handle() == nullptr) {
    // could not create SSL context - this is mostly due to the OpenSSL
    // library not having been initialized
    SDB_THROW(ERROR_INTERNAL, "unable to create SSL context");
  }

  // load our keys and certificates
  boost::system::error_code ec;
  sslctx.use_certificate_chain_file(keyfile, ec);
  if (ec) {
    SDB_ERROR(SSL, "cannot read certificate from '", keyfile,
              "': ", ec.to_string());
    SDB_THROW(ERROR_BAD_PARAMETER, "unable to read certificate from file");
  }

  sslctx.use_private_key_file(keyfile, asio_ns::ssl::context::file_format::pem,
                              ec);
  if (ec) {
    SDB_ERROR(GENERAL, "cannot read key from '", keyfile,
              "': ", ec.to_string());
    SDB_THROW(ERROR_BAD_PARAMETER, "unable to read key from keyfile");
  }

  return sslctx;
}

////////////////////////////////////////////////////////////////////////////////
/// get the name of an SSL protocol version
////////////////////////////////////////////////////////////////////////////////

std::string sdb::ProtocolName(SslProtocol protocol) {
  switch (protocol) {
    case kSslV2:
      return "SSLv2";

    case kSslV23:
      return "SSLv23";

    case kSslV3:
      return "SSLv3";

    case kTlsV1:
      return "TLSv1";

    case kTlsV12:
      return "TLSv12";

    case kTlsV13:
      return "TLSv13";

    case kTlsGeneric:
      return "TLS";

    default:
      return "unknown";
  }
}

containers::FlatHashSet<uint64_t> sdb::AvailableSslProtocols() {
  // openssl version number format is
  // MNNFFPPS: major minor fix patch status
  // TLS 1.3, only support from OpenSSL 1.1.1 onwards
  return {
    SslProtocol::kSslV2,  // unsupported!
    SslProtocol::kSslV23, SslProtocol::kSslV3,  SslProtocol::kTlsV1,
    SslProtocol::kTlsV12, SslProtocol::kTlsV13, SslProtocol::kTlsGeneric,
  };
}

std::string sdb::AvailableSslProtocolsDescription() {
  return "The SSL protocol (1 = SSLv2 (unsupported), 2 = SSLv2 or SSLv3 "
         "(negotiated), 3 = SSLv3, 4 = TLSv1, 5 = TLSv1.2, 6 = TLSv1.3, "
         "9 = generic TLS (negotiated))";
}

////////////////////////////////////////////////////////////////////////////////
/// get last SSL error
////////////////////////////////////////////////////////////////////////////////

std::string sdb::LastSslError() {
  char buf[122];
  memset(buf, 0, sizeof(buf));

  unsigned long err = ERR_get_error();
  ERR_error_string_n(err, buf, sizeof(buf) - 1);

  return std::string(buf);
}
