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

#include <absl/cleanup/cleanup.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>

#include "basics/common.h"
#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_WINSOCK2_H
#include <WS2tcpip.h>
#include <WinSock2.h>
#endif

#if defined(__linux__)
#include <fcntl.h>
#endif
#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#ifndef OPENSSL_VERSION_NUMBER
#error expecting OPENSSL_VERSION_NUMBER to be defined
#endif

#include <openssl/err.h>
#include <openssl/opensslconf.h>
#include <openssl/ssl3.h>
#include <openssl/x509.h>

#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/socket-utils.h"
#include "basics/ssl/ssl_helper.h"
#include "basics/string_buffer.h"
#include "ssl_client_connection.h"

#undef TRACE_SSL_CONNECTIONS

#define STR_ERROR() strerror(errno)

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::httpclient;

namespace {

#ifdef TRACE_SSL_CONNECTIONS
static std::string_view tlsTypeName(int type) {
  switch (type) {
#ifdef SSL3_RT_HEADER
    case SSL3_RT_HEADER:
      return "TLS header";
#endif
    case SSL3_RT_CHANGE_CIPHER_SPEC:
      return "TLS change cipher";
    case SSL3_RT_ALERT:
      return "TLS alert";
    case SSL3_RT_HANDSHAKE:
      return "TLS handshake";
    case SSL3_RT_APPLICATION_DATA:
      return "TLS app data";
    case SSL3_RT_INNER_CONTENT_TYPE:
      return "TLS inner content type";
    default:
      return "TLS Unknown";
  }
}

static const char* sslMessageType(int sslVersion, int msg) {
#ifdef SSL2_VERSION_MAJOR
  if (sslVersion == SSL2_VERSION_MAJOR) {
    switch (msg) {
      case SSL2_MT_ERROR:
        return "Error";
      case SSL2_MT_CLIENT_HELLO:
        return "Client hello";
      case SSL2_MT_CLIENT_MASTER_KEY:
        return "Client key";
      case SSL2_MT_CLIENT_FINISHED:
        return "Client finished";
      case SSL2_MT_SERVER_HELLO:
        return "Server hello";
      case SSL2_MT_SERVER_VERIFY:
        return "Server verify";
      case SSL2_MT_SERVER_FINISHED:
        return "Server finished";
      case SSL2_MT_REQUEST_CERTIFICATE:
        return "Request CERT";
      case SSL2_MT_CLIENT_CERTIFICATE:
        return "Client CERT";
    }
  } else
#endif
    if (sslVersion == SSL3_VERSION_MAJOR) {
    switch (msg) {
      case SSL3_MT_HELLO_REQUEST:
        return "Hello request";
      case SSL3_MT_CLIENT_HELLO:
        return "Client hello";
      case SSL3_MT_SERVER_HELLO:
        return "Server hello";
#ifdef SSL3_MT_NEWSESSION_TICKET
      case SSL3_MT_NEWSESSION_TICKET:
        return "Newsession Ticket";
#endif
      case SSL3_MT_CERTIFICATE:
        return "Certificate";
      case SSL3_MT_SERVER_KEY_EXCHANGE:
        return "Server key exchange";
      case SSL3_MT_CLIENT_KEY_EXCHANGE:
        return "Client key exchange";
      case SSL3_MT_CERTIFICATE_REQUEST:
        return "Request CERT";
      case SSL3_MT_SERVER_DONE:
        return "Server finished";
      case SSL3_MT_CERTIFICATE_VERIFY:
        return "CERT verify";
      case SSL3_MT_FINISHED:
        return "Finished";
#ifdef SSL3_MT_CERTIFICATE_STATUS
      case SSL3_MT_CERTIFICATE_STATUS:
        return "Certificate Status";
#endif
    }
  }
  return "Unknown";
}

static void sslTlsTrace(int direction, int sslVersion, int contentType,
                        const void* buf, size_t, SSL*, void*) {
  // enable this for tracing SSL connections
  if (sslVersion) {
    sslVersion >>= 8; /* check the upper 8 bits only below */
    std::string_view tlsRtName;
    if (sslVersion == SSL3_VERSION_MAJOR && contentType) {
      tlsRtName = tlsTypeName(contentType);
    } else {
      tlsRtName = "";
    }

    SDB_TRACE("xxxxx", Logger::FIXME,
              "SSL connection trace: ", (direction ? "out" : "in"), ", ",
              tlsRtName, ", ",
              sslMessageType(sslVersion, *static_cast<const char*>(buf)));
  }
}
#endif

}  // namespace

SslClientConnection::SslClientConnection(app::CommunicationFeaturePhase& comm,
                                         std::unique_ptr<Endpoint> endpoint,
                                         double request_timeout,
                                         double connect_timeout,
                                         size_t connect_retries,
                                         uint64_t ssl_protocol)
  : GeneralClientConnection{comm, std::move(endpoint), request_timeout,
                            connect_timeout, connect_retries},
    _ssl_protocol{ssl_protocol} {
  init();
}

SslClientConnection::~SslClientConnection() {
  disconnect();

  if (_ssl != nullptr) {
    SSL_free(_ssl);
  }

  if (_ctx != nullptr) {
    SSL_CTX_free(_ctx);
  }
}

void SslClientConnection::init() {
  Sdbinvalidatesocket(&_socket);

  SSL_METHOD SSL_CONST* meth = nullptr;

  switch (SslProtocol(_ssl_protocol)) {
    case kSslV2:
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "support for SSLv2 has been dropped");

#ifndef OPENSSL_NO_SSL3_METHOD
    case SSL_V3:
      meth = SSLv3_method();
      break;
#endif

    case kSslV23:
      meth = SSLv23_method();
      break;

    case kTlsV1:
      meth = TLS_client_method();
      break;

    case kTlsV12:
      meth = TLS_client_method();
      break;

    case kTlsV13:
      meth = TLS_client_method();
      break;

    case kTlsGeneric:
      meth = TLS_client_method();
      break;

    case kSslUnknown:
    default:
      // The actual protocol version used will be negotiated to the highest
      // version mutually supported by the client and the server. The supported
      // protocols are SSLv3, TLSv1, TLSv1.1 and TLSv1.2. Applications should
      // use these methods, and avoid the version-specific methods described
      // below.
      meth = TLS_method();
      break;
  }

  _ctx = SSL_CTX_new(meth);

  if (_ctx != nullptr) {
#ifdef TRACE_SSL_CONNECTIONS
    SSL_CTX_set_msg_callback(_ctx, sslTlsTrace);
#endif
    SSL_CTX_set_cipher_list(_ctx, "ALL");
    // TODO: better use the following ciphers...
    // SSL_CTX_set_cipher_list(_ctx,
    // "ALL:!EXPORT:!EXPORT40:!EXPORT56:!aNULL:!LOW:!RC4:@STRENGTH");

    bool ssl_cache = true;
    SSL_CTX_set_session_cache_mode(
      _ctx, ssl_cache ? SSL_SESS_CACHE_SERVER : SSL_SESS_CACHE_OFF);
  }
}

bool SslClientConnection::connectSocket() {
  SDB_ASSERT(_endpoint != nullptr);

  if (_endpoint->isConnected()) {
    disconnectSocket();
    _is_connected = false;
  }

  _error_details.clear();
  _socket = _endpoint->connect(_connect_timeout, _request_timeout);

  if (!Sdbisvalidsocket(_socket) || _ctx == nullptr) {
    _error_details = _endpoint->error_message;
    _is_connected = false;
    return false;
  }

  if (_is_socket_non_blocking) {
    if (!setSocketToNonBlocking()) {
      _is_connected = false;
      disconnectSocket();
      return false;
    }
  }

  _is_connected = true;

  _ssl = SSL_new(_ctx);

  if (_ssl == nullptr) {
    if (_is_socket_non_blocking) {
      // we don't care about the return value here because we were already
      // unsuccessful in the connection attempt
      cleanUpSocketFlags();
    }
    _error_details = "failed to create ssl context";
    disconnectSocket();
    _is_connected = false;
    return false;
  }

  switch (SslProtocol(_ssl_protocol)) {
    case kTlsV1:
    case kTlsV12:
    case kTlsV13:
    case kTlsGeneric:
    default:
      SSL_set_tlsext_host_name(_ssl, _endpoint->host().c_str());
  }

  SSL_set_connect_state(_ssl);

  if (SSL_set_fd(_ssl, (int)SdbgetFdOrHandleOfSocket(_socket)) != 1) {
    if (_is_socket_non_blocking) {
      // we don't care about the return value here because we were already
      // unsuccessful in the connection attempt
      cleanUpSocketFlags();
    }
    _error_details = std::string("SSL: failed to create context ") +
                     ERR_error_string(ERR_get_error(), nullptr);
    disconnectSocket();
    _is_connected = false;
    return false;
  }

  if (_verify_certificates) {
    SSL_set_verify(_ssl, SSL_VERIFY_PEER, nullptr);
    SSL_set_verify_depth(_ssl, _verify_depth);

    SSL_CTX_set_default_verify_paths(_ctx);
    SSL_CTX_set_default_verify_dir(_ctx);
  } else {
    SSL_set_verify(_ssl, SSL_VERIFY_NONE, nullptr);
  }

  ERR_clear_error();

  int ret = -1;
  int error_detail = -1;

  if (_is_socket_non_blocking) {
    auto start = std::chrono::steady_clock::now();
    while ((ret = SSL_connect(_ssl)) == -1) {
      error_detail = SSL_get_error(_ssl, ret);
      if (_is_interrupted) {
        break;
      }
      auto end = std::chrono::steady_clock::now();
      if ((error_detail != SSL_ERROR_WANT_READ &&
           error_detail != SSL_ERROR_WANT_WRITE) ||
          std::chrono::duration_cast<std::chrono::seconds>(end - start)
              .count() >= _connect_timeout) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  } else {
    if ((ret = SSL_connect(_ssl)) == -1) {
      error_detail = SSL_get_error(_ssl, ret);
    }
  }

  if (_is_socket_non_blocking) {
    if (!cleanUpSocketFlags()) {
      disconnectSocket();
      _is_connected = false;
      return false;
    }
  }

  if (ret != 1) {
    _error_details.append("SSL: during SSL_connect: ");

    long cert_error;

    if (!_is_socket_non_blocking && ((error_detail == SSL_ERROR_WANT_READ) ||
                                     (error_detail == SSL_ERROR_WANT_WRITE))) {
      return true;
    }

    /* Gets the earliest error code from the
       thread's error queue and removes the entry. */
    unsigned long last_error = ERR_get_error();

    _error_details.append(ERR_error_string(last_error, nullptr)).append(" - ");

    if (error_detail == SSL_ERROR_SYSCALL && last_error == 0) {
      if (ret == 0) {
        _error_details +=
          "an EOF was observed that violates the protocol. this may happen "
          "when the other side has closed the connection";
      } else if (ret == -1) {
        _error_details += "I/O reported by BIO";
      }
    }

    switch (error_detail) {
      case 0x1407E086:
      /* 1407E086:
        SSL routines:
        SSL2_SET_CERTIFICATE:
        certificate verify failed */
      /* fall-through */
      case 0x14090086:
        /* 14090086:
          SSL routines:
          SSL3_GET_SERVER_CERTIFICATE:
          certificate verify failed */

        cert_error = SSL_get_verify_result(_ssl);

        if (cert_error != X509_V_OK) {
          _error_details += std::string("certificate problem: ") +
                            X509_verify_cert_error_string(cert_error);
        } else {
          _error_details = "certificate problem, verify that the CA cert is OK";
        }
        break;

      default:
        char error_buffer[256];
        ERR_error_string_n(error_detail, error_buffer, sizeof(error_buffer));
        _error_details += std::string(" - details: ") + error_buffer;
        break;
    }

    disconnectSocket();
    _is_connected = false;
    return false;
  }

  SDB_TRACE("xxxxx", Logger::FIXME,
            "SSL connection opened: ", SSL_get_cipher(_ssl), ", ",
            SSL_get_cipher_version(_ssl), " (",
            SSL_get_cipher_bits(_ssl, nullptr), " bits)");

  return true;
}

void SslClientConnection::disconnectSocket() {
  _endpoint->disconnect();
  Sdbinvalidatesocket(&_socket);

  if (_ssl != nullptr) {
    SSL_free(_ssl);
    _ssl = nullptr;
  }
}

bool SslClientConnection::writeClientConnection(const void* buffer,
                                                size_t length,
                                                size_t* bytes_written) {
  SDB_ASSERT(bytes_written != nullptr);

  *bytes_written = 0;

  if (_ssl == nullptr) {
    return false;
  }

  int written = SSL_write(_ssl, buffer, (int)length);
  int err = SSL_get_error(_ssl, written);
  switch (err) {
    case SSL_ERROR_NONE:
      *bytes_written = written;
#ifdef SDB_DEV
      _written += written;
#endif
      return true;

    case SSL_ERROR_ZERO_RETURN:
      SSL_shutdown(_ssl);
      break;

    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
    case SSL_ERROR_WANT_CONNECT:
      break;

    case SSL_ERROR_SYSCALL: {
      const char* p_err = STR_ERROR();
      _error_details =
        std::string("SSL: while writing: SYSCALL returned errno = ") +
        std::to_string(errno) + std::string(" - ") + p_err;
      break;
    }

    case SSL_ERROR_SSL: {
      /*  A failure in the SSL library occurred, usually a protocol error.
          The OpenSSL error queue contains more information on the error. */
      unsigned long error_detail = ERR_get_error();
      char error_buffer[256];
      ERR_error_string_n(error_detail, error_buffer, sizeof(error_buffer));
      _error_details = std::string("SSL: while writing: ") + error_buffer;
      break;
    }

    default:
      /* a true error */
      _error_details =
        std::string("SSL: while writing: error ") + std::to_string(err);
  }

  return false;
}

bool SslClientConnection::readClientConnection(StringBuffer& string_buffer,
                                               bool& connection_closed) {
  connection_closed = true;
  if (_ssl == nullptr) {
    return false;
  }
  if (!_is_connected) {
    return true;
  }

  connection_closed = false;
  auto true_size = string_buffer.size();
  absl::Cleanup guard = [&] { string_buffer.erase(true_size); };
  do {
  again:
    string_buffer.resizeAdditional(true_size, kReadbufferSize);
    ERR_clear_error();

    int len_read =
      SSL_read(_ssl, string_buffer.data() + true_size, kReadbufferSize);

    switch (SSL_get_error(_ssl, len_read)) {
      case SSL_ERROR_NONE:
        true_size += len_read;
#ifdef SDB_DEV
        _read += len_read;
#endif
        break;

      case SSL_ERROR_ZERO_RETURN:
        connection_closed = true;
        SSL_shutdown(_ssl);
        _is_connected = false;
        return true;

      case SSL_ERROR_WANT_READ:
        goto again;

      case SSL_ERROR_WANT_WRITE:
      case SSL_ERROR_WANT_CONNECT:
      case SSL_ERROR_SYSCALL:
      default: {
        const char* p_err = STR_ERROR();
        unsigned long error_detail = ERR_get_error();
        char error_buffer[256];
        ERR_error_string_n(error_detail, error_buffer, sizeof(error_buffer));
        _error_details = std::string("SSL: while reading: error '") +
                         std::to_string(errno) + std::string("' - ") +
                         error_buffer + std::string("' - ") + p_err;

        /* unexpected */
        connection_closed = true;
        return false;
      }
    }
  } while (readable());

  return true;
}

bool SslClientConnection::readable() {
  // must use SSL_pending() and not select() as SSL_read() might read more
  // bytes from the socket into the _ssl structure than we actually requested
  // via SSL_read().
  // if we used select() to check whether there is more data available, select()
  // might return 0 already but we might not have consumed all bytes yet

  // ...........................................................................
  // SSL_pending(...) is an OpenSSL function which returns the number of bytes
  // which are available inside ssl for reading.
  // ...........................................................................

  if (SSL_pending(_ssl) > 0) {
    return true;
  }

  if (prepare(_socket, 0.0, false)) {
    return checkSocket();
  }

  return false;
}

bool SslClientConnection::setSocketToNonBlocking() {
#if defined(__linux__)
  _socket_flags = fcntl(_socket.file_descriptor, F_GETFL, 0);
  if (_socket_flags == -1) {
    _error_details = "Socket file descriptor read returned with error " +
                     std::to_string(errno);
    return false;
  }
  if (fcntl(_socket.file_descriptor, F_SETFL, _socket_flags | O_NONBLOCK) ==
      -1) {
    _error_details = "Attempt to create non-blocking socket generated error " +
                     std::to_string(errno);
    return false;
  }
#else
  u_long nonBlocking = 1;
  if (ioctlsocket(_socket.fileDescriptor, FIONBIO, &nonBlocking) != 0) {
    _error_details = "Attempt to create non-blocking socket generated error " +
                     std::to_string(WSAGetLastError());
    return false;
  }
#endif
  return true;
}

bool SslClientConnection::cleanUpSocketFlags() {
  SDB_ASSERT(_is_socket_non_blocking);
#if defined(__linux__)
  if (fcntl(_socket.file_descriptor, F_SETFL, _socket_flags & ~O_NONBLOCK) ==
      -1) {
    _error_details = "Attempt to make socket blocking generated error " +
                     std::to_string(errno);
    return false;
  }
#else
  u_long nonBlocking = 0;
  if (ioctlsocket(_socket.fileDescriptor, FIONBIO, &nonBlocking) != 0) {
    _error_details = "Attempt to make socket blocking generated error " +
                     std::to_string(WSAGetLastError());
    return false;
  }
#endif
  return true;
}
