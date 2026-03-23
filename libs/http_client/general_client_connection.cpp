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

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "basics/common.h"
#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_POLL_H
#include <poll.h>
#endif

#ifdef SERENEDB_HAVE_WINSOCK2_H
#include <WS2tcpip.h>
#include <WinSock2.h>
#endif

#include "app/app_server.h"
#include "app/communication_phase.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/socket-utils.h"
#include "basics/string_buffer.h"
#include "basics/system-functions.h"
#include "general_client_connection.h"
#include "http_client/client_connection.h"
#include "http_client/ssl_client_connection.h"

#define STR_ERROR() strerror(errno)

namespace sdb::httpclient {

using namespace basics;

GeneralClientConnection::GeneralClientConnection(
  app::CommunicationFeaturePhase& comm, std::unique_ptr<Endpoint> endpoint,
  double request_timeout, double connect_timeout, size_t connect_retries)
  : _endpoint{std::move(endpoint)},
    _comm{comm},
    _request_timeout{request_timeout},
    _connect_timeout{connect_timeout},
    _connect_retries{connect_retries} {
  Sdbinvalidatesocket(&_socket);
}

GeneralClientConnection::~GeneralClientConnection() = default;

std::unique_ptr<GeneralClientConnection> GeneralClientConnection::factory(
  app::CommunicationFeaturePhase& comm, std::unique_ptr<Endpoint> endpoint,
  double request_timeout, double connect_timeout, size_t num_retries,
  uint64_t ssl_protocol) {
  if (endpoint->encryption() == Endpoint::EncryptionType::SSL) {
    return std::make_unique<SslClientConnection>(
      comm, std::move(endpoint), request_timeout, connect_timeout, num_retries,
      ssl_protocol);
  }
  SDB_ASSERT(endpoint->encryption() == Endpoint::EncryptionType::None);
  return std::make_unique<ClientConnection>(
    comm, std::move(endpoint), request_timeout, connect_timeout, num_retries);
}

void GeneralClientConnection::repurpose(double connect_timeout,
                                        double request_timeout,
                                        size_t connect_retries) {
  _request_timeout = request_timeout;
  _connect_timeout = connect_timeout;
  _connect_retries = connect_retries;
  _num_connect_retries = 0;
  setInterrupted(false);
}

bool GeneralClientConnection::connect() {
  disconnect();
  if (_num_connect_retries >= _connect_retries + 1) {
    return false;
  }
  _num_connect_retries++;
  connectSocket();
  if (!_is_connected) {
    return false;
  }
  _num_connect_retries = 0;
  return true;
}

void GeneralClientConnection::disconnect() {
  if (isConnected()) {
    disconnectSocket();
    _num_connect_retries = 0;
    _is_connected = false;
  }
}

bool GeneralClientConnection::prepare(SocketWrapper socket, double timeout,
                                      bool is_write) {
  // wait for at most 0.5 seconds for poll/select to complete
  // if it takes longer, break each poll/select into smaller chunks so we can
  // interrupt the whole process if it takes too long in total
  static constexpr double kPollDuration = 0.5;
  const auto fd = SdbgetFdOrHandleOfSocket(socket);
  double start = utilities::GetMicrotime();
  int res;

#ifdef SERENEDB_HAVE_POLL_H
  // Here we have poll, on all other platforms we use select
  double since_last_socket_check = start;
  bool nowait = (timeout == 0.0);
  int towait;
  if (timeout * 1000.0 > static_cast<double>(INT_MAX)) {
    towait = INT_MAX;
  } else {
    towait = static_cast<int>(timeout * 1000.0);
  }

  struct pollfd poller;
  memset(&poller, 0, sizeof(struct pollfd));  // for our old friend Valgrind
  poller.fd = fd;
  poller.events = (is_write ? POLLOUT : POLLIN);

  while (true) {  // will be left by break
    res = poll(&poller, 1,
               towait > static_cast<int>(kPollDuration * 1000.0)
                 ? static_cast<int>(kPollDuration * 1000.0)
                 : towait);
    if (res == -1 && errno == EINTR) {
      if (!nowait) {
        double end = utilities::GetMicrotime();
        towait -= static_cast<int>((end - start) * 1000.0);
        start = end;
        if (towait <= 0) {  // Should not happen, but there might be rounding
                            // errors, so just to prevent a poll call with
                            // negative timeout...
          res = 0;
          break;
        }
      }
      continue;
    }

    if (res == 0) {
      if (isInterrupted() || !_comm.IsCommAllowed()) {
        _error_details = std::string("command locally aborted");
        SetError(ERROR_REQUEST_CANCELED);
        return false;
      }
      double end = utilities::GetMicrotime();
      towait -= static_cast<int>((end - start) * 1000.0);
      if (towait <= 0) {
        break;
      }

      // periodically recheck our socket
      if (end - since_last_socket_check >= 20.0) {
        since_last_socket_check = end;
        if (!checkSocket()) {
          // socket seems broken. now escape this loop
          break;
        }
      }

      start = end;
      continue;
    }

    break;
  }
// Now res can be:
//   1 : if the file descriptor was ready
//   0 : if the timeout happened
//   -1: if an error happened, EINTR within the timeout is already caught
#else
  // All other versions use select:

  // An fd_set is a fixed size buffer.
  // Executing FD_CLR() or FD_SET() with a value of fd that is negative or is
  // equal to or larger than FD_SETSIZE
  // will result in undefined behavior. Moreover, POSIX requires fd to be a
  // valid file descriptor.
  if (fd < 0 || fd >= FD_SETSIZE) {
    // invalid or too high file descriptor value...
    // if we call FD_ZERO() or FD_SET() with it, the program behavior will be
    // undefined
    _error_details = std::string("file descriptor value too high");
    return false;
  }

  // handle interrupt
  do {
  retry:
    fd_set fdset;
    FD_ZERO(&fdset);
    FD_SET(fd, &fdset);

    fd_set* readFds = nullptr;
    fd_set* writeFds = nullptr;

    if (isWrite) {
      writeFds = &fdset;
    } else {
      readFds = &fdset;
    }

    int sockn = (int)(fd + 1);

    double waitTimeout = timeout;
    if (waitTimeout > POLL_DURATION) {
      waitTimeout = POLL_DURATION;
    }

    struct timeval t;
    t.tv_sec = (long)waitTimeout;
    t.tv_usec = (long)((waitTimeout - (double)t.tv_sec) * 1000000.0);

    res = select(sockn, readFds, writeFds, nullptr, &t);

    if ((res == -1 && errno == EINTR)) {
      int myerrno = errno;
      double end = utilities::GetMicrotime();
      errno = myerrno;
      timeout = timeout - (end - start);
      start = end;
    } else if (res == 0) {
      if (isInterrupted() || !_comm.getCommAllowed()) {
        _error_details = std::string("command locally aborted");
        SetError(ERROR_REQUEST_CANCELED);
        return false;
      }
      double end = utilities::GetMicrotime();
      timeout = timeout - (end - start);
      if (timeout <= 0.0) {
        break;
      }
      start = end;
      goto retry;
    }
  } while (res == -1 && errno == EINTR && timeout > 0.0);
#endif

  if (res > 0) {
    if (isInterrupted() || !_comm.IsCommAllowed()) {
      _error_details = std::string("command locally aborted");
      SetError(ERROR_REQUEST_CANCELED);
      return false;
    }
    return true;
  }

  if (res == 0) {
    if (is_write) {
      _error_details = std::string("timeout during write");
      SetError(ERROR_SIMPLE_CLIENT_COULD_NOT_WRITE);
    } else {
      _error_details = std::string("timeout during read");
      SetError(ERROR_SIMPLE_CLIENT_COULD_NOT_READ);
    }
  } else {  // res < 0
    const char* p_err = STR_ERROR();
    _error_details = std::string("during prepare: ") + std::to_string(errno) +
                     std::string(" - ") + p_err;

    SetError(ERROR_SYS_ERROR);
  }

  return false;
}

bool GeneralClientConnection::checkSocket() {
  int so_error = -1;
  socklen_t len = sizeof so_error;

  SDB_ASSERT(Sdbisvalidsocket(_socket));

  int res =
    Sdbgetsockopt(_socket, SOL_SOCKET, SO_ERROR, (void*)&so_error, &len);

  if (res != 0) {
    SetError(ERROR_SYS_ERROR);
    disconnect();
    return false;
  }

  if (so_error == 0) {
    return true;
  }

  errno = so_error;
  SetError(ERROR_SYS_ERROR);
  disconnect();

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// handleWrite
/// Write data to endpoint, this uses select to block until some
/// data can be written. Then it writes as much as it can without further
/// blocking, not calling select again. What has happened is
/// indicated by the return value and the bytesWritten variable,
/// which is always set by this method. The bytesWritten indicates
/// how many bytes have been written from the buffer
/// (regardless of whether there was an error or not). The return value
/// indicates, whether an error has happened. Note that the other side
/// closing the connection is not considered to be an error! The call to
/// prepare() does a select and the call to readClientConnection does
/// what is described here.
////////////////////////////////////////////////////////////////////////////////

bool GeneralClientConnection::handleWrite(double timeout, const void* buffer,
                                          size_t length,
                                          size_t* bytes_written) {
  *bytes_written = 0;

  if (prepare(_socket, timeout, true)) {
    return this->writeClientConnection(buffer, length, bytes_written);
  }

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// handleRead
/// Read data from endpoint, this uses select to block until some
/// data has arrived. Then it reads as much as it can without further
/// blocking, using select multiple times. What has happened is
/// indicated by two flags, the return value and the connectionClosed flag,
/// which is always set by this method. The connectionClosed flag indicates
/// whether or not the connection has been closed by the other side
/// (regardless of whether there was an error or not). The return value
/// indicates, whether an error has happened. Note that the other side
/// closing the connection is not considered to be an error! The call to
/// prepare() does a select and the call to readClientCollection does
/// what is described here.
////////////////////////////////////////////////////////////////////////////////

bool GeneralClientConnection::handleRead(double timeout, StringBuffer& buffer,
                                         bool& connection_closed) {
  connection_closed = false;

  if (prepare(_socket, timeout, false)) {
    return this->readClientConnection(buffer, connection_closed);
  }

  connection_closed = true;
  return false;
}

app::AppServer& GeneralClientConnection::server() const {
  return _comm.server();
}

bool GeneralClientConnection::isIdleConnection() const noexcept {
#ifdef __linux__
  char buffer[16];
  return recv(_socket.file_descriptor, buffer, sizeof(buffer),
              MSG_PEEK | MSG_DONTWAIT) < 0;
#else
  return true;
#endif
}

}  // namespace sdb::httpclient
