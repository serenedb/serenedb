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

#include "endpoint_unix_domain.h"

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS

#include <errno.h>
#include <stdio.h>
#include <sys/un.h>

#include <cstring>

#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "endpoint/endpoint.h"

using namespace sdb;
using namespace sdb::basics;

EndpointUnixDomain::EndpointUnixDomain(EndpointType type, int listen_backlog,
                                       const std::string& path)
  : Endpoint(DomainType::UNIX, type, TransportType::HTTP, EncryptionType::None,
             "http+unix://" + path, listen_backlog),
    _path(path) {}

EndpointUnixDomain::~EndpointUnixDomain() {
  if (_connected) {
    disconnect();
  }
}

SocketWrapper EndpointUnixDomain::connect(double connect_timeout,
                                          double request_timeout) {
  SocketWrapper listen_socket;
  Sdbinvalidatesocket(&listen_socket);

  SDB_DEBUG(GENERAL, "connecting to unix endpoint '",
            _specification, "'");

  SDB_ASSERT(!Sdbisvalidsocket(_socket));
  SDB_ASSERT(!_connected);

  listen_socket = Sdbsocket(AF_UNIX, SOCK_STREAM, 0);
  if (!Sdbisvalidsocket(listen_socket)) {
    SDB_ERROR(GENERAL, "socket() failed with ", errno, " (",
              strerror(errno), ")");
    return listen_socket;
  }

  struct sockaddr_un address;

  memset(&address, 0, sizeof(address));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, 100, "%s", _path.c_str());

  if (_type == EndpointType::Server) {
    int result = Sdbbind(listen_socket, (struct sockaddr*)&address,
                         (int)SUN_LEN(&address));
    if (result != 0) {
      // bind error
      SDB_ERROR(GENERAL, "bind() failed with ", errno, " (",
                strerror(errno), ")");
      Sdbclosesocket(listen_socket);
      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }

    // listen for new connection, executed for server endpoints only
    SDB_TRACE(GENERAL, "using backlog size ",
              _listen_backlog);
    result = Sdblisten(listen_socket, _listen_backlog);

    if (result < 0) {
      SDB_ERROR(GENERAL, "listen() failed with ", errno,
                " (", strerror(errno), ")");
      Sdbclosesocket(listen_socket);
      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }
  }

  else if (_type == EndpointType::Client) {
    // connect to endpoint, executed for client endpoints only

    // set timeout
    setTimeout(listen_socket, connect_timeout);

    if (Sdbconnect(listen_socket, (const struct sockaddr*)&address,
                   SUN_LEN(&address)) != 0) {
      Sdbclosesocket(listen_socket);
      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }
  }

  if (!setSocketFlags(listen_socket)) {
    Sdbclosesocket(listen_socket);
    Sdbinvalidatesocket(&listen_socket);
    return listen_socket;
  }

  if (_type == EndpointType::Client) {
    setTimeout(listen_socket, request_timeout);
  }

  _connected = true;
  _socket = listen_socket;

  return _socket;
}

void EndpointUnixDomain::disconnect() {
  if (_connected) {
    SDB_ASSERT(Sdbisvalidsocket(_socket));

    _connected = false;
    Sdbclosesocket(_socket);

    Sdbinvalidatesocket(&_socket);

    if (_type == EndpointType::Server) {
      if (file_utils::Remove(_path) != ERROR_OK) {
        SDB_TRACE(GENERAL, "unable to remove socket file '",
                  _path, "': ", LastError());
      }
    }
  }
}

#endif
