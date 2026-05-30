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
#include <stdio.h>

#include <cstring>

#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef SERENEDB_HAVE_NETINET_STAR_H
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif

#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"
#include "endpoint/endpoint.h"
#include "endpoint_ip.h"

using namespace sdb;
using namespace sdb::basics;

#define STR_ERROR() strerror(errno)

static std::string BuildSpecification(Endpoint::DomainType domain_type,
                                      Endpoint::TransportType transport,
                                      Endpoint::EncryptionType encryption,
                                      const std::string& host,
                                      const uint16_t port) {
  std::string specification;

  switch (transport) {
    case Endpoint::TransportType::HTTP:
      specification = kHttp;
      break;
    case Endpoint::TransportType::PGSQL:
      specification = kPgSql;
      break;
  }

  switch (encryption) {
    case Endpoint::EncryptionType::None:
      specification += "tcp://";
      break;
    case Endpoint::EncryptionType::SSL:
      specification += "ssl://";
      break;
  }

  switch (domain_type) {
    case Endpoint::DomainType::IPv4:
      specification += host + ":" + string_utils::Itoa(port);
      break;

    case Endpoint::DomainType::IPv6:
      specification += "[" + host + "]" + ":" + string_utils::Itoa(port);
      break;

    default:
      SDB_ASSERT(false);
      break;
  }

  return specification;
}

////////////////////////////////////////////////////////////////////////////////
/// creates an IP socket endpoint
////////////////////////////////////////////////////////////////////////////////

EndpointIp::EndpointIp(DomainType domain_type, EndpointType type,
                       TransportType transport, EncryptionType encryption,
                       int listen_backlog, bool reuse_address,
                       const std::string& host, const uint16_t port)
  : Endpoint(domain_type, type, transport, encryption,
             BuildSpecification(domain_type, transport, encryption, host, port),
             listen_backlog),
    _host(host),
    _port(port),
    _reuse_address(reuse_address) {
  SDB_ASSERT(domain_type == Endpoint::DomainType::IPv4 ||
             domain_type == Endpoint::DomainType::IPv6);
}

////////////////////////////////////////////////////////////////////////////////
/// destroys an IP socket endpoint
////////////////////////////////////////////////////////////////////////////////

EndpointIp::~EndpointIp() {
  if (_connected) {
    disconnect();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// connects a socket
////////////////////////////////////////////////////////////////////////////////

SocketWrapper EndpointIp::connectSocket(const struct addrinfo* aip,
                                        double connect_timeout,
                                        double request_timeout) {
  const char* p_err;
  char err_buf[1080];

  // set address and port
  char host[NI_MAXHOST];
  char serv[NI_MAXSERV];

  if (::getnameinfo(aip->ai_addr, (socklen_t)aip->ai_addrlen, host,
                    sizeof(host), serv, sizeof(serv),
                    NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
    SDB_TRACE(GENERAL, "bind to address '", host,
              "', port ", _port);
  }

  SocketWrapper listen_socket;
  listen_socket = Sdbsocket(aip->ai_family, aip->ai_socktype, aip->ai_protocol);

  if (!Sdbisvalidsocket(listen_socket)) {
    p_err = STR_ERROR();
    snprintf(err_buf, sizeof(err_buf), "socket() failed with %d - %s", errno,
             p_err);

    error_message = err_buf;
    return listen_socket;
  }

  if (_type == EndpointType::Server) {
    // try to reuse address
    if (_reuse_address) {
      int opt = 1;
      if (Sdbsetsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR,
                        reinterpret_cast<char*>(&opt), sizeof(opt)) == -1) {
        p_err = STR_ERROR();
        snprintf(err_buf, sizeof(err_buf), "setsockopt() failed with #%d - %s",
                 errno, p_err);

        error_message = err_buf;

        Sdbclosesocket(listen_socket);
        Sdbinvalidatesocket(&listen_socket);
        return listen_socket;
      }
    }

    // server needs to bind to socket
    int result = Sdbbind(listen_socket, aip->ai_addr, aip->ai_addrlen);

    if (result != 0) {
      p_err = STR_ERROR();
      snprintf(err_buf, sizeof(err_buf),
               "bind(address '%s', port %d) failed with #%d - %s", host,
               (int)_port, errno, p_err);

      error_message = err_buf;

      Sdbclosesocket(listen_socket);

      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }

    // listen for new connection, executed for server endpoints only
    SDB_TRACE(GENERAL, "using backlog size ",
              _listen_backlog);
    result = Sdblisten(listen_socket, _listen_backlog);

    if (result != 0) {
      p_err = STR_ERROR();
      snprintf(err_buf, sizeof(err_buf), "listen() failed with #%d - %s", errno,
               p_err);

      error_message = err_buf;

      Sdbclosesocket(listen_socket);
      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }
  } else if (_type == EndpointType::Client) {
    // connect to endpoint, executed for client endpoints only

    // set timeout
    setTimeout(listen_socket, connect_timeout);

    int result = Sdbconnect(listen_socket, (const struct sockaddr*)aip->ai_addr,
                            aip->ai_addrlen);

    if (result != 0) {
      p_err = STR_ERROR();
      snprintf(err_buf, sizeof(err_buf), "connect() failed with #%d - %s",
               errno, p_err);

      error_message = err_buf;

      Sdbclosesocket(listen_socket);
      Sdbinvalidatesocket(&listen_socket);
      return listen_socket;
    }
  }

  if (!setSocketFlags(listen_socket)) {  // set some common socket flags for
                                         // client and server
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

////////////////////////////////////////////////////////////////////////////////
/// connect the endpoint
////////////////////////////////////////////////////////////////////////////////

SocketWrapper EndpointIp::connect(double connect_timeout,
                                  double request_timeout) {
  struct addrinfo* result = nullptr;
  struct addrinfo* aip;
  struct addrinfo hints;
  int error;
  SocketWrapper listen_socket;
  Sdbinvalidatesocket(&listen_socket);

  SDB_DEBUG(GENERAL, "connecting to ip endpoint '",
            _specification, "'");

  SDB_ASSERT(!Sdbisvalidsocket(_socket));
  SDB_ASSERT(!_connected);

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = domain();  // Allow IPv4 or IPv6
  hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV | AI_ALL;
  hints.ai_socktype = SOCK_STREAM;

  std::string port_string = string_utils::Itoa(_port);

  error = getaddrinfo(_host.c_str(), port_string.c_str(), &hints, &result);

  if (error != 0) {
    error_message = std::string("getaddrinfo for host '") + _host +
                    std::string("': ") + gai_strerror(error);

    if (result != nullptr) {
      freeaddrinfo(result);
    }
    return listen_socket;
  }

  // Try all returned addresses until one works
  for (aip = result; aip != nullptr; aip = aip->ai_next) {
    // try to bind the address info pointer
    listen_socket = connectSocket(aip, connect_timeout, request_timeout);
    if (Sdbisvalidsocket(listen_socket)) {
      // OK
      break;
    }
  }

  freeaddrinfo(result);

  return listen_socket;
}

////////////////////////////////////////////////////////////////////////////////
/// destroys an IPv4 socket endpoint
////////////////////////////////////////////////////////////////////////////////

void EndpointIp::disconnect() {
  if (_connected) {
    SDB_ASSERT(Sdbisvalidsocket(_socket));

    _connected = false;
    Sdbclosesocket(_socket);
    Sdbinvalidatesocket(&_socket);
  }
}
