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

#include "endpoint.h"

#include <absl/strings/str_cat.h>
#include <errno.h>

#include <cstdint>
#include <cstring>
#include <limits>

#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/socket-utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "endpoint/endpoint_ip.h"
#include "endpoint/endpoint_ip_v4.h"
#include "endpoint/endpoint_ip_v6.h"
#include "endpoint/endpoint_srv.h"

#if SERENEDB_HAVE_DOMAIN_SOCKETS
#include "endpoint/endpoint_unix_domain.h"
#endif

namespace sdb {

using namespace sdb::basics;

Endpoint::Endpoint(DomainType domain_type, EndpointType type,
                   TransportType transport, EncryptionType encryption,
                   std::string_view specification, int listen_backlog)
  : _domain_type(domain_type),
    _type(type),
    _transport(transport),
    _encryption(encryption),
    _specification(specification),
    _listen_backlog(listen_backlog),
    _connected(false) {
  Sdbinvalidatesocket(&_socket);
}

std::string Endpoint::uriForm(std::string_view endpoint) {
  if (endpoint.starts_with("http+tcp://")) {
    return absl::StrCat("http://", endpoint.substr(11));
  } else if (endpoint.starts_with("http+ssl://")) {
    return absl::StrCat("https://", endpoint.substr(11));
  } else if (endpoint.starts_with("tcp://")) {
    return absl::StrCat("http://", endpoint.substr(6));
  } else if (endpoint.starts_with("ssl://")) {
    return absl::StrCat("https://", endpoint.substr(6));
  } else if (endpoint.starts_with("unix://")) {
    return std::string{endpoint};
  } else if (endpoint.starts_with("http+unix://")) {
    return absl::StrCat("unix://", endpoint.substr(12));
  } else {
    return {};
  }
}

std::string Endpoint::unifiedForm(std::string_view spec) {
  if (spec.size() < 7) {
    return {};
  }
  spec = string_utils::Trim(spec);
  if (spec.ends_with('/')) {
    // address ends with a slash => remove
    spec.remove_prefix(1);
  }

  TransportType protocol = TransportType::HTTP;

  std::string prefix{kHttp};
  static constexpr std::string_view kLocalName = "localhost";
  static constexpr std::string_view kLocalIP = "127.0.0.1";

  std::string copy;
  if (spec.starts_with("https://")) {
    // turn https:// into ssl:// for convenience
    copy = absl::StrCat("ssl://", spec.substr(8));
    spec = copy;
  } else if (spec.starts_with("http://")) {
    // turn http:// into tcp:// for convenience
    copy = absl::StrCat("tcp://", spec.substr(7));
    spec = copy;
  }

  const auto pos = spec.find("://");
  if (pos == std::string_view::npos) {
    return {};
  }
  // lowercase schema for prefix-checks
  auto schema = absl::AsciiStrToLower(spec.substr(0, pos + 3));

  // read protocol from string
  if (schema.starts_with(kHttp) || schema.starts_with("http@")) {
    protocol = TransportType::HTTP;
    prefix = kHttp;
    spec = spec.substr(kHttp.size());
    schema = schema.substr(kHttp.size());
  } else if (schema.starts_with(kPgSql)) {
    protocol = TransportType::PGSQL;
    prefix = kPgSql;
    spec = spec.substr(kPgSql.size());
    schema = schema.substr(kPgSql.size());
  }

  if (schema.starts_with("unix://")) {
#if SERENEDB_HAVE_DOMAIN_SOCKETS
    return absl::StrCat(prefix, schema, spec.substr(7));
#else
    // no unix socket for windows
    return {};
#endif
  } else if (schema.starts_with("srv://")) {
    return absl::StrCat(prefix, schema, spec.substr(6));
  }

  // strip tcp:// or ssl://
  if (schema.starts_with("ssl://")) {
    prefix.append("ssl://");
  } else if (schema.starts_with("tcp://")) {
    prefix.append("tcp://");
  } else {
    return {};
  }

  copy = absl::AsciiStrToLower(spec.substr(6));
  spec = copy;

  // handle tcp or ssl
  size_t found;
  if (spec[0] == '[') {
    // ipv6
    found = spec.find("]:", 1);
    if (found != std::string_view::npos && found > 2 &&
        found + 2 < spec.size()) {
      // hostname and port (e.g. [address]:port)
      return absl::StrCat(prefix, spec);
    }

    found = spec.find("]", 1);
    if (found != std::string_view::npos && found > 2 &&
        found + 1 == spec.size()) {
      // hostname only (e.g. [address])
      if (protocol == TransportType::PGSQL) {
        return absl::StrCat(prefix, spec, ":", EndpointIp::kDefaultPortPgSql);
      } else {
        SDB_ASSERT(protocol == TransportType::HTTP);
        return absl::StrCat(prefix, spec, ":", EndpointIp::kDefaultPortHttp);
      }
    }

    // invalid address specification
    return {};
  }

  // Replace localhost with 127.0.0.1
  found = spec.find(kLocalName);
  if (found != std::string::npos) {
    copy.replace(found, kLocalName.length(), kLocalIP);
    spec = copy;
  }

  // ipv4
  found = spec.find(':');
  if (found != std::string::npos && found + 1 < spec.size()) {
    // hostname and port
    return prefix + spec;
  }

  // hostname only
  if (protocol == TransportType::HTTP) {
    return absl::StrCat(prefix, spec, ":", EndpointIp::kDefaultPortHttp);
  } else {
    SDB_ASSERT(protocol == TransportType::PGSQL);
    return absl::StrCat(prefix, spec, ":", EndpointIp::kDefaultPortPgSql);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// create a server endpoint object from a string value
////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<Endpoint> Endpoint::serverFactory(
  std::string_view specification, int listen_backlog, bool reuse_address) {
  return Endpoint::factory(EndpointType::Server, specification, listen_backlog,
                           reuse_address);
}

////////////////////////////////////////////////////////////////////////////////
/// create a client endpoint object from a string value
////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<Endpoint> Endpoint::clientFactory(
  std::string_view specification) {
  return Endpoint::factory(EndpointType::Client, specification, 0, false);
}

////////////////////////////////////////////////////////////////////////////////
/// create an endpoint object from a string value
////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<Endpoint> Endpoint::factory(Endpoint::EndpointType type,
                                            std::string_view specification,
                                            int listen_backlog,
                                            bool reuse_address) {
  if (specification.size() < 7) {
    return nullptr;
  }

  // backlog is only allowed for server endpoints
  SDB_ASSERT(listen_backlog == 0 || type != EndpointType::Client);

  if (listen_backlog == 0 && type == EndpointType::Server) {
    // use some default value
    listen_backlog = 10;
  }

  std::string copy = unifiedForm(specification);
  TransportType protocol = TransportType::HTTP;

  if (copy.starts_with(kHttp)) {
    copy = copy.substr(kHttp.size());
  } else if (copy.starts_with(kPgSql)) {
    copy = copy.substr(kPgSql.size());
    protocol = TransportType::PGSQL;
  } else {
    // invalid protocol
    return nullptr;
  }

  EncryptionType encryption = EncryptionType::None;

  if (copy.starts_with("unix://")) {
#if SERENEDB_HAVE_DOMAIN_SOCKETS
    return std::make_unique<EndpointUnixDomain>(type, listen_backlog,
                                                copy.substr(7));
#else
    // no unix socket for windows
    return nullptr;
#endif
  }

  if (copy.starts_with("srv://")) {
    if (type != EndpointType::Client) {
      return nullptr;
    }

    return std::make_unique<EndpointSrv>(copy.substr(6));
  }

  if (copy.starts_with("ssl://")) {
    encryption = EncryptionType::SSL;
  } else if (!copy.starts_with("tcp://")) {
    // invalid type
    return nullptr;
  }

  // tcp or ssl
  copy = copy.substr(6);
  uint16_t default_port = EndpointIp::kDefaultPortHttp;
  size_t found;

  if (copy[0] == '[') {
    found = copy.find("]:", 1);

    // hostname and port (e.g. [address]:port)
    if (found != std::string::npos && found > 2 && found + 2 < copy.size()) {
      int64_t value = string_utils::Int64(copy.substr(found + 2));
      // check port over-/underrun
      if (value < (std::numeric_limits<uint16_t>::min)() ||
          value > (std::numeric_limits<uint16_t>::max)()) {
        SDB_ERROR(GENERAL, "specified port number '", value,
                  "' is outside the allowed range");
        return nullptr;
      }
      uint16_t port = static_cast<uint16_t>(value);
      std::string host = copy.substr(1, found - 1);

      return std::make_unique<EndpointIpV6>(
        type, protocol, encryption, listen_backlog, reuse_address, host, port);
    }

    found = copy.find("]", 1);

    // hostname only (e.g. [address])
    if (found != std::string::npos && found > 2 && found + 1 == copy.size()) {
      std::string host = copy.substr(1, found - 1);

      return std::make_unique<EndpointIpV6>(type, protocol, encryption,
                                            listen_backlog, reuse_address, host,
                                            default_port);
    }

    // invalid address specification
    return nullptr;
  }

  // ipv4
  found = copy.find(':');

  // hostname and port
  if (found != std::string::npos && found + 1 < copy.size()) {
    int64_t value = string_utils::Int64(copy.substr(found + 1));
    // check port over-/underrun
    if (value < (std::numeric_limits<uint16_t>::min)() ||
        value > (std::numeric_limits<uint16_t>::max)()) {
      SDB_ERROR(GENERAL, "specified port number '", value,
                "' is outside the allowed range");
      return nullptr;
    }
    uint16_t port = static_cast<uint16_t>(value);
    std::string host = copy.substr(0, found);

    return std::make_unique<EndpointIpV4>(
      type, protocol, encryption, listen_backlog, reuse_address, host, port);
  }

  // hostname only
  return std::make_unique<EndpointIpV4>(type, protocol, encryption,
                                        listen_backlog, reuse_address, copy,
                                        default_port);
}

std::string Endpoint::defaultEndpoint(TransportType type) {
  switch (type) {
    case TransportType::HTTP:
      return absl::StrCat("http+tcp://", EndpointIp::kDefaultHost, ":",
                          EndpointIp::kDefaultPortHttp);
    default:
      SDB_THROW(sdb::ERROR_INTERNAL, "invalid transport type");
  }
}

////////////////////////////////////////////////////////////////////////////////
/// compare two endpoints
////////////////////////////////////////////////////////////////////////////////

bool Endpoint::operator==(const Endpoint& that) const {
  return specification() == that.specification();
}

////////////////////////////////////////////////////////////////////////////////
/// set socket timeout
////////////////////////////////////////////////////////////////////////////////

bool Endpoint::setTimeout(SocketWrapper s, double timeout) {
  return Sdbsetsockopttimeout(s, timeout);
}

////////////////////////////////////////////////////////////////////////////////
/// set common socket flags
////////////////////////////////////////////////////////////////////////////////

bool Endpoint::setSocketFlags(SocketWrapper s) {
  if (_encryption == EncryptionType::SSL && _type == EndpointType::Client) {
    // SSL client endpoints are not set to non-blocking
    return true;
  }

  // set to non-blocking, executed for both client and server endpoints
  bool ok = SdbSetNonBlockingSocket(s);

  if (!ok) {
    SDB_ERROR(GENERAL,
              "cannot switch to non-blocking: ", errno, " (", strerror(errno),
              ")");

    return false;
  }

  // set close-on-exec flag, executed for both client and server endpoints
  ok = SdbSetCloseOnExecSocket(s);

  if (!ok) {
    SDB_ERROR(GENERAL, "cannot set close-on-exit: ", errno,
              " (", strerror(errno), ")");

    return false;
  }

  return true;
}

}  // namespace sdb
