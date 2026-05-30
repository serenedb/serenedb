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

#include "endpoint_ip.h"

#include "basics/debugging.h"
#include "basics/string_utils.h"

using namespace sdb;
using namespace sdb::basics;

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

EndpointIp::EndpointIp(DomainType domain_type, TransportType transport,
                       EncryptionType encryption, int listen_backlog,
                       bool reuse_address, const std::string& host,
                       const uint16_t port)
  : Endpoint(domain_type, transport, encryption,
             BuildSpecification(domain_type, transport, encryption, host, port),
             listen_backlog),
    _host(host),
    _port(port),
    _reuse_address(reuse_address) {
  SDB_ASSERT(domain_type == Endpoint::DomainType::IPv4 ||
             domain_type == Endpoint::DomainType::IPv6);
}
