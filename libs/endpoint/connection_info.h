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

#pragma once

#include <sys/socket.h>

#include <cstring>

#include "basics/common.h"
#include "endpoint/endpoint.h"

namespace sdb {

struct ConnectionInfo {
 public:
  ConnectionInfo()
    : server_address(),
      client_address(),
      endpoint(),
      sas_len(0),
      server_port(0),
      client_port(0),
      endpoint_type(Endpoint::DomainType::Unknown),
      encryption_type(Endpoint::EncryptionType::None) {
    std::memset(&sas, 0, sizeof(sas));
  }

 public:
  std::string portType() const {
    switch (endpoint_type) {
      case Endpoint::DomainType::UNIX:
        return "unix";
      case Endpoint::DomainType::IPv4:
      case Endpoint::DomainType::IPv6:
        return "tcp/ip";
      default:
        return "unknown";
    }
  }

  std::string fullClient() const {
    return client_address + ":" + std::to_string(client_port);
  }

  std::string server_address;
  std::string client_address;
  std::string endpoint;

  sockaddr_storage sas;
  size_t sas_len;
  int server_port;
  int client_port;

  Endpoint::DomainType endpoint_type;
  Endpoint::EncryptionType encryption_type;
};

}  // namespace sdb
