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

#include <cstdint>
#include <string>

#include "basics/socket-utils.h"
#include "endpoint/endpoint.h"

struct addrinfo;

namespace sdb {
class EndpointIp : public Endpoint {
 protected:
  EndpointIp(DomainType, EndpointType, TransportType, EncryptionType, int, bool,
             const std::string&, const uint16_t);

 public:
  ~EndpointIp() override;

  static constexpr uint16_t kDefaultPortHttp{8529};
  static constexpr uint16_t kDefaultPortPgSql{5432};
  static constexpr std::string_view kDefaultHost{"127.0.0.1"};

 private:
  SocketWrapper connectSocket(const addrinfo*, double, double);

 public:
  SocketWrapper connect(double, double) final;
  void disconnect() final;

  int port() const override { return _port; }
  std::string host() const override { return _host; }

  bool reuseAddress() const { return _reuse_address; }

 private:
  const std::string _host;
  const uint16_t _port;
  const bool _reuse_address;
};
}  // namespace sdb
