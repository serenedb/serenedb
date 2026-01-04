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

#include "basics/operating-system.h"
#include "basics/socket-utils.h"
#include "endpoint/endpoint.h"

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS

#include <sys/socket.h>

#include <string>

namespace sdb {
class EndpointUnixDomain final : public Endpoint {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// creates an endpoint
  //////////////////////////////////////////////////////////////////////////////

  EndpointUnixDomain(EndpointType, int, const std::string&);

  //////////////////////////////////////////////////////////////////////////////
  /// destroys an endpoint
  //////////////////////////////////////////////////////////////////////////////

  ~EndpointUnixDomain() final;

 public:
  //////////////////////////////////////////////////////////////////////////////
  /// connect the endpoint
  //////////////////////////////////////////////////////////////////////////////

  SocketWrapper connect(double, double) override;

  //////////////////////////////////////////////////////////////////////////////
  /// disconnect the endpoint
  //////////////////////////////////////////////////////////////////////////////

  void disconnect() override;

  int domain() const override { return AF_UNIX; }
  int port() const override { return 0; }
  std::string host() const override { return "localhost"; }
  std::string hostAndPort() const override { return "localhost"; }
  std::string path() { return _path; }

 private:
  const std::string _path;
};
}  // namespace sdb

#endif
