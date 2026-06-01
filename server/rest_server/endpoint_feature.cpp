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

#include "endpoint_feature.h"

#include <absl/flags/flag.h>
#include <sys/socket.h>

#include "basics/application-exit.h"
#include "basics/log.h"

ABSL_FLAG(std::vector<std::string>, server_endpoint, {},
          "Endpoint for client requests (e.g. `pgsql+tcp://127.0.0.1:7890`). "
          "Repeat for multiple. Supported schemes: pgsql+tcp, tcp, ssl, "
          "unix.");

namespace sdb {

EndpointList& Endpoints() {
  static EndpointList list = [] {
    EndpointList l;
    auto endpoints = absl::GetFlag(FLAGS_server_endpoint);
    constexpr uint64_t kDefaultBacklog = 64;
    const uint64_t backlog_size =
      kDefaultBacklog <= SOMAXCONN ? kDefaultBacklog : SOMAXCONN / 2;
    constexpr bool kReuseAddress = true;
    if (endpoints.empty()) {
      endpoints.emplace_back("pgsql+tcp://127.0.0.1:7890");
      SDB_INFO(GENERAL, "no endpoints have been specified, using default: ",
               endpoints.back());
    }
    for (const auto& ep : endpoints) {
      if (!l.add(ep, static_cast<int>(backlog_size), kReuseAddress)) {
        SDB_FATAL(GENERAL, "invalid endpoint '", ep, "'");
      }
    }
    return l;
  }();
  return list;
}

}  // namespace sdb
