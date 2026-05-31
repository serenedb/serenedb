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

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/logger/logger.h"
#include "general_server/scheduler_feature.h"

ABSL_FLAG(std::vector<std::string>, server_endpoint, {},
          "Endpoint for client requests (e.g. `pgsql+tcp://127.0.0.1:7890`). "
          "Repeat for multiple. Supported schemes: pgsql+tcp, tcp, ssl, "
          "unix.");

using namespace sdb::basics;

namespace sdb {

EndpointFeature::EndpointFeature()
  : _endpoints(absl::GetFlag(FLAGS_server_endpoint)) {
  // if our default value is too high, we'll use half of the max value provided
  // by the system
  if (_backlog_size > SOMAXCONN) {
    _backlog_size = SOMAXCONN / 2;
  }
  if (_backlog_size > SOMAXCONN) {
    SDB_WARN(GENERAL,
             "value for --tcp.backlog-size exceeds default system "
             "header SOMAXCONN value ",
             SOMAXCONN, ". trying to use ", SOMAXCONN, " anyway");
  }
  if (_endpoints.empty()) {
    _endpoints.emplace_back("pgsql+tcp://127.0.0.1:7890");
    SDB_INFO(GENERAL, "no endpoints have been specified, using default: ",
             _endpoints.back());
  }
  buildEndpointLists();
  gInstance = this;
}

EndpointFeature::~EndpointFeature() { gInstance = nullptr; }

void EndpointFeature::buildEndpointLists() {
  for (const auto& it : _endpoints) {
    bool ok =
      _endpoint_list.add(it, static_cast<int>(_backlog_size), _reuse_address);

    if (!ok) {
      SDB_FATAL(GENERAL, "invalid endpoint '", it, "'");
    }
  }
}

}  // namespace sdb
