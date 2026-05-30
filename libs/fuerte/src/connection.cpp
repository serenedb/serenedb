////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Jan Christoph Uhde
/// @author Ewout Prangsma
////////////////////////////////////////////////////////////////////////////////

#include <fuerte/connection.h>

#include <yaclib/algo/wait_group.hpp>

#include "basics/logger/logger.h"

namespace sdb::fuerte {

Connection::~Connection() {
  SDB_DEBUG(GENERAL, "Destroying Connection");
}

// sendRequest and wait for it to finished.
std::unique_ptr<Response> Connection::sendRequest(
  std::unique_ptr<Request> request) {
  SDB_TRACE(GENERAL, "sendRequest (sync): before send");

  // TODO(mbkkt) just atomic, aka one shot event without multiple wait
  yaclib::OneShotEvent wg;
  std::unique_ptr<Response> rv;
  auto error = Error::NoError;

  auto cb = [&](Error e, std::unique_ptr<Request> request,
                std::unique_ptr<Response> response) {
    rv = std::move(response);
    error = e;
    wg.Set();
  };

  {
    // Start asynchronous request
    sendRequest(std::move(request), cb);

    // Wait for request to finish.
    SDB_TRACE(GENERAL, "sendRequest (sync): before wait");
    wg.Wait();
  }

  SDB_TRACE(GENERAL, "sendRequest (sync): done");

  if (error != Error::NoError) {
    throw error;
  }

  return rv;
}

std::string Connection::endpoint() const {
  std::string endpoint;
  endpoint.reserve(32);
  // http
  endpoint.append(fuerte::ToString(_config.protocol_type));
  endpoint.push_back('+');
  // tcp/ssl/unix
  endpoint.append(fuerte::ToString(_config.socket_type));
  endpoint.append("://");
  // domain name or IP (xxx.xxx.xxx.xxx)
  endpoint.append(_config.host);
  if (_config.socket_type != SocketType::Unix) {
    endpoint.push_back(':');
    endpoint.append(_config.port);
  }
  return endpoint;
}

}  // namespace sdb::fuerte
