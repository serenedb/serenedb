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

#include <basics/buffer.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>

#include "auth/token_cache.h"
#include "basics/asio_ns.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "endpoint/connection_info.h"
#include "general_server/state.h"
#include "statistics/connection_statistics.h"
#include "statistics/request_statistics.h"

namespace sdb {
class AuthenticationFeature;
class ConnectionStatistics;
class GeneralRequest;
class GeneralResponse;
class RequestStatistics;

namespace rest {
class RestHandler;
class GeneralServer;

class CommTask : public std::enable_shared_from_this<CommTask> {
  CommTask(const CommTask&) = delete;
  const CommTask& operator=(const CommTask&) = delete;

 public:
  CommTask(GeneralServer& server, ConnectionInfo info);

  virtual ~CommTask() = default;

  // callable from any thread
  virtual void Start() = 0;
  virtual void Stop() = 0;
  // callable from asio context thread
  virtual void Close(asio_ns::error_code err = {}) = 0;

  const ConnectionInfo& GetConnectionInfo() const noexcept {
    return _connection_info;
  }

 protected:
  /// set / reset connection timeout
  virtual void SetIOTimeout() = 0;

  virtual bool Stopped() const noexcept = 0;

  GeneralServer& _server;
  ConnectionInfo _connection_info;
  std::chrono::milliseconds _keep_alive_timeout;

  // contains value of "x-serene-source"
  std::string _request_source;
  // whether or not the request was originated by a user (true)
  // or if it is a cluster-internal request (false).
  // we use this flag to save some reduce verbosity of responses
  // (verbose user-facing HTTP response headers) in
  // cluster-internal-only traffic.
  bool _is_user_request{true};
};
}  // namespace rest
}  // namespace sdb
