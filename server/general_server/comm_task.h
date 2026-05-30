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

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>

#include "auth/token_cache.h"
#include "basics/asio_ns.h"
#include "basics/buffer.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/result.h"
#include "endpoint/connection_info.h"
#include "general_server/state.h"

namespace sdb {

class GeneralRequest;
class GeneralResponse;

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

  // True for the single-node user-facing case; legacy cluster-internal
  // requests (coordinator <-> DB-server) would have set this to false to
  // skip verbose headers. Kept until the last reader (verbose-header
  // suppression) gets pruned out.
  bool _is_user_request{true};
};

}  // namespace rest
}  // namespace sdb
