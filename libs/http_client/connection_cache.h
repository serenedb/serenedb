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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "http_client/general_client_connection.h"

namespace sdb {
namespace app {

class AppServer;
class CommunicationFeaturePhase;

}  // namespace app
namespace httpclient {

class ConnectionCache;

struct ConnectionLease {
  ConnectionLease() noexcept = default;

  ConnectionLease(ConnectionCache* cache,
                  std::unique_ptr<GeneralClientConnection> connection) noexcept
    : cache{cache}, connection{std::move(connection)} {}

  ~ConnectionLease();

  ConnectionLease(const ConnectionLease&) = delete;
  ConnectionLease& operator=(const ConnectionLease&) = delete;

  ConnectionLease(ConnectionLease&& other) noexcept
    : cache{other.cache},
      connection{std::move(other.connection)},
      prevent_recycling{
        other.prevent_recycling.load(std::memory_order_relaxed)} {}

  ConnectionLease& operator=(ConnectionLease&& other) noexcept {
    if (this != &other) {
      cache = other.cache;
      connection = std::move(other.connection);
      prevent_recycling.store(
        other.prevent_recycling.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    }
    return *this;
  }

  void preventRecycling() noexcept {
    prevent_recycling.store(true, std::memory_order_relaxed);
  }

  ConnectionCache* cache = nullptr;
  std::unique_ptr<GeneralClientConnection> connection;
  std::atomic_bool prevent_recycling = false;
};

class ConnectionCache {
  ConnectionCache(const ConnectionCache&) = delete;
  ConnectionCache& operator=(const ConnectionCache&) = delete;

 public:
  struct Options {
    size_t max_connections_per_endpoint = 0;
    // TODO(mbkkt) timeout should be choosen more meaningfully
    uint32_t idle_connection_timeout_sec = 120;
  };

  ConnectionCache(app::CommunicationFeaturePhase& comm, const Options& options);

  ConnectionLease acquire(std::string endpoint, double connect_timeout,
                          double request_timeout, size_t connect_retries,
                          uint64_t ssl_protocol);

  /// the force flag also moves unconnected connections back into the
  /// cache. this is currently used only for testing
  void release(std::unique_ptr<GeneralClientConnection> connection,
               bool force = false);

#ifdef SDB_GTEST
  const auto& connections() const { return _connections; }
#endif

 private:
  struct ConnInfo {
    std::unique_ptr<GeneralClientConnection> connection;
    std::chrono::steady_clock::time_point last_used;
  };

  app::CommunicationFeaturePhase& _comm;

  const Options _options;

  mutable absl::Mutex _lock;

  containers::FlatHashMap<std::string, std::vector<ConnInfo>> _connections;

  uint64_t _connections_created = 0;
  uint64_t _connections_recycled = 0;
};

}  // namespace httpclient
}  // namespace sdb
