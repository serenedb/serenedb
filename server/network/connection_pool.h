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

#include <fuerte/types.h>

#include "basics/common.h"
#include "metrics/fwd.h"

namespace sdb {
namespace fuerte {

class Connection;
class ConnectionBuilder;

}  // namespace fuerte
class ClusterInfo;

namespace network {

class ConnectionPtr;

/// simple connection pool managing fuerte connections
#ifdef SDB_GTEST
class ConnectionPool {
#else
class ConnectionPool final {
#endif
 protected:
  struct Connection;
  friend class ConnectionPtr;

 public:
  struct Config {
    metrics::MetricsFeature& metrics_feature;
    // note: clusterInfo can remain a nullptr in unit tests
    ClusterInfo* cluster_info = nullptr;
    uint64_t max_open_connections = 1024;     /// max number of connections
    uint64_t idle_connection_milli = 120000;  /// unused connection lifetime
    unsigned int num_io_threads = 1;          /// number of IO threads
    bool verify_hosts = false;
    fuerte::ProtocolType protocol = fuerte::ProtocolType::Http;
    // name must remain valid for the lifetime of the Config object.
    const char* name = "";

    Config(metrics::MetricsFeature& metrics_feature)
      : metrics_feature(metrics_feature) {}
  };

  ConnectionPool(const ConnectionPool& other) = delete;
  ConnectionPool& operator=(const ConnectionPool& other) = delete;

  explicit ConnectionPool(const ConnectionPool::Config& config);
  TEST_VIRTUAL ~ConnectionPool();

  /// request a connection for a specific endpoint
  /// note: it is the callers responsibility to ensure the endpoint
  /// is always the same, we do not do any post-processing
  ConnectionPtr leaseConnection(const std::string& endpoint,
                                bool& is_from_pool);

  /// @brief stops the connection pool (also calls drainConnections)
  void stop();

  /// shutdown all connections
  void drainConnections();

  /// shutdown all connections
  void shutdownConnections();

  /// automatically prune connections
  void pruneConnections();

  /// cancel connections to this endpoint
  size_t cancelConnections(const std::string& endpoint);

  /// return the number of open connections
  size_t numOpenConnections() const;

  const Config& config() const;

 protected:
  struct Context;

  /// endpoint bucket
  struct Bucket;

  TEST_VIRTUAL std::shared_ptr<fuerte::Connection> createConnection(
    fuerte::ConnectionBuilder&);

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

class ConnectionPtr {
 public:
  ConnectionPtr(std::shared_ptr<ConnectionPool::Context> context);
  ConnectionPtr(ConnectionPtr&& ctx) noexcept;
  ConnectionPtr(const ConnectionPtr&) = delete;
  ~ConnectionPtr();

  ConnectionPtr operator=(ConnectionPtr&&) = delete;
  ConnectionPtr operator=(const ConnectionPtr&) = delete;

  fuerte::Connection& operator*() const;
  fuerte::Connection* operator->() const;
  fuerte::Connection* get() const;

 private:
  std::shared_ptr<ConnectionPool::Context> _context;
};

}  // namespace network
}  // namespace sdb
