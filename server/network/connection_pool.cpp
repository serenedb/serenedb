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

#include "connection_pool.h"

#include <fuerte/connection.h>
#include <fuerte/loop.h>
#include <fuerte/types.h>

#include <memory>
#include <vector>

#include "auth/token_cache.h"
#include "basics/containers/small_vector.h"
#include "basics/logger/logger.h"
#include "basics/write_locker.h"
#include "general_server/authentication_feature.h"
#include "metrics/counter_builder.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/log_scale.h"
#include "metrics/metrics_feature.h"
#include "network/network_feature.h"

DECLARE_GAUGE(serenedb_connection_pool_connections_current, uint64_t,
              "Current number of connections in pool");
DECLARE_COUNTER(serenedb_connection_pool_leases_successful_total,
                "Total number of successful connection leases");
DECLARE_COUNTER(serenedb_connection_pool_leases_failed_total,
                "Total number of failed connection leases");
DECLARE_COUNTER(serenedb_connection_pool_connections_created_total,
                "Total number of connections created");

struct LeaseTimeScale {
  static sdb::metrics::LogScale<float> scale() {
    return {2.f, 0.f, 1000.f, 10};
  }
};
DECLARE_HISTOGRAM(serenedb_connection_pool_lease_time_hist, LeaseTimeScale,
                  "Time to lease a connection from pool [ms]");

namespace sdb {
namespace network {

struct ConnectionPool::Context {
  Context(std::shared_ptr<fuerte::Connection>,
          std::chrono::steady_clock::time_point, size_t);

  std::shared_ptr<fuerte::Connection> fuerte;
  std::chrono::steady_clock::time_point last_leased;  /// last time leased
  std::atomic<size_t> leases;  // number of active users, including those
                               // who may not have sent a request yet
};

struct ConnectionPool::Bucket {
  mutable absl::Mutex mutex;
  containers::SmallVector<std::shared_ptr<Context>, 4> list;
};

struct ConnectionPool::Impl {
  Impl(const Impl& other) = delete;
  Impl& operator=(const Impl& other) = delete;

  explicit Impl(const ConnectionPool::Config& config, ConnectionPool& pool)
    : config(config),
      pool(pool),
      loop(config.num_io_threads, config.name),
      stopped(false),
      total_connections_in_pool(config.metrics_feature.add(
        serenedb_connection_pool_connections_current{}.withLabel("pool",
                                                                 config.name))),
      success_select(config.metrics_feature.add(
        serenedb_connection_pool_leases_successful_total{}.withLabel(
          "pool", config.name))),
      no_success_select(config.metrics_feature.add(
        serenedb_connection_pool_leases_failed_total{}.withLabel("pool",
                                                                 config.name))),
      connections_created(config.metrics_feature.add(
        serenedb_connection_pool_connections_created_total{}.withLabel(
          "pool", config.name))),
      lease_hist_m_sec(config.metrics_feature.add(
        serenedb_connection_pool_lease_time_hist{}.withLabel("pool",
                                                             config.name))) {
    SDB_ASSERT(config.num_io_threads > 0);
  }

  /// request a connection for a specific endpoint
  /// note: it is the callers responsibility to ensure the endpoint
  /// is always the same, we do not do any post-processing
  network::ConnectionPtr LeaseConnection(const std::string& endpoint,
                                         bool& is_from_pool) {
    std::shared_lock read_guard{lock};
    if (stopped) {
      SDB_THROW(ERROR_SHUTTING_DOWN, "connection pool already stopped");
    }

    auto it = connections.find(endpoint);
    if (it == connections.end()) {
      read_guard.unlock();

      auto tmp = std::make_unique<Bucket>();  // get memory outside lock
      absl::WriterMutexLock write_guard{&lock};
      auto [it2, emplaced] = connections.try_emplace(endpoint, std::move(tmp));
      return SelectConnection(endpoint, *it2->second, is_from_pool);
    }
    return SelectConnection(endpoint, *it->second, is_from_pool);
  }

  /// @brief stops the connection pool
  void Stop() {
    {
      absl::WriterMutexLock guard{&lock};
      stopped = true;
    }
    DrainConnections();
    loop.stop();
  }

  /// @brief drain all connections
  void DrainConnections() {
    absl::WriterMutexLock guard{&lock};
    size_t n = 0;
    for (auto& pair : connections) {
      Bucket& buck = *(pair.second);
      std::lock_guard lock(buck.mutex);
      n += buck.list.size();
      buck.list.clear();
    }
    // We drop everything.
    SDB_ASSERT(total_connections_in_pool.load() == n);
    total_connections_in_pool -= n;
    connections.clear();
  }

  /// shutdown all connections
  void ShutdownConnections() {
    absl::WriterMutexLock guard{&lock};
    for (auto& pair : connections) {
      Bucket& buck = *(pair.second);
      std::lock_guard lock(buck.mutex);
      for (std::shared_ptr<Context>& c : buck.list) {
        c->fuerte->cancel();
      }
    }
  }

  /// remove unused and broken connections
  void PruneConnections() {
    const std::chrono::milliseconds ttl(config.idle_connection_milli);

    absl::ReaderMutexLock guard{&lock};
    for (auto& pair : connections) {
      Bucket& buck = *(pair.second);
      std::lock_guard lock(buck.mutex);

      // get under lock
      auto now = std::chrono::steady_clock::now();

      // this loop removes broken connections, and closes the ones we don't
      // need anymore
      size_t alive_count = 0;

      // make a single pass over the connections in this bucket
      auto it = buck.list.begin();
      while (it != buck.list.end()) {
        bool remove = false;

        if ((*it)->fuerte->state() == fuerte::Connection::State::Closed) {
          // lets not keep around disconnected fuerte connection objects
          remove = true;
        } else if ((*it)->leases.load() == 0 &&
                   (*it)->fuerte->requestsLeft() == 0) {
          if ((now - (*it)->last_leased) > ttl ||
              alive_count >= config.max_open_connections) {
            // connection hasn't been used for a while, or there are too many
            // connections
            remove = true;
          }  // else keep the connection
        }

        if (remove) {
          it = buck.list.erase(it);
          total_connections_in_pool -= 1;
        } else {
          ++alive_count;
          ++it;

          if (alive_count == config.max_open_connections &&
              it != buck.list.end()) {
            SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
                      "pruning extra connections to '", pair.first, "' (",
                      buck.list.size(), ")");
          }
        }
      }
    }
  }

  /// cancel connections to this endpoint
  size_t CancelConnections(const std::string& endpoint) {
    absl::WriterMutexLock guard{&lock};
    const auto& it = connections.find(endpoint);
    if (it != connections.end()) {
      size_t n;
      {
        Bucket& buck = *(it->second);
        std::lock_guard lock(buck.mutex);
        n = buck.list.size();
        for (std::shared_ptr<Context>& c : buck.list) {
          c->fuerte->cancel();
        }
      }
      connections.erase(it);
      // We just erased `n` connections on the bucket.
      // Let's count it.
      SDB_ASSERT(total_connections_in_pool.load() >= n);
      total_connections_in_pool -= n;
      return n;
    }
    return 0;
  }

  /// return the number of open connections
  size_t NumOpenConnections() const {
    size_t conns = 0;

    absl::ReaderMutexLock guard{&lock};
    for (auto& pair : connections) {
      Bucket& buck = *(pair.second);
      std::lock_guard lock(buck.mutex);
      conns += buck.list.size();
    }
    return conns;
  }

  ConnectionPtr SelectConnection(const std::string& endpoint,
                                 ConnectionPool::Bucket& bucket,
                                 bool& is_from_pool) {
    using namespace std::chrono;
    const milliseconds ttl(config.idle_connection_milli);

    auto start = steady_clock::now();
    is_from_pool = true;  // Will revert for new connections

    // exclusively lock the bucket
    std::unique_lock guard(bucket.mutex);

    for (std::shared_ptr<Context>& c : bucket.list) {
      if (c->fuerte->state() == fuerte::Connection::State::Closed ||
          (start - c->last_leased) > ttl) {
        continue;
      }

      SDB_ASSERT(config.protocol != fuerte::ProtocolType::Undefined);

      size_t limit = 0;
      switch (config.protocol) {
        case fuerte::ProtocolType::Http2:
          limit = 4;
          break;
        default:
          break;  // keep default of 0
      }

      // first check against number of active users
      size_t num = c->leases.load(std::memory_order_relaxed);
      while (num <= limit) {
        const bool leased = c->leases.compare_exchange_strong(
          num, num + 1, std::memory_order_relaxed);
        if (leased) {
          // next check against the number of requests in flight
          if (c->fuerte->requestsLeft() <= limit &&
              c->fuerte->state() != fuerte::Connection::State::Closed) {
            c->last_leased = std::chrono::steady_clock::now();
            ++success_select;
            lease_hist_m_sec.count(
              duration<float, std::micro>(c->last_leased - start).count());
            return {c};
          } else {  // too many requests,
            c->leases.fetch_sub(1, std::memory_order_relaxed);
            ++no_success_select;
            break;
          }
        }
      }
    }

    ++connections_created;
    // no free connection found, so we add one
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "creating connection to ",
              endpoint, " bucket size  ", bucket.list.size());

    fuerte::ConnectionBuilder builder;
    builder.endpoint(endpoint);  // picks the socket type

    auto now = steady_clock::now();
    auto c = std::make_shared<Context>(pool.createConnection(builder), now,
                                       1 /* leases*/);
    bucket.list.push_back(c);

    guard.unlock();
    // continue without the bucket lock

    is_from_pool = false;
    total_connections_in_pool += 1;
    lease_hist_m_sec.count(duration<float, std::micro>(now - start).count());
    return {std::move(c)};
  }

  std::shared_ptr<fuerte::Connection> CreateConnection(
    fuerte::ConnectionBuilder& builder) {
    builder.useIdleTimeout(false);
    builder.verifyHost(config.verify_hosts);
    builder.protocolType(config.protocol);  // always overwrite protocol
    SDB_ASSERT(builder.socketType() != fuerte::SocketType::Undefined);

    auto* af = AuthenticationFeature::instance();
    if (af && af->isActive()) {
      const auto& token = af->tokenCache().jwtToken();
      builder.jwtToken(token);
      builder.authenticationType(fuerte::AuthenticationType::Jwt);
    }
    return builder.connect(loop);
  }

  const Config config;
  ConnectionPool& pool;

  mutable absl::Mutex lock;
  /// @brief map from endpoint to a bucket with connections to the endpoint.
  /// protected by _lock.
  containers::FlatHashMap<std::string, std::unique_ptr<Bucket>> connections;

  /// contains fuerte asio::io_context
  fuerte::EventLoopService loop;

  /// @brief whether or not the connection pool was already stopped. if set
  /// to true, calling leaseConnection will throw an exception. protected
  /// by _lock.
  bool stopped;

  metrics::Gauge<uint64_t>& total_connections_in_pool;
  metrics::Counter& success_select;
  metrics::Counter& no_success_select;
  metrics::Counter& connections_created;

  metrics::Histogram<metrics::LogScale<float>>& lease_hist_m_sec;
};

ConnectionPool::ConnectionPool(const ConnectionPool::Config& config)
  : _impl(std::make_unique<Impl>(config, *this)) {}

ConnectionPool::~ConnectionPool() {
  shutdownConnections();
  stop();
}

/// request a connection for a specific endpoint
/// note: it is the callers responsibility to ensure the endpoint
/// is always the same, we do not do any post-processing
network::ConnectionPtr ConnectionPool::leaseConnection(
  const std::string& endpoint, bool& is_from_pool) {
  return _impl->LeaseConnection(endpoint, is_from_pool);
}

/// @brief stops the connection pool (also calls drainConnections)
void ConnectionPool::stop() { _impl->Stop(); }

/// @brief drain all connections
void ConnectionPool::drainConnections() { _impl->DrainConnections(); }

/// shutdown all connections
void ConnectionPool::shutdownConnections() { _impl->ShutdownConnections(); }

/// remove unused and broken connections
void ConnectionPool::pruneConnections() { _impl->PruneConnections(); }

/// cancel connections to this endpoint
size_t ConnectionPool::cancelConnections(const std::string& endpoint) {
  return _impl->CancelConnections(endpoint);
}

/// return the number of open connections
size_t ConnectionPool::numOpenConnections() const {
  return _impl->NumOpenConnections();
}

std::shared_ptr<fuerte::Connection> ConnectionPool::createConnection(
  fuerte::ConnectionBuilder& builder) {
  return _impl->CreateConnection(builder);
}

const ConnectionPool::Config& ConnectionPool::config() const {
  return _impl->config;
}

ConnectionPool::Context::Context(std::shared_ptr<fuerte::Connection> c,
                                 std::chrono::steady_clock::time_point t,
                                 size_t l)
  : fuerte(std::move(c)), last_leased(t), leases(l) {}

ConnectionPtr::ConnectionPtr(std::shared_ptr<ConnectionPool::Context> ctx)
  : _context{std::move(ctx)} {}

ConnectionPtr::ConnectionPtr(ConnectionPtr&& other) noexcept
  : _context(std::move(other._context)) {}

ConnectionPtr::~ConnectionPtr() {
  if (_context) {
    _context->leases.fetch_sub(1, std::memory_order_relaxed);
  }
}

fuerte::Connection& ConnectionPtr::operator*() const {
  return *(_context->fuerte);
}

fuerte::Connection* ConnectionPtr::operator->() const {
  return _context->fuerte.get();
}

fuerte::Connection* ConnectionPtr::get() const {
  return _context->fuerte.get();
}

}  // namespace network
}  // namespace sdb
