////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/algo/wait_group.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/task.hpp>

#include "network/cancel_registry.h"
#include "network/http/handler.h"
#include "network/http/quack/connection.h"
#include "quack_message.hpp"

namespace sdb::network::http::quack {

struct Request;

// One client session, as quack_active_connections() lists it.
struct ConnectionSnapshot {
  std::string server_id;
  std::string connection_id;
  std::string client_id_hash;
  std::string sql;
  QueryState state = QueryState::Idle;
  duckdb::timestamp_t started_at{0};
  std::optional<size_t> cached_rows;
};

// The Quack protocol on serened's HTTP listeners: the clients' sessions, their
// leases and the statements they run.
class Service final {
 public:
  Service(CancelRegistry& cancel, yaclib::WaitGroup<>& tasks,
          std::string server_id);
  ~Service();

  // The service of this server, null while no listener serves Quack.
  static Service* Instance() noexcept { return gInstance; }

  void RequestStop();

  yaclib::Task<> Handle(RequestContext& context, const HttpRequest& request,
                        HttpResponseWriter& writer);

  // Ends the session, as a DISCONNECT does. False when there is none.
  bool Disconnect(std::string_view connection_id);

  std::vector<ConnectionSnapshot> Connections();

  CancelRegistry& Cancels() noexcept { return _cancel; }

  yaclib::WaitGroup<>& Tasks() noexcept { return _tasks; }

  bool Stopping() const noexcept {
    return _stopping.load(std::memory_order_acquire);
  }

 private:
  struct ExpiryEntry {
    duckdb::timestamp_t served_at;
    std::string connection_id;

    bool operator>(const ExpiryEntry& other) const {
      return served_at > other.served_at;
    }
  };

  yaclib::Future<> SweepLoop();
  void Sweep();
  void SweepCaches();

  // The response, or null while the request waits on its statement.
  duckdb::unique_ptr<duckdb::QuackMessage> Answer(Request& request,
                                                  RequestContext& context);
  duckdb::unique_ptr<duckdb::QuackMessage> Decode(Request& request);
  duckdb::unique_ptr<duckdb::QuackMessage> Connect(Request& request,
                                                   RequestContext& context);
  duckdb::unique_ptr<duckdb::QuackMessage> Prepare(Request& request);
  duckdb::unique_ptr<duckdb::QuackMessage> Fetch(Request& request);
  duckdb::unique_ptr<duckdb::QuackMessage> SendData(Request& request);
  duckdb::unique_ptr<duckdb::QuackMessage> Cancel(Request& request);
  duckdb::unique_ptr<duckdb::QuackMessage> Acknowledge(Request& request);
  void Finish(Request& request,
              duckdb::unique_ptr<duckdb::QuackMessage> response);

  std::shared_ptr<Connection> Find(std::string_view connection_id);
  bool RenewLease(const std::shared_ptr<Connection>& connection);
  bool ReserveSession(uint64_t max_sessions);
  void CacheResult(Request& request);
  void SyncCache(Connection& connection, duckdb::QuackResultStream& stream);
  void QueueForExpiry(Connection& connection)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(connection.mu);

  inline static Service* gInstance = nullptr;

  CancelRegistry& _cancel;
  yaclib::WaitGroup<>& _tasks;
  // The sweep reads the service, so the service outlives it
  yaclib::WaitGroup<> _sweep{1};
  duckdb::DatabaseInstance& _db;
  const std::string _server_id;
  // Keys the HMAC of a client's CLIENT_ID in quack_active_connections()
  const std::string _client_id_key;
  std::atomic<bool> _stopping{false};
  std::atomic<size_t> _live_caches{0};

  absl::Mutex _mu;
  irs::containers::FlatHashMap<std::string, std::shared_ptr<Connection>>
    _connections ABSL_GUARDED_BY(_mu);
  // CONNECTs between their sdb_quack_max_sessions check and their registration
  size_t _opening ABSL_GUARDED_BY(_mu) = 0;

  // The connections with a result cache, oldest served first
  absl::Mutex _expiry_mu;
  std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>,
                      std::greater<ExpiryEntry>>
    _expiry ABSL_GUARDED_BY(_expiry_mu);
};

}  // namespace sdb::network::http::quack
