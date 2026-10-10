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
#include <chrono>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/coro/task.hpp>

#include "replication/pg_replication_client.h"
#include "server/utils/asio_ns.h"

namespace sdb {
namespace network {

class IoExecutor;
class IoThreadPool;

}  // namespace network
namespace replication {

class SubscriptionEngine final {
 public:
  inline static SubscriptionEngine* gInstance = nullptr;

  explicit SubscriptionEngine(network::IoThreadPool& pool);
  ~SubscriptionEngine();

  void start();
  void RequestStop() noexcept;
  void stop();

  void Sync(std::string_view database, duckdb::idx_t subscription,
            bool restart = false);
  void Stop(duckdb::idx_t subscription);

  struct SubRuntime {
    uint64_t received_lsn = 0;
    uint64_t flushed_lsn = 0;
    int64_t last_send_time = 0;
    int64_t last_receipt_time = 0;
    int64_t latest_end_time = 0;
  };
  irs::containers::FlatHashMap<duckdb::idx_t, SubRuntime> RuntimeSnapshot(
    std::string_view database) const;

  struct SubStats {
    uint64_t apply_error_count = 0;
    uint64_t sync_error_count = 0;
    uint64_t insert_exists = 0;
    uint64_t update_exists = 0;
    uint64_t update_missing = 0;
    uint64_t delete_missing = 0;
    uint64_t multiple_unique_conflicts = 0;
    int64_t stats_reset = 0;

    void Add(const ConflictCounters& conflicts) noexcept {
      insert_exists += conflicts.insert_exists.load(std::memory_order_relaxed);
      update_exists += conflicts.update_exists.load(std::memory_order_relaxed);
      update_missing +=
        conflicts.update_missing.load(std::memory_order_relaxed);
      delete_missing +=
        conflicts.delete_missing.load(std::memory_order_relaxed);
      multiple_unique_conflicts +=
        conflicts.multiple_unique_conflicts.load(std::memory_order_relaxed);
    }
  };
  irs::containers::FlatHashMap<duckdb::idx_t, SubStats> Stats() const;
  void ResetStats(std::optional<duckdb::idx_t> subscription);
  bool Running(duckdb::idx_t subscription) const;

 private:
  struct SubState {
    std::string database;
    std::optional<ReplicationTarget> target;
    duckdb::shared_ptr<PgReplicationClient> client;
    std::shared_ptr<asio_ns::steady_timer> retry;
    bool stopping = false;
    bool restart = false;
    size_t host = 0;
    size_t encryption = 0;
    uint32_t transient_failures = 0;
    uint64_t host_seed = 0;
    bool any_session = false;
  };

  void LaunchLocked(std::string_view database, duckdb::idx_t subscription)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mu);
  void StopLocked(SubState& state) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mu);
  SubStats& StatsLocked(duckdb::idx_t subscription)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mu);
  void RestartLocked(SubState& state) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mu);
  yaclib::Task<bool> Backoff(duckdb::idx_t subscription,
                             network::IoExecutor& exec,
                             std::chrono::milliseconds delay);
  yaclib::Task<> Supervise(duckdb::idx_t subscription,
                           network::IoExecutor& exec);
  void Disable(std::string_view database, duckdb::idx_t subscription);

  network::IoThreadPool& _pool;
  std::atomic<bool> _stopping{false};
  mutable absl::Mutex _mu;
  irs::containers::NodeHashMap<duckdb::idx_t, SubState> _subs
    ABSL_GUARDED_BY(_mu);
  irs::containers::FlatHashMap<duckdb::idx_t, SubStats> _stats
    ABSL_GUARDED_BY(_mu);
};

}  // namespace replication
}  // namespace sdb
