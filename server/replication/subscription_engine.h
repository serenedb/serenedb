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

#include <atomic>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <memory>
#include <optional>
#include <yaclib/coro/task.hpp>

#include "absl/synchronization/mutex.h"
#include "basics/asio_ns.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "replication/pg_replication_client.h"

namespace sdb {
namespace catalog {

class Subscription;
}
namespace network {

class IoThreadPool;
}

namespace replication {

class PgReplicationClient;
struct ReplicationTarget;

class SubscriptionEngine;
SubscriptionEngine& GetSubscriptionEngine();

// Owns the subscriber apply clients: one PgReplicationClient coroutine per
// enabled subscription, each spawned on an io-pool worker (round-robin, like
// the acceptor spawns inbound sessions) -- no dedicated threads. start()
// launches clients for every enabled subscription already in the catalog;
// CREATE/DROP/ ALTER drive StartSubscription/StopSubscription at runtime.
class SubscriptionEngine final {
 public:
  inline static SubscriptionEngine* gInstance = nullptr;

  explicit SubscriptionEngine(network::IoThreadPool& pool);
  ~SubscriptionEngine();

  void start();
  // Signal every client to unwind (idempotent); the io-pool teardown then
  // drains their coroutines.
  void RequestStop() noexcept;
  void stop();

  void StartSubscription(ObjectId database_id,
                         const std::shared_ptr<catalog::Subscription>& sub);
  void StopSubscription(ObjectId subscription_id);
  // Reconnect a running subscription with a refreshed target (e.g. after ALTER
  // SUBSCRIPTION ... SET (binary = ...)): drop the current client and let the
  // supervisor immediately re-launch with the new config. No-op if the sub is
  // not currently running (a later enable picks up the new config).
  void RestartSubscription(ObjectId database_id,
                           const std::shared_ptr<catalog::Subscription>& sub);
  // Spawn a one-shot client that connects to the publisher and drops its
  // logical slot (best-effort; logs on failure). Call after StopSubscription so
  // the streaming connection has released the slot.
  void DropRemoteSlot(ReplicationTarget target);

  // A live apply worker's runtime state, for the observability catalogs
  // (pg_stat_subscription / pg_subscription_rel). Only subscriptions that
  // currently have a connected apply client are reported.
  struct SubRuntime {
    ObjectId subscription_id;
    uint64_t received_lsn = 0;
    uint64_t flushed_lsn = 0;
    std::vector<ObjectId> tables;  // replicated local table OIDs
  };
  // Snapshot the running apply workers whose subscription lives in
  // `database_id`. Safe to call from any thread.
  std::vector<SubRuntime> RuntimeSnapshot(ObjectId database_id) const;

 private:
  void LaunchLocked(ObjectId database_id,
                    const std::shared_ptr<catalog::Subscription>& sub)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mu);
  // Build the apply target from a subscription's catalog config; nullopt if the
  // local database is gone.
  std::optional<ReplicationTarget> MakeTarget(
    ObjectId database_id,
    const std::shared_ptr<catalog::Subscription>& sub) const;
  // Per-subscription supervisor loop: run one fresh client to completion, and
  // (unless stopped) reconnect -- like PG's apply worker retrying on
  // wal_retrieve_retry_interval. Each attempt is a brand-new client because
  // Transport::Stop is a one-way latch. The target is re-read from SubState
  // each attempt, so a RestartSubscription mid-flight reconnects with the new
  // config.
  yaclib::Task<> Supervise(ObjectId sub_id);

  // One supervised subscription. PgWireSession uses enable_shared_from_this, so
  // the client is a duckdb::shared_ptr (as the acceptor makes sessions).
  struct SubState {
    ReplicationTarget target;  // config for the next connect attempt
    duckdb::shared_ptr<PgReplicationClient> client;  // current attempt, if any
    std::shared_ptr<asio_ns::steady_timer> retry;    // pending reconnect delay
    bool stopping = false;  // DROP/DISABLE/shutdown requested for this sub
    bool restart = false;   // reconnect immediately with the refreshed target
  };

  network::IoThreadPool& _pool;
  std::atomic<bool> _stopping{false};
  mutable absl::Mutex _mu;
  containers::NodeHashMap<ObjectId, SubState> _subs ABSL_GUARDED_BY(_mu);
};

}  // namespace replication
}  // namespace sdb
