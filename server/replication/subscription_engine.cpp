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

#include "replication/subscription_engine.h"

#include <chrono>
#include <utility>
#include <yaclib/async/contract.hpp>

#include "basics/asio_ns.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/subscription.h"
#include "network/io_context.h"
#include "replication/pg_replication_client.h"

namespace sdb::replication {
namespace {

// Reconnect delay, mirroring PostgreSQL's wal_retrieve_retry_interval default.
constexpr int kReconnectSeconds = 5;

}  // namespace

SubscriptionEngine& GetSubscriptionEngine() {
  return *SubscriptionEngine::gInstance;
}

SubscriptionEngine::SubscriptionEngine(network::IoThreadPool& pool)
  : _pool(pool) {
  gInstance = this;
}

SubscriptionEngine::~SubscriptionEngine() {
  stop();
  if (gInstance == this) {
    gInstance = nullptr;
  }
}

void SubscriptionEngine::start() {
  absl::MutexLock lock{&_mu};
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  for (const auto& db : snapshot->GetDatabases()) {
    for (const auto& sub : snapshot->GetSubscriptions(db->GetId())) {
      LaunchLocked(db->GetId(), sub);
    }
  }
  SDB_INFO(REPLICATION, "subscription engine started (", _subs.size(),
           " enabled subscriber(s))");
}

void SubscriptionEngine::RequestStop() noexcept {
  _stopping.store(true, std::memory_order_release);
  absl::MutexLock lock{&_mu};
  for (auto& [_, sub] : _subs) {
    sub.stopping = true;
    if (sub.client) {
      sub.client->StopClient();
    }
    if (sub.retry) {
      auto timer = sub.retry;
      asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
    }
  }
}

void SubscriptionEngine::stop() {
  // Signal every supervisor to stop; each unwinds (and erases its _subs entry)
  // as the io pool drains at network teardown.
  RequestStop();
}

std::optional<ReplicationTarget> SubscriptionEngine::MakeTarget(
  ObjectId database_id,
  const std::shared_ptr<catalog::Subscription>& sub) const {
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(database_id);
  if (!database) {
    return std::nullopt;
  }
  const auto& cfg = sub->GetConfig();
  ReplicationTarget target = ParseConnInfo(cfg.conninfo);
  target.subscription_id = sub->GetId();
  target.database_id = database_id;
  target.database_name = database->GetName();
  target.subscription_name = sub->GetName();
  target.publications = cfg.publications;
  target.slot_name = cfg.slot_name;
  target.binary = cfg.binary;
  target.copy_data = cfg.copy_data;
  target.disable_on_error = cfg.disable_on_error;
  target.origin = cfg.origin_name;
  target.create_slot = cfg.create_slot;
  // Resolve the subscription owner so apply runs with that role's privileges.
  target.owner_id = sub->GetOwner();
  for (const auto& role : snapshot->GetRoles()) {
    if (role->GetId() == target.owner_id) {
      target.owner_name = role->GetName();
      break;
    }
  }
  return target;
}

std::vector<SubscriptionEngine::SubRuntime> SubscriptionEngine::RuntimeSnapshot(
  ObjectId database_id) const {
  std::vector<SubRuntime> out;
  absl::MutexLock lock{&_mu};
  for (const auto& [sub_id, st] : _subs) {
    if (!st.client || st.target.database_id != database_id) {
      continue;
    }
    SubRuntime r;
    r.subscription_id = sub_id;
    r.received_lsn = st.client->ReceivedLsn();
    r.flushed_lsn = st.client->FlushedLsn();
    r.tables = st.client->ReplicatedTables();
    out.push_back(std::move(r));
  }
  return out;
}

void SubscriptionEngine::LaunchLocked(
  ObjectId database_id, const std::shared_ptr<catalog::Subscription>& sub) {
  if (_stopping.load(std::memory_order_acquire)) {
    return;
  }
  const auto sub_id = sub->GetId();
  if (_subs.contains(sub_id)) {
    return;
  }
  if (!sub->GetConfig().enabled) {
    return;
  }
  auto target = MakeTarget(database_id, sub);
  if (!target) {
    return;
  }

  auto [it, _] = _subs.try_emplace(sub_id);
  it->second.target = std::move(*target);
  auto& exec = _pool.Next();
  asio_ns::post(exec.Context(),
                [this, sub_id]() mutable { Supervise(sub_id).Detach(); });
}

yaclib::Task<> SubscriptionEngine::Supervise(ObjectId sub_id) {
  auto& exec = _pool.Next();
  std::string sub_name;
  for (;;) {
    if (_stopping.load(std::memory_order_acquire)) {
      break;
    }
    ReplicationTarget target;
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      target = it->second.target;  // re-read config (may have changed on ALTER)
    }
    sub_name = target.subscription_name;
    auto client = duckdb::make_shared_ptr<PgReplicationClient>(exec, target);
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      it->second.client = client;
    }
    co_await client->RunClient();
    // WITH (disable_on_error): the apply hit an error and asked to be disabled
    // rather than retried -- persist enabled=false and stop supervising.
    if (client->DisableRequested()) {
      SDB_WARN(REPLICATION, "subscription '", sub_name,
               "' disabled after apply error (disable_on_error)");
      const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
      for (const auto& sub : snapshot->GetSubscriptions(target.database_id)) {
        if (sub->GetId() == sub_id) {
          try {
            catalog::GetCatalog().SetSubscriptionEnabled(
              catalog::AccessContext{sub->GetOwner()}, target.database_id,
              sub->GetName(), false);
          } catch (const std::exception& ex) {
            SDB_WARN(REPLICATION, "subscription '", sub_name,
                     "' disable failed: ", ex.what());
          }
          break;
        }
      }
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it != _subs.end()) {
        it->second.client.reset();
      }
      break;
    }
    bool restart = false;
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it != _subs.end()) {
        it->second.client.reset();
      }
      if (it == _subs.end() || it->second.stopping ||
          _stopping.load(std::memory_order_acquire)) {
        break;
      }
      restart = it->second.restart;
      it->second.restart = false;
    }
    if (restart) {
      SDB_INFO(REPLICATION, "subscription '", sub_name,
               "' restarting with updated configuration");
      continue;  // reconnect immediately with the refreshed target
    }

    SDB_INFO(REPLICATION, "subscription '", sub_name,
             "' disconnected; reconnecting in ", kReconnectSeconds, "s");
    // Interruptible delay: StopSubscription/RequestStop cancels the timer,
    // which still fires the handler (with an aborted error) so we wake
    // immediately.
    auto [future, promise] = yaclib::MakeContract<>();
    auto timer = std::make_shared<asio_ns::steady_timer>(
      exec.Context(), std::chrono::seconds(kReconnectSeconds));
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      it->second.retry = timer;
    }
    timer->async_wait(
      [timer, p = std::move(promise)](const asio_ns::error_code&) mutable {
        std::move(p).Set();
      });
    co_await std::move(future);
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(sub_id);
      if (it != _subs.end()) {
        it->second.retry.reset();
      }
    }
  }
  {
    absl::MutexLock lock{&_mu};
    _subs.erase(sub_id);
  }
  co_return {};
}

void SubscriptionEngine::StartSubscription(
  ObjectId database_id, const std::shared_ptr<catalog::Subscription>& sub) {
  absl::MutexLock lock{&_mu};
  LaunchLocked(database_id, sub);
}

void SubscriptionEngine::StopSubscription(ObjectId subscription_id) {
  duckdb::shared_ptr<PgReplicationClient> client;
  std::shared_ptr<asio_ns::steady_timer> timer;
  {
    absl::MutexLock lock{&_mu};
    auto it = _subs.find(subscription_id);
    if (it == _subs.end()) {
      return;
    }
    it->second.stopping = true;  // the supervisor will stop retrying + erase
    client = it->second.client;
    timer = it->second.retry;
  }
  if (client) {
    client->StopClient();
  }
  if (timer) {
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

void SubscriptionEngine::RestartSubscription(
  ObjectId database_id, const std::shared_ptr<catalog::Subscription>& sub) {
  auto target = MakeTarget(database_id, sub);
  duckdb::shared_ptr<PgReplicationClient> client;
  std::shared_ptr<asio_ns::steady_timer> timer;
  {
    absl::MutexLock lock{&_mu};
    auto it = _subs.find(sub->GetId());
    if (it == _subs.end() || it->second.stopping) {
      return;  // not running -- a later enable picks up the new config
    }
    if (target) {
      it->second.target = std::move(*target);
    }
    it->second.restart = true;
    client = it->second.client;
    timer = it->second.retry;
  }
  // Drop the current client (or wake the reconnect delay): the supervisor then
  // reconnects immediately with the refreshed target.
  if (client) {
    client->StopClient();
  }
  if (timer) {
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

void SubscriptionEngine::DropRemoteSlot(ReplicationTarget target) {
  if (_stopping.load(std::memory_order_acquire) || target.slot_name.empty() ||
      target.host.empty()) {
    return;
  }
  auto& exec = _pool.Next();
  auto client =
    duckdb::make_shared_ptr<PgReplicationClient>(exec, std::move(target));
  // Fire-and-forget: the one-shot coroutine owns itself via shared_from_this
  // until it unwinds, so it is not tracked in _subs.
  asio_ns::post(exec.Context(), [client] { client->StartSlotDrop(); });
}

}  // namespace sdb::replication
