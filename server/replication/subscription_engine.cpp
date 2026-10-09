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

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <utility>
#include <yaclib/async/contract.hpp>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "connector/duckdb_client_state.h"
#include "network/io_context.h"
#include "replication/settings.h"

namespace sdb::replication {
namespace {

constexpr auto kTransientRetry = std::chrono::milliseconds{50};

bool SameConfig(ReplicationTarget running, const ReplicationTarget& target) {
  running.start_lsn = target.start_lsn;
  if (running.relations.size() != target.relations.size()) {
    return false;
  }
  for (size_t i = 0; i < running.relations.size(); ++i) {
    running.relations[i].state = target.relations[i].state;
    running.relations[i].lsn = target.relations[i].lsn;
  }
  return running == target;
}

duckdb::shared_ptr<duckdb::AttachedDatabase> FindDatabase(
  std::string_view name) {
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  auto database = manager.GetDatabase(duckdb::Identifier{name});
  if (!database || database->GetCatalog().GetCatalogType() !=
                     catalog::SereneDBCatalog::kStorageType) {
    return nullptr;
  }
  return database;
}

std::optional<ReplicationTarget> ResolveTarget(std::string_view database,
                                               duckdb::idx_t subscription) {
  auto attached = FindDatabase(database);
  if (!attached) {
    return std::nullopt;
  }
  auto entry = attached->GetCatalog()
                 .Cast<duckdb::DuckCatalog>()
                 .GetOidIndex()
                 .GetCommitted(subscription);
  if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    return std::nullopt;
  }
  auto target = MakeReplicationTarget(
    entry->Cast<catalog::SubscriptionCatalogEntry>(), database);
  target.database_oid = attached->oid;
  return target;
}

bool Enabled(std::string_view database, duckdb::idx_t subscription) {
  auto attached = FindDatabase(database);
  if (!attached) {
    return false;
  }
  auto entry = attached->GetCatalog()
                 .Cast<duckdb::DuckCatalog>()
                 .GetOidIndex()
                 .GetCommitted(subscription);
  return entry && entry->type == duckdb::CatalogType::SUBSCRIPTION_ENTRY &&
         entry->Cast<catalog::SubscriptionCatalogEntry>().Config().enabled;
}

}  // namespace

ReplicationTarget MakeReplicationTarget(
  const catalog::SubscriptionCatalogEntry& subscription,
  std::string_view database) {
  const auto& config = subscription.Config();
  ReplicationTarget target;
  target.conninfo = ParseConnInfo(config.conninfo);
  target.subscription_oid = subscription.oid;
  target.database_name.assign(database);
  target.subscription_name = subscription.name.GetIdentifierName();
  target.publications = config.publications;
  target.slot_name = config.slot_name;
  target.binary = config.binary;
  target.streaming = config.streaming != "off";
  target.disable_on_error = config.disable_on_error;
  target.run_as_owner = config.run_as_owner;
  target.origin = config.origin;
  target.start_lsn = subscription.RemoteLsn();
  target.skip_lsn = config.skip_lsn;
  target.relations = config.relations;
  target.owner_id = subscription.permissions.owner;
  auto roles = auth::RolesOf(nullptr);
  target.owner_name = roles->NameOf(target.owner_id);
  const auto* owner = roles->Find(target.owner_id);
  target.require_password =
    config.password_required && !(owner && owner->is_superuser);
  if (target.conninfo.user.empty()) {
    target.conninfo.user = target.owner_name;
  }
  return target;
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
  std::vector<std::pair<std::string, duckdb::idx_t>> subscriptions;
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  for (const auto& database : manager.GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<duckdb::DuckCatalog>()
      .GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
      .Scan([&](duckdb::CatalogEntry& entry) {
        subscriptions.emplace_back(database->GetName().GetIdentifierName(),
                                   entry.oid);
      });
  }
  absl::MutexLock lock{&_mu};
  for (const auto& [database, subscription] : subscriptions) {
    LaunchLocked(database, subscription);
  }
  SDB_INFO(REPLICATION, "subscription engine started (", _subs.size(),
           " enabled subscriber(s))");
}

void SubscriptionEngine::RequestStop() noexcept {
  _stopping.store(true, std::memory_order_release);
  absl::MutexLock lock{&_mu};
  for (auto& [_, state] : _subs) {
    StopLocked(state);
  }
}

void SubscriptionEngine::stop() { RequestStop(); }

void SubscriptionEngine::Sync(std::string_view database,
                              duckdb::idx_t subscription, bool restart) {
  const bool enabled = Enabled(database, subscription);
  absl::MutexLock lock{&_mu};
  auto it = _subs.find(subscription);
  if (!enabled) {
    if (it != _subs.end()) {
      StopLocked(it->second);
    }
    return;
  }
  if (it == _subs.end() || it->second.stopping) {
    LaunchLocked(database, subscription);
    return;
  }
  if (restart || !it->second.client) {
    RestartLocked(it->second);
    return;
  }
  auto target = ResolveTarget(database, subscription);
  if (!target) {
    StopLocked(it->second);
    return;
  }
  if (!it->second.target || !SameConfig(*it->second.target, *target)) {
    RestartLocked(it->second);
  }
}

std::vector<SubscriptionEngine::SubRuntime> SubscriptionEngine::RuntimeSnapshot(
  std::string_view database) const {
  std::vector<SubRuntime> result;
  absl::MutexLock lock{&_mu};
  for (const auto& [subscription, state] : _subs) {
    if (!state.client || state.database != database) {
      continue;
    }
    if (!state.client->Connected()) {
      continue;
    }
    result.push_back({
      .subscription = subscription,
      .received_lsn = state.client->ReceivedLsn(),
      .flushed_lsn = state.client->FlushedLsn(),
      .last_send_time = state.client->LastSendTime(),
      .last_receipt_time = state.client->LastReceiptTime(),
      .latest_end_time = state.client->LatestEndTime(),
    });
  }
  return result;
}

std::vector<SubscriptionEngine::SubStats> SubscriptionEngine::Stats(
  std::string_view database) const {
  std::vector<SubStats> result;
  absl::MutexLock lock{&_mu};
  for (const auto& [subscription, state] : _subs) {
    if (state.database != database) {
      continue;
    }
    auto stats = state.stats;
    stats.subscription = subscription;
    if (state.client) {
      const auto& conflicts = state.client->Conflicts();
      stats.insert_exists += conflicts.insert_exists.load();
      stats.update_exists += conflicts.update_exists.load();
      stats.update_missing += conflicts.update_missing.load();
      stats.delete_missing += conflicts.delete_missing.load();
      stats.multiple_unique_conflicts +=
        conflicts.multiple_unique_conflicts.load();
    }
    result.push_back(stats);
  }
  return result;
}

void SubscriptionEngine::ResetStats(std::optional<duckdb::idx_t> subscription) {
  const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  absl::MutexLock lock{&_mu};
  for (auto& [id, state] : _subs) {
    if (subscription && *subscription != id) {
      continue;
    }
    state.stats = {.stats_reset = now};
    if (state.client) {
      state.client->ResetConflicts();
    }
  }
}

void SubscriptionEngine::Stop(duckdb::idx_t subscription) {
  absl::MutexLock lock{&_mu};
  if (auto it = _subs.find(subscription); it != _subs.end()) {
    StopLocked(it->second);
  }
}

void SubscriptionEngine::LaunchLocked(std::string_view database,
                                      duckdb::idx_t subscription) {
  if (_stopping.load(std::memory_order_acquire)) {
    return;
  }
  auto [it, inserted] = _subs.try_emplace(subscription);
  if (!inserted) {
    if (!it->second.stopping) {
      return;
    }
    it->second.stopping = false;
    it->second.restart = true;
    return;
  }
  it->second.database.assign(database);
  auto& exec = _pool.Next();
  asio_ns::post(exec.Context(),
                [this, subscription] { Supervise(subscription).Detach(); });
}

void SubscriptionEngine::StopLocked(SubState& state) {
  state.stopping = true;
  if (state.client) {
    state.client->StopClient();
  }
  if (auto timer = state.retry) {
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

void SubscriptionEngine::RestartLocked(SubState& state) {
  state.restart = true;
  if (state.client) {
    state.client->StopClient();
  }
  if (auto timer = state.retry) {
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

void SubscriptionEngine::Disable(std::string_view database,
                                 duckdb::idx_t subscription) {
  auto attached = FindDatabase(database);
  if (!attached) {
    return;
  }
  auto system = connector::MakeSystemConnection(database, attached->oid);
  auto& context = *system.conn->context;
  context.RunFunctionInTransaction([&] {
    auto& catalog = attached->GetCatalog().Cast<catalog::SereneDBCatalog>();
    const auto transaction = catalog.GetCatalogTransaction(context);
    auto entry =
      catalog.GetOidIndex().GetVisible(subscription, transaction.view);
    if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
      return;
    }
    auto& current = entry->Cast<catalog::SubscriptionCatalogEntry>();
    auto definition = duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                              duckdb::CreateSubscriptionInfo>(
      current.GetInfo());
    definition->enabled = false;
    duckdb::ReplaceDefinitionInfo alter{std::move(definition)};
    alter.SetQualifiedName(duckdb::QualifiedName(current.name));
    catalog.Alter(transaction, alter);
  });
}

yaclib::Task<> SubscriptionEngine::Supervise(duckdb::idx_t subscription) {
  auto& exec = _pool.Next();
  std::string name;
  for (;;) {
    if (_stopping.load(std::memory_order_acquire)) {
      break;
    }
    std::string database;
    size_t host = 0;
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      it->second.restart = false;
      database = it->second.database;
      host = it->second.host;
    }
    auto target = ResolveTarget(database, subscription);
    if (!target) {
      break;
    }
    name = target->subscription_name;
    const auto hosts = std::max<size_t>(target->conninfo.hosts.size(), 1);
    host %= hosts;
    auto client =
      duckdb::make_shared_ptr<PgReplicationClient>(exec, *target, host);
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      it->second.client = client;
      it->second.target = std::move(*target);
    }
    co_await client->RunClient();
    const bool disable = client->DisableRequested();
    if (disable) {
      SDB_WARN(REPLICATION, "subscription \"", name,
               "\" has been disabled because of an error");
      try {
        Disable(database, subscription);
      } catch (const std::exception& ex) {
        SDB_WARN(REPLICATION, "subscription '", name,
                 "' disable failed: ", ex.what());
      }
    }
    bool restart = false;
    std::chrono::milliseconds delay{WalRetrieveRetryIntervalMillis()};
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end()) {
        break;
      }
      auto& state = it->second;
      const auto& conflicts = client->Conflicts();
      state.stats.insert_exists += conflicts.insert_exists.load();
      state.stats.update_exists += conflicts.update_exists.load();
      state.stats.update_missing += conflicts.update_missing.load();
      state.stats.delete_missing += conflicts.delete_missing.load();
      state.stats.multiple_unique_conflicts +=
        conflicts.multiple_unique_conflicts.load();
      state.client.reset();
      restart = state.restart;
      const bool failed = !restart && !state.stopping &&
                          !_stopping.load(std::memory_order_acquire);
      if (failed || disable) {
        if (!client->Connected()) {
          state.host = (host + 1) % hosts;
        } else if (client->SyncFailed()) {
          ++state.stats.sync_error_count;
        } else if (!client->Transient()) {
          ++state.stats.apply_error_count;
        }
      }
      if (disable || state.stopping ||
          _stopping.load(std::memory_order_acquire)) {
        break;
      }
      if (client->Transient()) {
        state.transient_failures = std::min(state.transient_failures + 1, 7u);
        delay = kTransientRetry * (1u << state.transient_failures);
      } else {
        state.transient_failures = 0;
      }
    }
    if (restart) {
      SDB_INFO(REPLICATION, "subscription '", name,
               "' restarting with updated configuration");
      continue;
    }
    if (!client->Connected() && host + 1 < hosts) {
      continue;
    }
    auto [future, promise] = yaclib::MakeContract<>();
    auto timer = std::make_shared<asio_ns::steady_timer>(exec.Context(), delay);
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
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
      auto it = _subs.find(subscription);
      if (it != _subs.end()) {
        it->second.retry.reset();
      }
    }
  }
  bool relaunch = false;
  {
    absl::MutexLock lock{&_mu};
    auto it = _subs.find(subscription);
    if (it != _subs.end()) {
      relaunch = !it->second.stopping && it->second.restart &&
                 !_stopping.load(std::memory_order_acquire);
      if (relaunch) {
        it->second.target.reset();
      } else {
        _subs.erase(it);
      }
    }
  }
  if (relaunch) {
    auto& next = _pool.Next();
    asio_ns::post(next.Context(),
                  [this, subscription] { Supervise(subscription).Detach(); });
  }
  co_return {};
}

}  // namespace sdb::replication
