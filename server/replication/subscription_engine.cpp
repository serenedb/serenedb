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

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>

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
#include <yaclib/coro/on.hpp>

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

template<typename Read>
void ReadCommitted(duckdb::AttachedDatabase& attached,
                   duckdb::idx_t subscription, Read&& read) {
  attached.GetCatalog()
    .Cast<duckdb::DuckCatalog>()
    .GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
    .Scan([&](duckdb::CatalogEntry& entry) {
      if (entry.oid == subscription) {
        read(entry.Cast<catalog::SubscriptionCatalogEntry>());
      }
    });
}

enum class Presence : uint8_t {
  Missing,
  Disabled,
  Enabled,
};

Presence Lookup(std::string_view database, duckdb::idx_t subscription) {
  auto presence = Presence::Missing;
  if (auto attached = FindDatabase(database)) {
    ReadCommitted(*attached, subscription,
                  [&](const catalog::SubscriptionCatalogEntry& entry) {
                    presence = entry.Config().enabled ? Presence::Enabled
                                                      : Presence::Disabled;
                  });
  }
  return presence;
}

ReplicationTarget MakeReplicationTarget(
  const duckdb::CreateSubscriptionInfo& config, duckdb::idx_t subscription,
  std::string_view database) {
  ReplicationTarget target;
  target.conninfo = ParseConnInfo(config.conninfo);
  target.subscription_oid = subscription;
  target.database_name.assign(database);
  target.subscription_name =
    config.GetQualifiedName().Name().GetIdentifierName();
  target.publications = config.publications;
  target.slot_name = config.slot_name;
  target.binary = config.binary;
  target.streaming = config.streaming != "off";
  target.disable_on_error = config.disable_on_error;
  target.run_as_owner = config.run_as_owner;
  target.origin = config.origin;
  target.start_lsn = config.remote_lsn;
  target.skip_lsn = config.skip_lsn;
  target.relations.assign(config.relations.begin(), config.relations.end());
  target.owner_id = config.permissions.owner;
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

std::optional<ReplicationTarget> ResolveTarget(std::string_view database,
                                               duckdb::idx_t subscription) {
  auto attached = FindDatabase(database);
  if (!attached) {
    return std::nullopt;
  }
  duckdb::unique_ptr<duckdb::CreateSubscriptionInfo> config;
  ReadCommitted(*attached, subscription,
                [&](const catalog::SubscriptionCatalogEntry& entry) {
                  if (entry.Config().enabled) {
                    config =
                      duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                              duckdb::CreateSubscriptionInfo>(
                        entry.GetInfo());
                  }
                });
  if (!config) {
    return std::nullopt;
  }
  auto target = MakeReplicationTarget(*config, subscription, database);
  target.database_oid = attached->oid;
  return target;
}

}  // namespace

SubscriptionEngine::SubscriptionEngine(network::IoThreadPool& pool)
  : _pool(pool) {
  gInstance = this;
}

SubscriptionEngine::~SubscriptionEngine() {
  RequestStop();
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
        if (entry.Cast<catalog::SubscriptionCatalogEntry>().Config().enabled) {
          subscriptions.emplace_back(database->GetName().GetIdentifierName(),
                                     entry.oid);
        }
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

void SubscriptionEngine::stop() {
  RequestStop();
  absl::MutexLock lock{&_mu};
  _mu.Await(absl::Condition(
    +[](decltype(_subs)* subs) { return subs->empty(); }, &_subs));
}

void SubscriptionEngine::Sync(std::string_view database,
                              duckdb::idx_t subscription, bool restart) {
  const auto presence = Lookup(database, subscription);
  std::optional<ReplicationTarget> target;
  if (presence == Presence::Enabled && !restart) {
    try {
      target = ResolveTarget(database, subscription);
    } catch (const std::exception&) {
    }
  }
  absl::MutexLock lock{&_mu};
  if (presence == Presence::Missing) {
    _stats.erase(subscription);
  }
  auto it = _subs.find(subscription);
  if (presence != Presence::Enabled) {
    if (it != _subs.end()) {
      StopLocked(it->second);
    }
    return;
  }
  if (it == _subs.end() || it->second.stopping) {
    LaunchLocked(database, subscription);
    return;
  }
  if (restart || !it->second.client || !target || !it->second.target ||
      !SameConfig(*it->second.target, *target)) {
    RestartLocked(it->second);
  }
}

irs::containers::FlatHashMap<duckdb::idx_t, SubscriptionEngine::SubRuntime>
SubscriptionEngine::RuntimeSnapshot(std::string_view database) const {
  irs::containers::FlatHashMap<duckdb::idx_t, SubRuntime> result;
  absl::MutexLock lock{&_mu};
  for (const auto& [subscription, state] : _subs) {
    if (!state.client || state.database != database ||
        !state.client->Connected()) {
      continue;
    }
    result.emplace(subscription,
                   SubRuntime{
                     .received_lsn = state.client->ReceivedLsn(),
                     .flushed_lsn = state.client->FlushedLsn(),
                     .last_send_time = state.client->LastSendTime(),
                     .last_receipt_time = state.client->LastReceiptTime(),
                     .latest_end_time = state.client->LatestEndTime(),
                   });
  }
  return result;
}

irs::containers::FlatHashMap<duckdb::idx_t, SubscriptionEngine::SubStats>
SubscriptionEngine::Stats() const {
  absl::MutexLock lock{&_mu};
  auto stats = _stats;
  for (const auto& [subscription, state] : _subs) {
    if (state.client) {
      stats[subscription].Add(state.client->Conflicts());
    }
  }
  return stats;
}

void SubscriptionEngine::ResetStats(std::optional<duckdb::idx_t> subscription) {
  const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  std::vector<duckdb::idx_t> subscriptions;
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
        if (!subscription || *subscription == entry.oid) {
          subscriptions.push_back(entry.oid);
        }
      });
  }
  absl::MutexLock lock{&_mu};
  for (const auto id : subscriptions) {
    _stats[id] = {.stats_reset = now};
    if (auto it = _subs.find(id); it != _subs.end() && it->second.client) {
      it->second.client->ResetConflicts();
    }
  }
}

SubscriptionEngine::SubStats& SubscriptionEngine::StatsLocked(
  duckdb::idx_t subscription) {
  return _stats[subscription];
}

bool SubscriptionEngine::Running(duckdb::idx_t subscription) const {
  absl::MutexLock lock{&_mu};
  const auto it = _subs.find(subscription);
  return it != _subs.end() && it->second.client && !it->second.stopping;
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
  asio_ns::post(exec.Context(), [this, subscription, &exec] {
    Supervise(subscription, exec).Detach();
  });
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

yaclib::Task<bool> SubscriptionEngine::Backoff(
  duckdb::idx_t subscription, network::IoExecutor& exec,
  std::chrono::milliseconds delay) {
  auto [future, promise] = yaclib::MakeContract<>();
  auto timer = std::make_shared<asio_ns::steady_timer>(exec.Context(), delay);
  {
    absl::MutexLock lock{&_mu};
    auto it = _subs.find(subscription);
    if (it == _subs.end() || it->second.stopping) {
      co_return false;
    }
    it->second.retry = timer;
  }
  timer->async_wait(
    [timer, p = std::move(promise)](const asio_ns::error_code&) mutable {
      std::move(p).Set();
    });
  co_await std::move(future);
  absl::MutexLock lock{&_mu};
  if (auto it = _subs.find(subscription); it != _subs.end()) {
    it->second.retry.reset();
  }
  co_return true;
}

yaclib::Task<> SubscriptionEngine::Supervise(duckdb::idx_t subscription,
                                             network::IoExecutor& exec) {
  std::string name = absl::StrCat(subscription);
  std::string database;
  for (;;) {
    if (_stopping.load(std::memory_order_acquire)) {
      break;
    }
    size_t host = 0;
    size_t encryption = 0;
    uint64_t host_seed = 0;
    bool any_session = false;
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      auto& state = it->second;
      state.restart = false;
      database = state.database;
      host = state.host;
      encryption = state.encryption;
      if (host == 0 && encryption == 0 && !state.any_session) {
        state.host_seed = absl::Uniform<uint64_t>(absl::BitGen{});
      }
      host_seed = state.host_seed;
      any_session = state.any_session;
    }
    duckdb::shared_ptr<PgReplicationClient> client;
    auto target_attrs = SessionAttrs::Any;
    size_t hosts = 1;
    try {
      auto target = ResolveTarget(database, subscription);
      if (!target) {
        break;
      }
      ArrangeHosts(target->conninfo, host_seed, any_session);
      name = target->subscription_name;
      target_attrs = target->conninfo.target_session_attrs;
      hosts = std::max<size_t>(target->conninfo.hosts.size(), 1);
      host %= hosts;
      client = duckdb::make_shared_ptr<PgReplicationClient>(exec, *target, host,
                                                            encryption);
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end() || it->second.stopping) {
        break;
      }
      if (it->second.restart) {
        continue;
      }
      it->second.client = client;
      it->second.target = std::move(*target);
    } catch (const std::exception& ex) {
      SDB_WARN(REPLICATION, "subscription '", name,
               "' cannot start: ", ex.what());
    }
    if (!client) {
      {
        absl::MutexLock lock{&_mu};
        ++StatsLocked(subscription).apply_error_count;
      }
      if (!co_await Backoff(
            subscription, exec,
            std::chrono::milliseconds{WalRetrieveRetryIntervalMillis()})) {
        break;
      }
      continue;
    }
    co_await client->RunClient();
    co_await yaclib::On(exec);
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
    bool retry_now = false;
    std::chrono::milliseconds delay{WalRetrieveRetryIntervalMillis()};
    {
      absl::MutexLock lock{&_mu};
      auto it = _subs.find(subscription);
      if (it == _subs.end()) {
        break;
      }
      auto& state = it->second;
      auto& stats = StatsLocked(subscription);
      stats.Add(client->Conflicts());
      state.client.reset();
      restart = state.restart;
      const bool failed = !restart && !state.stopping &&
                          !_stopping.load(std::memory_order_acquire);
      if (client->Connected()) {
        state.any_session = false;
        state.encryption = 0;
      }
      if (failed || disable) {
        if (!client->Connected()) {
          if (client->RetryEncryption()) {
            state.encryption = encryption + 1;
            retry_now = true;
          } else {
            state.encryption = 0;
            state.host = (host + 1) % hosts;
            retry_now = state.host != 0;
            if (state.host == 0 && !state.any_session &&
                target_attrs == SessionAttrs::PreferStandby) {
              state.any_session = true;
              retry_now = true;
            } else if (state.host == 0) {
              state.any_session = false;
            }
          }
        } else if (client->SyncFailed()) {
          ++stats.sync_error_count;
        } else if (!client->Transient()) {
          ++stats.apply_error_count;
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
    if (retry_now) {
      continue;
    }
    if (!co_await Backoff(subscription, exec, delay)) {
      break;
    }
  }
  const bool missing =
    !database.empty() && Lookup(database, subscription) == Presence::Missing;
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
    if (missing) {
      _stats.erase(subscription);
    }
  }
  if (relaunch) {
    auto& next = _pool.Next();
    asio_ns::post(next.Context(), [this, subscription, &next] {
      Supervise(subscription, next).Detach();
    });
  }
  co_return {};
}

}  // namespace sdb::replication
