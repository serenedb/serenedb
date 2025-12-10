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

#include "flush_feature.h"

#include "app/app_server.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/encoding.h"
#include "basics/logger/logger.h"
#include "general_server/state.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "storage_engine/engine_selector_feature.h"
#include "storage_engine/storage_engine.h"

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

DECLARE_GAUGE(serenedb_flush_subscriptions, uint64_t,
              "Number of active flush subscriptions");

namespace sdb {

FlushFeature::FlushFeature(Server& server)
  : SerenedFeature{server, name()},
    _stopped(false),
    _metrics_flush_subscriptions(
      server.getFeature<metrics::MetricsFeature>().add(
        serenedb_flush_subscriptions{})) {
  setOptional(true);

  static_assert(
    Server::isCreatedAfter<FlushFeature, metrics::MetricsFeature>());
}

FlushFeature::~FlushFeature() = default;

void FlushFeature::registerFlushSubscription(
  const std::shared_ptr<FlushSubscription>& subscription) {
  if (!subscription) {
    return;
  }

  std::lock_guard lock{_flush_subscriptions_mutex};

  if (_stopped) {
    SDB_ERROR("xxxxx", Logger::FLUSH, "FlushFeature not running");
    return;
  }

  _flush_subscriptions.emplace_back(subscription);

  SDB_DEBUG("xxxxx", sdb::Logger::FLUSH,
            "registered flush subscription: ", subscription->name(), ", tick ",
            subscription->tick());
}

std::tuple<size_t, size_t, Tick> FlushFeature::releaseUnusedTicks() {
  auto& engine = server().getFeature<EngineSelectorFeature>().engine();
  const auto initial_tick = engine.currentTick();

  size_t stale = 0;
  size_t active = 0;
  auto min_tick = initial_tick;

  {
    std::lock_guard lock{_flush_subscriptions_mutex};
    auto begin = _flush_subscriptions.begin();
    auto end = _flush_subscriptions.end();
    auto it = std::remove_if(begin, end, [&](auto& e) noexcept {
      if (auto entry = e.lock(); entry) {
        min_tick = std::min(min_tick, entry->tick());
        return false;
      }
      return true;
    });
    stale = static_cast<size_t>(end - it);
    active = static_cast<size_t>(it - begin);
    _flush_subscriptions.erase(it, end);
  }

  SDB_ASSERT(min_tick <= engine.currentTick());

  SDB_IF_FAILURE("FlushCrashBeforeSyncingMinTick") {
    if (ServerState::instance()->IsDBServer() ||
        ServerState::instance()->IsSingle()) {
      TerminateDebugging("crashing before syncing min tick");
    }
  }

  engine.releaseTick(min_tick);

  SDB_IF_FAILURE("FlushCrashAfterReleasingMinTick") {
    if (ServerState::instance()->IsDBServer() ||
        ServerState::instance()->IsSingle()) {
      TerminateDebugging("crashing after releasing min tick");
    }
  }

  SDB_DEBUG("xxxxx", sdb::Logger::FLUSH, "Flush tick released: ", min_tick,
            ", stale flush subscription(s) released: ", stale,
            ", active flush subscription(s): ", active,
            ", initial engine tick: ", initial_tick);

  _metrics_flush_subscriptions.store(active, std::memory_order_relaxed);

  return std::tuple{active, stale, min_tick};
}

void FlushFeature::stop() {
  std::lock_guard lock{_flush_subscriptions_mutex};

  _flush_subscriptions.clear();
  _stopped = true;
}

}  // namespace sdb
