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

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "catalog/types.h"
#include "metrics/fwd.h"
#include "rest_server/serened.h"

namespace sdb {

class FlushThread;

//////////////////////////////////////////////////////////////////////////////
/// @struct FlushSubscription
/// subscription is intenteded to notify FlushFeature
///        on the WAL tick which can be safely released
//////////////////////////////////////////////////////////////////////////////
struct FlushSubscription {
  virtual ~FlushSubscription() = default;
  virtual Tick tick() const = 0;
  virtual std::string_view name() const = 0;
};

class LowerBoundSubscription final : public FlushSubscription {
 public:
  explicit LowerBoundSubscription(Tick tick, std::string&& name)
    : _tick{tick}, _name{std::move(name)} {}

  /// earliest tick that can be released
  [[nodiscard]] Tick tick() const noexcept final {
    return _tick.load(std::memory_order_acquire);
  }

  void tick(Tick tick) noexcept {
    auto value = _tick.load(std::memory_order_acquire);
    SDB_ASSERT(value <= tick);

    // tick value must never go backwards
    while (tick > value) {
      if (_tick.compare_exchange_weak(value, tick, std::memory_order_release,
                                      std::memory_order_acquire)) {
        break;
      }
    }
  }

  std::string_view name() const final { return _name; }

 private:
  std::atomic<Tick> _tick;
  std::string _name;
};

class FlushFeature final {
 public:
  inline static FlushFeature* gInstance = nullptr;
  static FlushFeature& instance() noexcept { return *gInstance; }

  FlushFeature();
  ~FlushFeature();

  void start() {}
  void stop();

  /// register a flush subscription that will ensure replay of all WAL
  ///        entries after the latter of registration or the last successful
  ///        token commit
  /// subscription to register
  void registerFlushSubscription(
    const std::shared_ptr<FlushSubscription>& subscription);

  /// release all ticks not used by the flush subscriptions
  /// returns number of active flush subscriptions removed, the number of stale
  /// flush scriptions removed, and the tick value up to which the storage
  /// engine could release ticks. if no active or stale flush subscriptions were
  /// found, the returned tick value is 0.
  std::tuple<size_t, size_t, Tick> releaseUnusedTicks();

 private:
  absl::Mutex _flush_subscriptions_mutex;
  std::vector<std::weak_ptr<FlushSubscription>> _flush_subscriptions;

  metrics::Gauge<uint64_t>& _metrics_flush_subscriptions;
};

}  // namespace sdb
