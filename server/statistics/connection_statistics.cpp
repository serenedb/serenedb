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

#include "connection_statistics.h"

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <mutex>
#include <thread>
#include <vector>

#include "rest/common_defines.h"

using namespace sdb;

namespace {

#ifdef SDB_DEV
// this variable is only used in maintainer mode, to check that we are
// only acquiring memory for statistics in case they are enabled.
bool gStatisticsEnabled = false;
#endif

std::atomic<uint64_t> gMemoryUsage = 0;

// initial amount of empty statistics items to be created in statisticsItems
constexpr size_t kInitialQueueSize = 32;

// protects statisticsItems
constinit absl::Mutex gStatisticsMutex{absl::kConstInit};

// a container of ConnectionStatistics objects. the vector is populated
// initially with kInitialQueueSize items. It can grow at runtime. The addresses
// of objects in the vector can be stored in freeList, so the objects must not
// be destroyed if they are still in the free list. access to statisticsItems
// must be protected by statisticsMutex
std::vector<std::unique_ptr<ConnectionStatistics>> gStatisticsItems;

// a free list of ConnectionStatistics objects, not owning them. the free list
// is initially populated with kInitialQueueSize objects.
static boost::lockfree::queue<ConnectionStatistics*> gFreeList{0};

bool EnqueueItem(ConnectionStatistics* item) noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  int tries = 0;

  try {
    do {
      bool ok = gFreeList.push(item);
      if (ok) {
        return true;
      }
      std::this_thread::yield();
    } while (++tries < 1000);
  } catch (...) {
  }

  SDB_ASSERT(false);
  // if for whatever reason the push operation fails, we will
  // have a ConnectionStatistics object in statisticsItems
  // that is not in the queue anymore. this item will be
  // allocated at program end normally, but it becomes useless
  // until then. this should be a very rare case though.
  // each ConnectionStatistics object 24 bytes big.

  return false;
}

}  // namespace

void ConnectionStatistics::Item::SET_HTTP() {
  if (_stat != nullptr) {
    _stat->_http = true;

    statistics::gHttpConnections.incCounter();
  }
}

uint64_t ConnectionStatistics::memoryUsage() noexcept {
  return ::gMemoryUsage.load(std::memory_order_relaxed);
}

void ConnectionStatistics::initialize() {
#ifdef SDB_DEV
  SDB_ASSERT(!gStatisticsEnabled);
  gStatisticsEnabled = true;
#endif

  std::lock_guard guard{::gStatisticsMutex};

  ::gFreeList.reserve(kInitialQueueSize * 2);

  ::gStatisticsItems.reserve(kInitialQueueSize);
  for (size_t i = 0; i < kInitialQueueSize; ++i) {
    // create a new ConnectionStatistics object on the heap
    ::gStatisticsItems.emplace_back(std::make_unique<ConnectionStatistics>());
    // add its address to the freelist
    bool ok = ::EnqueueItem(::gStatisticsItems.back().get());
    if (!ok) {
      // for some reason we couldn't push the item to the queue. so there
      // is no further use for it.
      ::gStatisticsItems.pop_back();
    }
  }

  ::gMemoryUsage.fetch_add(::gStatisticsItems.size() *
                             (sizeof(decltype(::gStatisticsItems)::value_type) +
                              sizeof(ConnectionStatistics)),
                           std::memory_order_relaxed);
}

ConnectionStatistics::Item ConnectionStatistics::acquire() noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  ConnectionStatistics* statistics = nullptr;

  // try the happy path first
  if (!::gFreeList.pop(statistics)) {
    // didn't have any items on the free list. now try the
    // expensive path
    try {
      auto cs = std::make_unique<ConnectionStatistics>();
      // store pointer for just-created item
      statistics = cs.get();

      {
        std::lock_guard guard{::gStatisticsMutex};
        ::gStatisticsItems.emplace_back(std::move(cs));
      }

      ::gMemoryUsage.fetch_add(
        sizeof(decltype(::gStatisticsItems)::value_type) +
          sizeof(ConnectionStatistics),
        std::memory_order_relaxed);
    } catch (...) {
      statistics = nullptr;
    }
  }

  return Item{statistics};
}

void ConnectionStatistics::release() noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  if (_http) {
    statistics::gHttpConnections.decCounter();
  }

  if (_conn_start != 0.0 && _conn_end != 0.0) {
    double total_time = _conn_end - _conn_start;
    statistics::gConnectionTimeDistribution.addFigure(total_time);
  }

  // clear statistics
  reset();

  // put statistics item back onto the freelist
  EnqueueItem(this);
}

void ConnectionStatistics::getSnapshot(Snapshot& snapshot) {
  snapshot.http_connections = statistics::gHttpConnections;
  snapshot.total_requests = statistics::gTotalRequests;
  snapshot.total_requests_superuser = statistics::gTotalRequestsSuperuser;
  snapshot.total_requests_user = statistics::gTotalRequestsUser;
  snapshot.method_requests = statistics::gMethodRequests;
  snapshot.async_requests = statistics::gAsyncRequests;
  snapshot.connection_time = statistics::gConnectionTimeDistribution;
}
