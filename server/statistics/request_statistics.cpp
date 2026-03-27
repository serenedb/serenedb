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

#include "request_statistics.h"

#include <absl/strings/internal/ostringstream.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <iomanip>
#include <mutex>
#include <thread>
#include <vector>

using namespace sdb;

namespace {

#ifdef SDB_DEV
// this variable is only used in maintainer mode, to check that we are
// only acquiring memory for statistics in case they are enabled.
bool gStatisticsEnabled = false;
#endif

std::atomic<uint64_t> gMemoryUsage = 0;

// initial amount of empty statistics items to be created in statisticsItems
constexpr size_t kInitialQueueSize = 64;

// protects statisticsItems
constinit absl::Mutex gStatisticsMutex{absl::kConstInit};

// a container of RequestStatistics objects. the vector is populated initially
// with kInitialQueueSize items. It can grow at runtime. The addresses of
// objects in the vector can be stored in freeList, so the objects must not be
// destroyed if they are still in the free list. access to statisticsItems must
// be protected by statisticsMutex
std::vector<std::unique_ptr<RequestStatistics>> gStatisticsItems;

// a free list of RequestStatistics objects, not owning them. the free list
// is initially populated with kInitialQueueSize objects.
boost::lockfree::queue<RequestStatistics*> gFreeList{0};

// a list of finished (to-be-process) RequestStatistics objects, not owning
// them. this list will be initially empty
boost::lockfree::queue<RequestStatistics*> gFinishedList{0};

bool EnqueueItem(boost::lockfree::queue<RequestStatistics*>& queue,
                 RequestStatistics* item) noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  int tries = 0;

  try {
    do {
      bool ok = queue.push(item);
      if (ok) {
        return true;
      }
      std::this_thread::yield();
    } while (++tries < 1000);
  } catch (...) {
  }

  // if for whatever reason the push operation fails, we will
  // have a RequestStatistics object in statisticsItems
  // that is not in the queue anymore. this item will be
  // allocated at program end normally, but it becomes useless
  // until then. this should be a very rare case though.
  // a RequestStatisticsItem is around 100 bytes big.

  return false;
}

}  // namespace

uint64_t RequestStatistics::memoryUsage() noexcept {
  return ::gMemoryUsage.load(std::memory_order_relaxed);
}

void RequestStatistics::initialize() {
#ifdef SDB_DEV
  SDB_ASSERT(!gStatisticsEnabled);
  gStatisticsEnabled = true;
#endif

  std::lock_guard guard{::gStatisticsMutex};

  ::gFreeList.reserve(kInitialQueueSize * 2);
  ::gFinishedList.reserve(kInitialQueueSize * 2);

  ::gStatisticsItems.reserve(kInitialQueueSize);
  for (size_t i = 0; i < kInitialQueueSize; ++i) {
    // create a new RequestStatistics object on the heap
    ::gStatisticsItems.emplace_back(std::make_unique<RequestStatistics>());
    RequestStatistics* item = ::gStatisticsItems.back().get();
    // add its address to the freelist
    bool ok = ::EnqueueItem(::gFreeList, item);
    if (!ok) {
      // for some reason we couldn't push the item to the queue. so there
      // is no further use for it.
      ::gStatisticsItems.pop_back();
    }
  }

  ::gMemoryUsage.fetch_add(::gStatisticsItems.size() *
                             (sizeof(decltype(::gStatisticsItems)::value_type) +
                              sizeof(RequestStatistics)),
                           std::memory_order_relaxed);
}

size_t RequestStatistics::processAll() {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  RequestStatistics* statistics = nullptr;
  size_t count = 0;

  while (::gFinishedList.pop(statistics)) {
    if (statistics != nullptr) {
      process(statistics);
      ++count;
    }
  }

  return count;
}

RequestStatistics::Item RequestStatistics::acquire() noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  RequestStatistics* statistics = nullptr;

  // try the happy path first
  if (::gFreeList.pop(statistics)) {
    return Item{statistics};
  }

  // didn't have any items on the free list. now try the
  // expensive path
  try {
    auto cs = std::make_unique<RequestStatistics>();
    // store pointer for just-created item
    statistics = cs.get();

    {
      std::lock_guard guard{::gStatisticsMutex};
      ::gStatisticsItems.emplace_back(std::move(cs));
    }

    ::gMemoryUsage.fetch_add(sizeof(decltype(::gStatisticsItems)::value_type) +
                               sizeof(RequestStatistics),
                             std::memory_order_relaxed);
  } catch (...) {
    statistics = nullptr;
  }

  return Item{statistics};
}

void RequestStatistics::release() noexcept {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  ::EnqueueItem(::gFinishedList, this);
}

void RequestStatistics::process(RequestStatistics* statistics) {
#ifdef SDB_DEV
  SDB_ASSERT(gStatisticsEnabled);
#endif

  SDB_ASSERT(statistics != nullptr);

  statistics::gTotalRequests.incCounter();

  if (statistics->_async) {
    statistics::gAsyncRequests.incCounter();
  }

  statistics::gMethodRequests[(size_t)statistics->_request_type].incCounter();

  // check that the request was completely received and transmitted
  if (statistics->_read_start != 0.0 &&
      (statistics->_async || statistics->_write_end != 0.0)) {
    double total_time;

    if (statistics->_async) {
      total_time = statistics->_request_end - statistics->_read_start;
    } else {
      total_time = statistics->_write_end - statistics->_read_start;
    }

    const bool is_superuser = statistics->_superuser;
    if (is_superuser) {
      statistics::gTotalRequestsSuperuser.incCounter();
    } else {
      statistics::gTotalRequestsUser.incCounter();
    }

    statistics::RequestFigures& figures =
      is_superuser ? statistics::gSuperuserRequestFigures
                   : statistics::gUserRequestFigures;

    figures.total_time_distribution.addFigure(total_time);

    double request_time = statistics->_request_end - statistics->_request_start;
    figures.request_time_distribution.addFigure(request_time);

    double queue_time = 0.0;
    if (statistics->_queue_start != 0.0 && statistics->_queue_end != 0.0) {
      queue_time = statistics->_queue_end - statistics->_queue_start;
      figures.queue_time_distribution.addFigure(queue_time);
    }

    double io_time = total_time - request_time - queue_time;
    if (io_time >= 0.0) {
      figures.io_time_distribution.addFigure(io_time);
    }

    figures.bytes_sent_distribution.addFigure(statistics->_sent_bytes);
    figures.bytes_received_distribution.addFigure(statistics->_received_bytes);
  }

  // clear statistics
  statistics->reset();

  // put statistics item back onto the freelist
  ::EnqueueItem(::gFreeList, statistics);
}

void RequestStatistics::getSnapshot(Snapshot& snapshot,
                                    stats::RequestStatisticsSource source) {
  statistics::RequestFigures& figures =
    source == stats::RequestStatisticsSource::kUser
      ? statistics::gUserRequestFigures
      : statistics::gSuperuserRequestFigures;

  snapshot.total_time = figures.total_time_distribution;
  snapshot.request_time = figures.request_time_distribution;
  snapshot.queue_time = figures.queue_time_distribution;
  snapshot.io_time = figures.io_time_distribution;
  snapshot.bytes_sent = figures.bytes_sent_distribution;
  snapshot.bytes_received = figures.bytes_received_distribution;

  if (source == stats::RequestStatisticsSource::kAll) {
    SDB_ASSERT(&figures == &statistics::gSuperuserRequestFigures);
    snapshot.total_time.add(
      statistics::gUserRequestFigures.total_time_distribution);
    snapshot.request_time.add(
      statistics::gUserRequestFigures.request_time_distribution);
    snapshot.queue_time.add(
      statistics::gUserRequestFigures.queue_time_distribution);
    snapshot.io_time.add(statistics::gUserRequestFigures.io_time_distribution);
    snapshot.bytes_sent.add(
      statistics::gUserRequestFigures.bytes_sent_distribution);
    snapshot.bytes_received.add(
      statistics::gUserRequestFigures.bytes_received_distribution);
  }
}
