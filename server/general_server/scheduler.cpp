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

#include "scheduler.h"

#include <vpack/builder.h>

#include <thread>

#include "app/app_server.h"
#include "basics/exceptions.h"
#include "basics/lifecycle.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "basics/string_utils.h"
#include "basics/thread.h"
#include "general_server/acceptor.h"
#include "general_server/rest_handler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "metrics/counter_builder.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "rest/general_response.h"
#include "statistics/request_statistics.h"

namespace sdb {
namespace {

class SchedulerThread : public ServerThread<SerenedServer> {
 public:
  explicit SchedulerThread(Server& server, Scheduler& scheduler,
                           const std::string& name = "Scheduler")
    : ServerThread<SerenedServer>(server, name), _scheduler(scheduler) {}

  // shutdown is called by derived implementation!
  ~SchedulerThread() override = default;

 protected:
  Scheduler& _scheduler;
};

}  // namespace

class SchedulerCronThread final : public SchedulerThread {
 public:
  explicit SchedulerCronThread(SerenedServer& server, Scheduler& scheduler)
    : SchedulerThread(server, scheduler, "SchedCron") {}

  ~SchedulerCronThread() final { shutdown(); }

  void run() final { _scheduler.runCronThread(); }
};

DECLARE_COUNTER(serenedb_scheduler_handler_tasks_created_total,
                "Number of scheduler tasks created");
DECLARE_COUNTER(serenedb_scheduler_queue_time_violations_total,
                "Tasks dropped because the client-requested queue time "
                "restriction would be violated");

DECLARE_GAUGE(
  serenedb_scheduler_ongoing_low_prio, uint64_t,
  "Total number of ongoing RestHandlers coming from the low prio queue");
DECLARE_GAUGE(serenedb_scheduler_low_prio_queue_last_dequeue_time, uint64_t,
              "Last recorded dequeue time for a low priority queue item [ms]");

DECLARE_GAUGE(
  serenedb_scheduler_low_prio_queue_length, uint64_t,
  "Current queue length of the low priority queue in the scheduler");
DECLARE_GAUGE(
  serenedb_scheduler_medium_prio_queue_length, uint64_t,
  "Current queue length of the medium priority queue in the scheduler");
DECLARE_GAUGE(
  serenedb_scheduler_high_prio_queue_length, uint64_t,
  "Current queue length of the high priority queue in the scheduler");
DECLARE_GAUGE(
  serenedb_scheduler_maintenance_prio_queue_length, uint64_t,
  "Current queue length of the maintenance priority queue in the scheduler");

Scheduler::Scheduler(SerenedServer& server, uint64_t min_threads,
                     uint64_t max_threads, uint64_t max_queue_size,
                     uint64_t fifo1_size, uint64_t fifo2_size,
                     uint64_t fifo3_size, uint64_t ongoing_low_priority_limit,
                     double unavailability_queue_fill_grade,
                     metrics::MetricsFeature& metrics)
  : _server{server},
    _min_threads{min_threads},
    _max_threads{max_threads},
    _max_fifo_sizes{max_queue_size, fifo1_size, fifo2_size, fifo3_size},
    _ongoing_low_priority_limit{ongoing_low_priority_limit},
    _unavailability_queue_fill_grade{unavailability_queue_fill_grade},
    _metrics_handler_tasks_created(
      metrics.add(serenedb_scheduler_handler_tasks_created_total{})),
    _metrics_queue_time_violations(
      metrics.add(serenedb_scheduler_queue_time_violations_total{})),
    _ongoing_low_priority_gauge(
      metrics.add(serenedb_scheduler_ongoing_low_prio{})),
    _metrics_last_low_priority_dequeue_time(
      metrics.add(serenedb_scheduler_low_prio_queue_last_dequeue_time{})),
    _metrics_queue_lengths{
      metrics.add(serenedb_scheduler_maintenance_prio_queue_length{}),
      metrics.add(serenedb_scheduler_high_prio_queue_length{}),
      metrics.add(serenedb_scheduler_medium_prio_queue_length{}),
      metrics.add(serenedb_scheduler_low_prio_queue_length{})} {
  SDB_ASSERT(ToQueueNo(std::to_underlying(RequestPriority::Maintenance)) == 0);
  SDB_ASSERT(ToQueueNo(std::to_underlying(RequestPriority::High)) == 1);
  SDB_ASSERT(ToQueueNo(std::to_underlying(RequestPriority::Med)) == 2);
  SDB_ASSERT(ToQueueNo(std::to_underlying(RequestPriority::Low)) == 3);
}

Scheduler::~Scheduler() = default;

void Scheduler::JobObserver::taskEnqueued(
  const folly::ThreadPoolExecutor::TaskInfo& info) noexcept {
  const auto queue_no = ToQueueNo(info.priority);
  _scheduler._metrics_queue_lengths[queue_no].get() += 1;
}

void Scheduler::JobObserver::taskDequeued(
  const folly::ThreadPoolExecutor::DequeuedTaskInfo& info) noexcept {
  if (info.priority == std::to_underlying(RequestPriority::Low)) {
    _scheduler.setLastLowPriorityDequeueTime(
      std::chrono::duration_cast<std::chrono::milliseconds>(info.waitTime)
        .count());
  }
  const auto queue_no = ToQueueNo(info.priority);
  _scheduler._metrics_queue_lengths[queue_no].get() -= 1;
}

void Scheduler::JobObserver::taskProcessed(
  const folly::ThreadPoolExecutor::ProcessedTaskInfo& info) noexcept {}

bool Scheduler::start() {
  _executor_handle = std::make_unique<folly::CPUThreadPoolExecutor>(
    std::pair{_max_threads, _min_threads},
    folly::CPUThreadPoolExecutor::makeLifoSemPriorityQueue(4));
  _executor_handle->addTaskObserver(std::make_unique<JobObserver>(*this));
  _cron_thread = std::make_unique<SchedulerCronThread>(_server, *this);
  return _cron_thread->start();
}

void Scheduler::shutdown() {
  _executor_handle->join();

  _cron_queue_mutex.Lock();
  _cron_stopping = true;
  _croncv.notify_one();
  _cron_queue_mutex.Unlock();

  _cron_thread.reset();

#ifdef SDB_DEV
  // At this point the cron thread has been stopped
  // And there will be no other people posting on the queue
  // Lets make sure that all items on the queue are disabled
  while (!_cron_queue.empty()) {
    const auto& top = _cron_queue.top();
    auto item = top.second.lock();
    if (item) {
      SDB_ASSERT(item->isDisabled(), item->name());
    }
    _cron_queue.pop();
  }
#endif

  _executor_handle = {};
}

void Scheduler::runCronThread() {
  std::unique_lock guard{_cron_queue_mutex};

  while (!_cron_stopping) {
    auto now = clock::now();
    clock::duration sleep_time = std::chrono::milliseconds(50);

    while (!_cron_queue.empty()) {
      // top is a reference to a tuple containing the timepoint and a shared_ptr
      // to the work item
      auto top = _cron_queue.top();
      if (top.first < now) {
        _cron_queue.pop();
        guard.unlock();

        // It is time to schedule this task, try to get the lock and obtain a
        // shared_ptr If this fails a default DelayedWorkItem is constructed
        // which has disabled == true
        try {
          auto item = top.second.lock();
          if (item) {
            item->run();
          }
        } catch (const std::exception& ex) {
          SDB_WARN(GENERAL,
                   "caught exception in runCronThread: ", ex.what());
        }

        // always lock again, as we are going into the wait_for below
        guard.lock();

      } else {
        auto then = (top.first - now);

        sleep_time = (sleep_time > then ? then : sleep_time);
        break;
      }
    }

    _croncv.WaitWithTimeout(&_cron_queue_mutex, absl::FromChrono(sleep_time));
  }
}

void Scheduler::queue(RequestPriority prio, folly::Func func) noexcept {
  if (prio == RequestPriority::Low) {
    func = [this, func = std::move(func)] mutable {
      const bool needs_reschedule = [&] {
        if (_ongoing_low_priority_limit == 0) {
          return false;
        }
        const auto ongoing = _ongoing_low_priority_gauge.load();
        if (ongoing < _ongoing_low_priority_limit) {
          return false;
        }
        // Cluster-RPC saturation backpressure was the old reason to refuse
        // a dequeue here; without a cluster RPC pool the only backpressure
        // is the ongoing-low-priority-limit checked above.
        return false;
      }();
      if (needs_reschedule) {
        queue(RequestPriority::Low, std::move(func));
      } else {
        func();
      }
    };
  } else if (prio == RequestPriority::Med) {
    SDB_IF_FAILURE("BlockSchedulerMediumQueue") {
      func = [this, func = std::move(func)] mutable {
        queue(RequestPriority::Med, std::move(func));
      };
    }
  }
  _executor_handle->addWithPriority(std::move(func), std::to_underlying(prio));
}

void Scheduler::queue(RequestLane lane, folly::Func func) noexcept {
  queue(PriorityRequestLane(lane), std::move(func));
}

bool Scheduler::tryBoundedQueue(RequestLane lane, folly::Func func) noexcept {
  const auto queue_no =
    ToQueueNo(std::to_underlying(PriorityRequestLane(lane)));

  const auto max_size = _max_fifo_sizes[queue_no];
  auto& num_counted_items = _queues[queue_no].num_counted_items;
  if (num_counted_items.fetch_add(1, std::memory_order_relaxed) > max_size) {
    num_counted_items.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }

  queue(lane, [func = std::move(func), &num_counted_items] mutable {
    num_counted_items.fetch_sub(1, std::memory_order_relaxed);
    func();
  });
  return true;
}

Scheduler::WorkHandle Scheduler::queueDelayed(
  std::string_view name, RequestLane lane, clock::duration delay,
  absl::AnyInvocable<void(bool cancelled)> handler) noexcept try {
  std::shared_ptr<DelayedWorkItem> item;

  try {
    SDB_IF_FAILURE("Scheduler::queueDelayedFail1") { SDB_THROW(sdb::ERROR_DEBUG); }

    item =
      std::make_shared<DelayedWorkItem>(name, std::move(handler), lane, this);
  } catch (...) {
    return nullptr;
  }

  try {
    SDB_IF_FAILURE("Scheduler::queueDelayedFail2") { SDB_THROW(sdb::ERROR_DEBUG); }
    auto point = clock::now() + delay;
    absl::MutexLock guard{&_cron_queue_mutex};
    _cron_queue.emplace(point, item);
    if (delay < std::chrono::milliseconds(50)) {
      // wakeup thread
      _croncv.notify_one();
    }
  } catch (...) {
    // if emplacing throws, we can cancel the item directly
    item->cancel();
    return nullptr;
  }

  return item;
} catch (...) {
  return nullptr;
}

yaclib::Future<> Scheduler::delay(std::string_view name, clock::duration d) {
  if (d == clock::duration::zero()) {
    return yaclib::MakeFuture();
  }

  auto [f, p] = yaclib::MakeContract<bool>();

  auto item = queueDelayed(name, RequestLane::DelayedFuture, d,
                           [pr = std::move(p)](bool cancelled) mutable {
                             std::move(pr).Set(cancelled);
                           });

  if (item == nullptr) {
    return yaclib::MakeFuture();
  }

  return std::move(f).ThenInline([item = std::move(item)](bool cancelled) {
    if (cancelled) {
      throw std::logic_error("delay was cancelled");
    }
  });
}

void Scheduler::trackCreateHandlerTask() noexcept {
  ++_metrics_handler_tasks_created;
}

void Scheduler::trackBeginOngoingLowPriorityTask() noexcept {
  ++_ongoing_low_priority_gauge;
}

void Scheduler::trackEndOngoingLowPriorityTask() noexcept {
  --_ongoing_low_priority_gauge;
}

void Scheduler::trackQueueTimeViolation() { ++_metrics_queue_time_violations; }

uint64_t Scheduler::getLastLowPriorityDequeueTime() const noexcept {
  return _metrics_last_low_priority_dequeue_time.load();
}

void Scheduler::setLastLowPriorityDequeueTime(uint64_t time) noexcept {
  _metrics_last_low_priority_dequeue_time = time;
}

std::pair<uint64_t, uint64_t> Scheduler::getNumberLowPrioOngoingAndQueued()
  const {
  return {_ongoing_low_priority_gauge.load(std::memory_order_relaxed),
          _metrics_queue_lengths.back().get().load(std::memory_order_relaxed)};
}

double Scheduler::approximateQueueFillGrade() const {
  const auto max_length = _max_fifo_sizes[kNumberOfQueues - 1];
  const auto q_length =
    _metrics_queue_lengths[kNumberOfQueues - 1].get().load();
  return static_cast<double>(q_length) / static_cast<double>(max_length);
}

double Scheduler::unavailabilityQueueFillGrade() const {
  return _unavailability_queue_fill_grade;
}

Scheduler* GetScheduler() noexcept {
  return lifecycle::IsStopping() ? nullptr : SchedulerFeature::gScheduler;
}

}  // namespace sdb
