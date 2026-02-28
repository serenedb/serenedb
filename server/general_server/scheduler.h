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

#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <atomic>
#include <chrono>
#include <function2.hpp>
#include <functional>
#include <queue>
#include <string_view>
#include <utility>
#include <yaclib/async/contract.hpp>
#include <yaclib/async/make.hpp>

#include "basics/exceptions.h"
#include "basics/system-compiler.h"
#include "general_server/request_lane.h"
#include "metrics/fwd.h"
#include "rest_server/serened.h"
#include "utils/coro_helper.h"

namespace sdb {

class SchedulerCronThread;

class Scheduler {
 public:
  explicit Scheduler(SerenedServer& server, uint64_t min_threads,
                     uint64_t max_threads, uint64_t max_queue_size,
                     uint64_t fifo1_size, uint64_t fifo2_size,
                     uint64_t fifo3_size, uint64_t ongoing_low_priority_limit,
                     double unavailability_queue_fill_grade,
                     metrics::MetricsFeature& metrics);

  ~Scheduler();

  // ---------------------------------------------------------------------------
  // Scheduling and Task Queuing - the relevant stuff
  // ---------------------------------------------------------------------------
  class DelayedWorkItem;
  using clock = std::chrono::steady_clock;
  using WorkHandle = std::shared_ptr<DelayedWorkItem>;

  // push an item onto the queue. does not indicate success or failure
  // by returning a value, but will throw if the item could not be pushed
  // onto the queue. as the function is marked noexcept, this will also
  // lead to the program aborting with std::terminate.
  void queue(RequestLane lane, folly::Func func) noexcept;

  template<typename Func, typename R = std::invoke_result_t<Func>>
  yaclib::Future<R> queueWithFuture(RequestLane lane, Func&& func) {
    auto [f, p] = yaclib::MakeContract<R>();
    queue(lane, [p = std::move(p), func = std::forward<Func>(func)] mutable {
      std::move(p).Set(std::forward<Func>(func)());
    });
    return std::move(f);
  }

  // push an item onto the queue. indicates success or failure by returning
  // a boolean value (true = queueing successful, false = queueing failed)
  [[nodiscard]] bool tryBoundedQueue(RequestLane lane,
                                     folly::Func func) noexcept;

  // Enqueues a task after delay - this uses the queue functions above.
  // WorkHandle is a shared_ptr to a DelayedWorkItem. If all references the
  // DelayedWorkItem are dropped, the task is canceled.
  [[nodiscard]] WorkHandle queueDelayed(
    std::string_view name, RequestLane lane, clock::duration delay,
    absl::AnyInvocable<void(bool canceled)> handler) noexcept;

  // Returns the scheduler's server object
  SerenedServer& server() noexcept { return _server; }

  class DelayedWorkItem {
   public:
    ~DelayedWorkItem() noexcept try { cancel(); } catch (...) {
    }

    void run() { executeWithCancel(false); }

    void cancel() { executeWithCancel(true); }

    explicit DelayedWorkItem(std::string_view name,
                             absl::AnyInvocable<void(bool canceled)>&& func,
                             RequestLane lane, Scheduler* scheduler)
      : _name{name},
        _func{std::move(func)},
        _lane{lane},
        _scheduler{scheduler} {}

    std::string_view name() const noexcept { return _name; }

   private:
    void executeWithCancel(bool arg) {
      if (auto* scheduler =
            _scheduler.exchange(nullptr, std::memory_order_acq_rel)) {
        scheduler->queue(_lane,
                         [func = std::move(_func), arg] mutable { func(arg); });
      }
    }

#ifdef SDB_DEV
    bool isDisabled() const {
      return _scheduler.load(std::memory_order_acquire) == nullptr;
    }
    friend class Scheduler;
#endif

    std::string_view _name;
    absl::AnyInvocable<void(bool)> _func;
    RequestLane _lane;
    std::atomic<Scheduler*> _scheduler;
  };

  // delay Future. returns a future that will be fulfilled after the given
  // duration expires. If d is zero or we cannot post the future
  // to the scheduler, the future is fulfilled immediately.
  // Throws a logic error if delay was cancelled.
  yaclib::Future<> delay(std::string_view name, clock::duration d);

  bool start();
  void shutdown();

  void trackCreateHandlerTask() noexcept;
  void trackBeginOngoingLowPriorityTask() noexcept;
  void trackEndOngoingLowPriorityTask() noexcept;

  void trackQueueTimeViolation();

  /// returns the last stored dequeue time [ms]
  uint64_t getLastLowPriorityDequeueTime() const noexcept;

  /// set the time it took for the last low prio item to be dequeued
  /// (time between queuing and dequeing) [ms]
  void setLastLowPriorityDequeueTime(uint64_t time) noexcept;

  /// get information about low prio queue:
  std::pair<uint64_t, uint64_t> getNumberLowPrioOngoingAndQueued() const;

  /// approximate fill grade of the scheduler's queue (in %)
  double approximateQueueFillGrade() const;

  /// fill grade of the scheduler's queue (in %) from which onwards
  /// the server is considered unavailable (because of overload)
  double unavailabilityQueueFillGrade() const;

  folly::CPUThreadPoolExecutor& GetCPUExecutor() const noexcept {
    SDB_ASSERT(_executor_handle);
    return *_executor_handle;
  }

 private:
  void queue(RequestPriority prio, folly::Func func) noexcept;

  static constexpr const uint64_t kNumberOfQueues = 4;

  SerenedServer& _server;
  NetworkFeature& _nf;
  const uint64_t _min_threads;
  const uint64_t _max_threads;
  std::unique_ptr<folly::CPUThreadPoolExecutor> _executor_handle;

  static size_t ToQueueNo(int8_t priority) noexcept {
    const auto queue_no = -(priority - 1);
    SDB_ASSERT(queue_no < kNumberOfQueues);
    return queue_no;
  }

  class JobObserver : public folly::ThreadPoolExecutor::TaskObserver {
   public:
    explicit JobObserver(Scheduler& scheduler) noexcept
      : _scheduler{scheduler} {}

    void taskEnqueued(
      const folly::ThreadPoolExecutor::TaskInfo& info) noexcept final;
    void taskDequeued(
      const folly::ThreadPoolExecutor::DequeuedTaskInfo& info) noexcept final;
    void taskProcessed(
      const folly::ThreadPoolExecutor::ProcessedTaskInfo& info) noexcept final;

   private:
    friend class Scheduler;

    Scheduler& _scheduler;
  };

  struct {
    /// the number of items that have been enqueued via tryBoundedQueue
    /// Items that are added via an unbounded queue operation are not counted!
    std::atomic<uint64_t> num_counted_items{0};
  } _queues[kNumberOfQueues];
  const uint64_t _max_fifo_sizes[kNumberOfQueues];
  const uint64_t _ongoing_low_priority_limit;
  /// fill grade of the scheduler's queue (in %) from which onwards
  /// the server is considered unavailable (because of overload)
  const double _unavailability_queue_fill_grade;
  metrics::Counter& _metrics_handler_tasks_created;
  metrics::Counter& _metrics_queue_time_violations;
  metrics::Gauge<uint64_t>& _ongoing_low_priority_gauge;
  /// amount of time it took for the last low prio item to be dequeued
  /// (time between queuing and dequeing) [ms].
  /// this metric is only updated probabilistically
  metrics::Gauge<uint64_t>& _metrics_last_low_priority_dequeue_time;
  std::array<std::reference_wrapper<metrics::Gauge<uint64_t>>, kNumberOfQueues>
    _metrics_queue_lengths;

  // ---------------------------------------------------------------------------
  // CronThread and delayed tasks
  // ---------------------------------------------------------------------------
  // The priority queue is managed by a CronThread. It wakes up on a regular
  // basis (10ms currently) and looks at queue.top(). It the _expire time is
  // smaller than now() and the task is not canceled it is posted on the
  // scheduler. The next sleep time is computed depending on queue top.
  //
  // Note that tasks that have a delay of less than 1ms are posted directly.
  // For tasks above 50ms the CronThread is woken up to potentially update its
  // sleep time, which could now be shorter than before.

  // Entry point for the CronThread
  void runCronThread();
  friend class SchedulerCronThread;

  typedef std::pair<clock::time_point, std::weak_ptr<DelayedWorkItem>>
    CronWorkItem;

  struct CronWorkItemCompare {
    bool operator()(const CronWorkItem& left, const CronWorkItem& right) const {
      // Reverse order, because std::priority_queue is a max heap.
      return right.first < left.first;
    }
  };

  std::priority_queue<CronWorkItem, std::vector<CronWorkItem>,
                      CronWorkItemCompare>
    _cron_queue;
  bool _cron_stopping{false};
  absl::Mutex _cron_queue_mutex;
  absl::CondVar _croncv;
  std::unique_ptr<SchedulerCronThread> _cron_thread;
};

Scheduler* GetScheduler() noexcept;

}  // namespace sdb
