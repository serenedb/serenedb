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

#include <functional>
#include <memory>

#include "general_server/scheduler.h"
#include "rest_server/serened.h"

namespace sdb {

class SchedulerFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Scheduler"; }

  inline static Scheduler* gScheduler = nullptr;

  SchedulerFeature(Server& server);
  ~SchedulerFeature() final;

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void start() final;
  void stop() final;
  void unprepare() final;

  // -------------------------------------------------------------------------
  // UNRELATED SECTION STARTS HERE: Signals and other things crept into Sched
  // -------------------------------------------------------------------------
  void buildControlCHandler();
  void buildHangupHandler();

  uint64_t maximalThreads() const noexcept;

 private:
  void signalStuffInit();
  void signalStuffDeinit();

  uint64_t _nr_minimal_threads = 4;
  uint64_t _nr_maximal_threads = 0;
  uint64_t _queue_size = 4096;
  uint64_t _fifo1_size = 4096;
  uint64_t _fifo2_size = 4096;
  uint64_t _fifo3_size = 4096;
  double _ongoing_low_priority_multiplier = 4.0;
  double _unavailability_queue_fill_grade = 0.75;

  std::unique_ptr<Scheduler> _scheduler;
  metrics::MetricsFeature& _metrics_feature;

  struct AsioHandler;
  std::unique_ptr<AsioHandler> _asio_handler;
};

}  // namespace sdb
