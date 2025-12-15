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

#include <absl/base/thread_annotations.h>

#include <array>
#include <initializer_list>
#include <string>
#include <string_view>

#include "basics/cpu_usage_snapshot.h"
#include "basics/result.h"
#include "basics/system-functions.h"
#include "metrics/fwd.h"
#include "rest/common_defines.h"
#include "rest_server/serened.h"
#include "statistics/descriptions.h"
#include "statistics/figures.h"

namespace vpack {

class Builder;

}  // namespace vpack
namespace sdb {
namespace stats {

class Descriptions;

}  // namespace stats

class Thread;
class StatisticsWorker;

namespace statistics {

inline constexpr auto kBytesReceivedDistributionCuts =
  std::initializer_list{250.0, 1000.0, 2000.0, 5000.0, 10000.0};
inline constexpr auto kBytesSentDistributionCuts =
  std::initializer_list{250.0, 1000.0, 2000.0, 5000.0, 10000.0};
inline constexpr auto kConnectionTimeDistributionCuts =
  std::initializer_list{0.1, 1.0, 60.0};
inline constexpr auto kRequestTimeDistributionCuts =
  std::initializer_list{0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 15.0, 30.0};

extern Counter gAsyncRequests;
extern Counter gHttpConnections;
extern Counter gTotalRequests;
extern Counter gTotalRequestsSuperuser;
extern Counter gTotalRequestsUser;

constexpr size_t kMethodRequestsStatisticsSize =
  ((size_t)sdb::rest::RequestType::Illegal) + 1;
using MethodRequestCounters =
  std::array<Counter, kMethodRequestsStatisticsSize>;
extern MethodRequestCounters gMethodRequests;
extern Distribution gConnectionTimeDistribution;

struct RequestFigures {
  RequestFigures();

  RequestFigures(const RequestFigures&) = delete;
  RequestFigures(RequestFigures&&) = delete;
  RequestFigures& operator=(const RequestFigures&) = delete;
  RequestFigures& operator=(RequestFigures&&) = delete;

  Distribution bytes_received_distribution;
  Distribution bytes_sent_distribution;
  Distribution io_time_distribution;
  Distribution queue_time_distribution;
  Distribution request_time_distribution;
  Distribution total_time_distribution;
};
extern RequestFigures gSuperuserRequestFigures;
extern RequestFigures gUserRequestFigures;
}  // namespace statistics

class CpuUsage final {
 public:
  CpuUsage();
  ~CpuUsage();
  CpuUsageSnapshot snapshot();

 private:
  struct SnapshotProvider;

  std::unique_ptr<SnapshotProvider> _snapshot_provider;
  absl::Mutex _snapshot_mutex;
  CpuUsageSnapshot _snapshot ABSL_GUARDED_BY(_snapshot_mutex);
  CpuUsageSnapshot _snapshot_delta ABSL_GUARDED_BY(_snapshot_mutex);
  bool _update_in_progress ABSL_GUARDED_BY(_snapshot_mutex) = false;
};

class StatisticsFeature final : public SerenedFeature {
 public:
  static double time() { return utilities::GetMicrotime(); }

 public:
  static constexpr std::string_view name() noexcept { return "Statistics"; }

  explicit StatisticsFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void start() final;
  void stop() final;
  void toPrometheus(std::string& result, double now, std::string_view globals,
                    bool ensure_whitespace);

  const stats::Descriptions& descriptions() const { return _descriptions; }

  static vpack::Builder fillDistribution(const statistics::Distribution& dist);

  static void appendMetric(std::string& result, const std::string& val,
                           const std::string& label, bool ensure_whitespace);

 private:
  static void appendMetric(std::string& result, const std::string& val,
                           const std::string& label, std::string_view globals,
                           bool ensure_whitespace);

  static void appendHistogram(std::string& result,
                              const statistics::Distribution& dist,
                              const std::string& label,
                              const std::initializer_list<std::string>& les,
                              bool is_integer, std::string_view globals,
                              bool ensure_whitespace);
  bool _statistics;

  CpuUsage _cpu_usage;
  stats::Descriptions _descriptions;
  std::unique_ptr<Thread> _statistics_thread;
  metrics::Gauge<uint64_t>& _request_statistics_memory_usage;
  metrics::Gauge<uint64_t>& _connection_statistics_memory_usage;
};

}  // namespace sdb
