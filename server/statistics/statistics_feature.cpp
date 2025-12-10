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

#include "statistics_feature.h"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>

#include <chrono>
#include <initializer_list>
#include <thread>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/physical_memory.h"
#include "basics/process-utils.h"
#include "basics/thread.h"
#include "metrics/builder.h"
#include "metrics/counter_builder.h"
#include "metrics/fix_scale.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/metric.h"
#include "metrics/metrics_feature.h"
#include "network/network_feature.h"
#include "rest_server/cpu_usage_feature.h"
#include "rest_server/upgrade_feature.h"
#include "statistics/connection_statistics.h"
#include "statistics/descriptions.h"
#include "statistics/request_statistics.h"
#include "statistics/server_statistics.h"

using namespace sdb;
using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;
using namespace sdb::statistics;

namespace sdb {
namespace statistics {

struct BytesReceivedScale {
  static metrics::FixScale<double> scale() {
    return {250, 10000, kBytesReceivedDistributionCuts};
  }
};

struct BytesSentScale {
  static metrics::FixScale<double> scale() {
    return {250, 10000, kBytesSentDistributionCuts};
  }
};

struct ConnectionTimeScale {
  static metrics::FixScale<double> scale() {
    return {0.1, 60.0, kConnectionTimeDistributionCuts};
  }
};

struct RequestTimeScale {
  static metrics::FixScale<double> scale() {
    return {0.01, 30.0, kRequestTimeDistributionCuts};
  }
};

DECLARE_HISTOGRAM(serenedb_client_connection_statistics_bytes_received,
                  BytesReceivedScale, "Bytes received for requests");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_bytes_sent,
                  BytesSentScale, "Bytes sent for responses");
DECLARE_HISTOGRAM(serenedb_client_user_connection_statistics_bytes_received,
                  BytesReceivedScale,
                  "Bytes received for requests, only user traffic");
DECLARE_HISTOGRAM(serenedb_client_user_connection_statistics_bytes_sent,
                  BytesSentScale,
                  "Bytes sent for responses, only user traffic");
DECLARE_COUNTER(
  serenedb_process_statistics_minor_page_faults_total,
  "The number of minor faults the process has made which have not required "
  "loading a memory page from disk");
DECLARE_COUNTER(serenedb_process_statistics_major_page_faults_total,
                "This figure contains the number of major faults the process "
                "has made which have required loading a memory page from disk");
DECLARE_GAUGE(serenedb_process_statistics_user_time, double,
              "Amount of time that this process has been scheduled in user "
              "mode, measured in seconds");
DECLARE_GAUGE(serenedb_process_statistics_system_time, double,
              "Amount of time that this process has been scheduled in kernel "
              "mode, measured in seconds");
DECLARE_GAUGE(serenedb_process_statistics_number_of_threads, double,
              "Number of threads in the serened process");
DECLARE_GAUGE(
  serenedb_process_statistics_resident_set_size, double,
  "The total size of the number of pages the process has in real memory. "
  "This is just the pages which count toward text, data, or stack space. "
  "This does not include pages which have not been demand-loaded in, or "
  "which are swapped out. The resident set size is reported in bytes");
DECLARE_GAUGE(serenedb_process_statistics_resident_set_size_percent, double,
              "The relative size of the number of pages the process has in "
              "real memory compared to system memory. This is just the pages "
              "which count toward text, data, or stack space. This does not "
              "include pages which have not been demand-loaded in, or which "
              "are swapped out. The value is a ratio between 0.00 and 1.00");
DECLARE_GAUGE(
  serenedb_process_statistics_virtual_memory_size, double,
  "This figure contains The size of the virtual memory the process is using");
DECLARE_GAUGE(serenedb_client_connection_statistics_client_connections, double,
              "The number of client connections that are currently open");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_connection_time,
                  ConnectionTimeScale, "Total connection time of a client");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_total_time,
                  ConnectionTimeScale, "Total time needed to answer a request");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_request_time,
                  RequestTimeScale, "Request time needed to answer a request");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_queue_time,
                  RequestTimeScale, "Queue time needed to answer a request");
DECLARE_HISTOGRAM(serenedb_client_connection_statistics_io_time,
                  RequestTimeScale, "IO time needed to answer a request");
DECLARE_COUNTER(serenedb_http_request_statistics_total_requests_total,
                "Total number of HTTP requests");
DECLARE_COUNTER(serenedb_http_request_statistics_superuser_requests_total,
                "Total number of HTTP requests executed by superuser/JWT");
DECLARE_COUNTER(serenedb_http_request_statistics_user_requests_total,
                "Total number of HTTP requests executed by clients");
DECLARE_COUNTER(serenedb_http_request_statistics_async_requests_total,
                "Number of asynchronously executed HTTP requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_delete_requests_total,
                "Number of HTTP DELETE requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_get_requests_total,
                "Number of HTTP GET requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_head_requests_total,
                "Number of HTTP HEAD requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_options_requests_total,
                "Number of HTTP OPTIONS requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_patch_requests_total,
                "Number of HTTP PATCH requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_post_requests_total,
                "Number of HTTP POST requests");
DECLARE_COUNTER(serenedb_http_request_statistics_http_put_requests_total,
                "Number of HTTP PUT requests");
DECLARE_COUNTER(serenedb_http_request_statistics_other_http_requests_total,
                "Number of other HTTP requests");
DECLARE_COUNTER(serenedb_server_statistics_server_uptime_total,
                "Number of seconds elapsed since server start");
DECLARE_GAUGE(serenedb_server_statistics_physical_memory, double,
              "Physical memory in bytes");
DECLARE_GAUGE(serenedb_server_statistics_cpu_cores, double,
              "Number of CPU cores visible to the serened process");
DECLARE_GAUGE(
  serenedb_server_statistics_user_percent, double,
  "Percentage of time that the system CPUs have spent in user mode");
DECLARE_GAUGE(
  serenedb_server_statistics_system_percent, double,
  "Percentage of time that the system CPUs have spent in kernel mode");
DECLARE_GAUGE(serenedb_server_statistics_idle_percent, double,
              "Percentage of time that the system CPUs have been idle");
DECLARE_GAUGE(
  serenedb_server_statistics_iowait_percent, double,
  "Percentage of time that the system CPUs have been waiting for I/O");
DECLARE_GAUGE(serenedb_request_statistics_memory_usage, uint64_t,
              "Memory used by the internal request statistics");
DECLARE_GAUGE(serenedb_connection_statistics_memory_usage, uint64_t,
              "Memory used by the internal connection statistics");

namespace {
// local_name: {"prometheus_name", "type", "help"}
const auto kStatStrings =
  std::map<std::string_view, std::vector<std::string_view>>{
    {"bytesReceived",
     {"serenedb_client_connection_statistics_bytes_received", "histogram",
      "Bytes received for requests"}},
    {"bytesSent",
     {"serenedb_client_connection_statistics_bytes_sent", "histogram",
      "Bytes sent for responses"}},
    {"bytesReceivedUser",
     {"serenedb_client_user_connection_statistics_bytes_received", "histogram",
      "Bytes received for requests, only user traffic"}},
    {"bytesSentUser",
     {"serenedb_client_user_connection_statistics_bytes_sent", "histogram",
      "Bytes sent for responses, only user traffic"}},
    {"minorPageFaults",
     {"serenedb_process_statistics_minor_page_faults_total", "counter",
      "The number of minor faults the process has made which have not required "
      "loading a memory page from disk"}},
    {"majorPageFaults",
     {"serenedb_process_statistics_major_page_faults_total", "counter",
      "This figure contains the number of major faults the "
      "process has made which have required loading a memory page from disk"}},
    {"userTime",
     {"serenedb_process_statistics_user_time", "gauge",
      "Amount of time that this process has been scheduled in user mode, "
      "measured in seconds"}},
    {"systemTime",
     {"serenedb_process_statistics_system_time", "gauge",
      "Amount of time that this process has been scheduled in kernel mode, "
      "measured in seconds"}},
    {"numberOfThreads",
     {"serenedb_process_statistics_number_of_threads", "gauge",
      "Number of threads in the serened process"}},
    {"residentSize",
     {"serenedb_process_statistics_resident_set_size", "gauge",
      "The total size of the number of pages the process has in real memory. "
      "This is just the pages which count toward text, data, or stack space. "
      "This does not include pages which have not been demand-loaded in, or "
      "which are swapped out. The resident set size is reported in bytes"}},
    {"residentSizePercent",
     {"serenedb_process_statistics_resident_set_size_percent", "gauge",
      "The relative size of the number of pages the process has in real memory "
      "compared to system memory. This is just the pages which count toward "
      "text, data, or stack space. This does not include pages which have not "
      "been demand-loaded in, or which are swapped out. The value is a ratio "
      "between 0.00 and 1.00"}},
    {"virtualSize",
     {"serenedb_process_statistics_virtual_memory_size", "gauge",
      "This figure contains The size of the virtual memory the process is "
      "using"}},
    {"clientHttpConnections",
     {"serenedb_client_connection_statistics_client_connections", "gauge",
      "The number of client connections that are currently open"}},
    {"connectionTime",
     {"serenedb_client_connection_statistics_connection_time", "histogram",
      "Total connection time of a client"}},
    {"connectionTimeCount",
     {"serenedb_client_connection_statistics_connection_time_count", "gauge",
      "Total connection time of a client"}},
    {"connectionTimeSum",
     {"serenedb_client_connection_statistics_connection_time_sum", "gauge",
      "Total connection time of a client"}},
    {"totalTime",
     {"serenedb_client_connection_statistics_total_time", "histogram",
      "Total time needed to answer a request"}},
    {"totalTimeCount",
     {"serenedb_client_connection_statistics_total_time_count", "gauge",
      "Total time needed to answer a request"}},
    {"totalTimeSum",
     {"serenedb_client_connection_statistics_total_time_sum", "gauge",
      "Total time needed to answer a request"}},
    {"requestTime",
     {"serenedb_client_connection_statistics_request_time", "histogram",
      "Request time needed to answer a request"}},
    {"requestTimeCount",
     {"serenedb_client_connection_statistics_request_time_count", "gauge",
      "Request time needed to answer a request"}},
    {"requestTimeSum",
     {"serenedb_client_connection_statistics_request_time_sum", "gauge",
      "Request time needed to answer a request"}},
    {"queueTime",
     {"serenedb_client_connection_statistics_queue_time", "histogram",
      "Request time needed to answer a request"}},
    {"queueTimeCount",
     {"serenedb_client_connection_statistics_queue_time_count", "gauge",
      "Request time needed to answer a request"}},
    {"queueTimeSum",
     {"serenedb_client_connection_statistics_queue_time_sum", "gauge",
      "Request time needed to answer a request"}},
    {"ioTime",
     {"serenedb_client_connection_statistics_io_time", "histogram",
      "Request time needed to answer a request"}},
    {"ioTimeCount",
     {"serenedb_client_connection_statistics_io_time_count", "gauge",
      "Queue time needed to answer a request"}},
    {"ioTimeSum",
     {"serenedb_client_connection_statistics_io_time_sum", "gauge",
      "IO time needed to answer a request"}},
    {"httpReqsTotal",
     {"serenedb_http_request_statistics_total_requests_total", "counter",
      "Total number of HTTP requests"}},
    {"httpReqsSuperuser",
     {"serenedb_http_request_statistics_superuser_requests_total", "counter",
      "Total number of HTTP requests executed by superuser/JWT"}},
    {"httpReqsUser",
     {"serenedb_http_request_statistics_user_requests_total", "counter",
      "Total number of HTTP requests executed by clients"}},
    {"httpReqsAsync",
     {"serenedb_http_request_statistics_async_requests_total", "counter",
      "Number of asynchronously executed HTTP requests"}},
    {"httpReqsDelete",
     {"serenedb_http_request_statistics_http_delete_requests_total", "counter",
      "Number of HTTP DELETE requests"}},
    {"httpReqsGet",
     {"serenedb_http_request_statistics_http_get_requests_total", "counter",
      "Number of HTTP GET requests"}},
    {"httpReqsHead",
     {"serenedb_http_request_statistics_http_head_requests_total", "counter",
      "Number of HTTP HEAD requests"}},
    {"httpReqsOptions",
     {"serenedb_http_request_statistics_http_options_requests_total", "counter",
      "Number of HTTP OPTIONS requests"}},
    {"httpReqsPatch",
     {"serenedb_http_request_statistics_http_patch_requests_total", "counter",
      "Number of HTTP PATCH requests"}},
    {"httpReqsPost",
     {"serenedb_http_request_statistics_http_post_requests_total", "counter",
      "Number of HTTP POST requests"}},
    {"httpReqsPut",
     {"serenedb_http_request_statistics_http_put_requests_total", "counter",
      "Number of HTTP PUT requests"}},
    {"httpReqsOther",
     {"serenedb_http_request_statistics_other_http_requests_total", "counter",
      "Number of other HTTP requests"}},
    {"uptime",
     {"serenedb_server_statistics_server_uptime_total", "counter",
      "Number of seconds elapsed since server start"}},
    {"physicalSize",
     {"serenedb_server_statistics_physical_memory", "gauge",
      "Physical memory in bytes"}},
    {"cores",
     {"serenedb_server_statistics_cpu_cores", "gauge",
      "Number of CPU cores visible to the serened process"}},
    {"userPercent",
     {"serenedb_server_statistics_user_percent", "gauge",
      "Percentage of time that the system CPUs have spent in user mode"}},
    {"systemPercent",
     {"serenedb_server_statistics_system_percent", "gauge",
      "Percentage of time that the system CPUs have spent in kernel mode"}},
    {"idlePercent",
     {"serenedb_server_statistics_idle_percent", "gauge",
      "Percentage of time that the system CPUs have been idle"}},
    {"iowaitPercent",
     {"serenedb_server_statistics_iowait_percent", "gauge",
      "Percentage of time that the system CPUs have been waiting for I/O"}},
  };

// Connect legacy statistics with metrics definitions for automatic checks
#ifdef SDB_DEV
using StatBuilder =
  containers::FlatHashMap<std::string_view,
                          std::unique_ptr<const metrics::Builder>>;
auto MakeStatBuilder(std::initializer_list<
                     std::pair<std::string_view, const metrics::Builder* const>>
                       init_list) -> StatBuilder {
  auto unomap = StatBuilder{};
  unomap.reserve(init_list.size());
  for (const auto& it : init_list) {
    unomap.emplace(it.first, it.second);
  }
  return unomap;
}
const auto kStatBuilder = MakeStatBuilder({
  {"bytesReceived", new serenedb_client_connection_statistics_bytes_received()},
  {"bytesSent", new serenedb_client_connection_statistics_bytes_sent()},
  {"bytesReceivedUser",
   new serenedb_client_user_connection_statistics_bytes_received()},
  {"bytesSentUser",
   new serenedb_client_user_connection_statistics_bytes_sent()},
  {"minorPageFaults",
   new serenedb_process_statistics_minor_page_faults_total()},
  {"majorPageFaults",
   new serenedb_process_statistics_major_page_faults_total()},
  {"userTime", new serenedb_process_statistics_user_time()},
  {"systemTime", new serenedb_process_statistics_system_time()},
  {"numberOfThreads", new serenedb_process_statistics_number_of_threads()},
  {"residentSize", new serenedb_process_statistics_resident_set_size()},
  {"residentSizePercent",
   new serenedb_process_statistics_resident_set_size_percent()},
  {"virtualSize", new serenedb_process_statistics_virtual_memory_size()},
  {"clientHttpConnections",
   new serenedb_client_connection_statistics_client_connections()},
  {"connectionTime",
   new serenedb_client_connection_statistics_connection_time()},
  {"connectionTimeCount", nullptr},
  {"connectionTimeSum", nullptr},
  {"totalTime", new serenedb_client_connection_statistics_total_time()},
  {"totalTimeCount", nullptr},
  {"totalTimeSum", nullptr},
  {"requestTime", new serenedb_client_connection_statistics_request_time()},
  {"requestTimeCount", nullptr},
  {"requestTimeSum", nullptr},
  {"queueTime", new serenedb_client_connection_statistics_queue_time()},
  {"queueTimeCount", nullptr},
  {"queueTimeSum", nullptr},
  {"ioTime", new serenedb_client_connection_statistics_io_time()},
  {"ioTimeCount", nullptr},
  {"ioTimeSum", nullptr},
  {"httpReqsTotal",
   new serenedb_http_request_statistics_total_requests_total()},
  {"httpReqsSuperuser",
   new serenedb_http_request_statistics_superuser_requests_total()},
  {"httpReqsUser", new serenedb_http_request_statistics_user_requests_total()},
  {"httpReqsAsync",
   new serenedb_http_request_statistics_async_requests_total()},
  {"httpReqsDelete",
   new serenedb_http_request_statistics_http_delete_requests_total()},
  {"httpReqsGet",
   new serenedb_http_request_statistics_http_get_requests_total()},
  {"httpReqsHead",
   new serenedb_http_request_statistics_http_head_requests_total()},
  {"httpReqsOptions",
   new serenedb_http_request_statistics_http_options_requests_total()},
  {"httpReqsPatch",
   new serenedb_http_request_statistics_http_patch_requests_total()},
  {"httpReqsPost",
   new serenedb_http_request_statistics_http_post_requests_total()},
  {"httpReqsPut",
   new serenedb_http_request_statistics_http_put_requests_total()},
  {"httpReqsOther",
   new serenedb_http_request_statistics_other_http_requests_total()},
  {"uptime", new serenedb_server_statistics_server_uptime_total()},
  {"physicalSize", new serenedb_server_statistics_physical_memory()},
  {"cores", new serenedb_server_statistics_cpu_cores()},
  {"userPercent", new serenedb_server_statistics_user_percent()},
  {"systemPercent", new serenedb_server_statistics_system_percent()},
  {"idlePercent", new serenedb_server_statistics_idle_percent()},
  {"iowaitPercent", new serenedb_server_statistics_iowait_percent()},
});
#endif

}  // namespace

Counter gAsyncRequests;
Counter gHttpConnections;
Counter gTotalRequests;
Counter gTotalRequestsSuperuser;
Counter gTotalRequestsUser;
MethodRequestCounters gMethodRequests;
Distribution gConnectionTimeDistribution(kConnectionTimeDistributionCuts);

RequestFigures::RequestFigures()
  : bytes_received_distribution(kBytesReceivedDistributionCuts),
    bytes_sent_distribution(kBytesSentDistributionCuts),
    io_time_distribution(kRequestTimeDistributionCuts),
    queue_time_distribution(kRequestTimeDistributionCuts),
    request_time_distribution(kRequestTimeDistributionCuts),
    total_time_distribution(kRequestTimeDistributionCuts) {}

RequestFigures gSuperuserRequestFigures;
RequestFigures gUserRequestFigures;
}  // namespace statistics
}  // namespace sdb

class StatisticsThread final : public ServerThread<SerenedServer> {
 public:
  explicit StatisticsThread(Server& server)
    : ServerThread<SerenedServer>(server, "Statistics") {}
  ~StatisticsThread() final { shutdown(); }

 public:
  void run() final {
    constexpr const uint64_t kMinIdleSleepTime = 100;
    constexpr const uint64_t kMaxIdleSleepTime = 250;

    uint64_t sleep_time = kMinIdleSleepTime;
    int nothing_happened = 0;

    while (!isStopping()) {
      size_t count = 0;
      try {
        count = RequestStatistics::processAll();
      } catch (const std::exception& ex) {
        SDB_WARN(
          "xxxxx", Logger::STATISTICS,
          "caught exception during request statistics processing: ", ex.what());
      }

      if (count == 0) {
        // nothing needed to be processed
        if (++nothing_happened == 10 * 30) {
          // increase sleep time every 30 seconds
          nothing_happened = 0;
          sleep_time = std::min(sleep_time + 50, kMaxIdleSleepTime);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));

      } else {
        // something needed to be processed
        nothing_happened = 0;
        sleep_time = kMinIdleSleepTime;

        if (count < 10) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else if (count < 100) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
    }
  }
};

StatisticsFeature::StatisticsFeature(Server& server)
  : SerenedFeature{server, name()},
    _statistics(true),
    _descriptions(server),
    _request_statistics_memory_usage{
      server.getFeature<metrics::MetricsFeature>().add(
        serenedb_request_statistics_memory_usage{})},
    _connection_statistics_memory_usage{
      server.getFeature<metrics::MetricsFeature>().add(
        serenedb_connection_statistics_memory_usage{})} {
  setOptional(true);

#ifdef SDB_DEV
  bool found_error = false;
  for (const auto& it : kStatBuilder) {
    if (const auto& stat_it = kStatStrings.find(it.first);
        stat_it != kStatStrings.end()) {
      if (it.second != nullptr) {
        const auto& builder = *it.second;
        const auto& stat = stat_it->second;
        const auto name = stat[0];
        const auto type = stat[1];
        if (builder.name() != name) {
          found_error = true;
          SDB_ERROR("xxxxx", Logger::STATISTICS, "Statistic '", it.first,
                    "' has mismatching names: '", builder.name(),
                    "' in statBuilder but '", name, "' in statStrings");
        }
        if (builder.type() != type) {
          found_error = true;
          SDB_ERROR("xxxxx", Logger::STATISTICS, "Statistic '", it.first,
                    "' has mismatching types (for API v2): '", builder.type(),
                    "' in statBuilder but '", type, "' in statStrings");
        }
      }
    } else {
      found_error = true;
      SDB_ERROR("xxxxx", Logger::STATISTICS, "Statistic '", it.first,
                "' defined in statBuilder, but not in statStrings");
    }
  }
  for (const auto& it : kStatStrings) {
    const auto& stat_it = kStatBuilder.find(it.first);
    if (stat_it == kStatBuilder.end()) {
      found_error = true;
      SDB_ERROR("xxxxx", Logger::STATISTICS, "Statistic '", it.first,
                "' defined in statStrings, but not in statBuilder");
    }
  }
  if (found_error) {
    FatalErrorExit();
  }
#endif
}

void StatisticsFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options
    ->addOption("--server.statistics",
                "Whether to enable statistics gathering and statistics APIs.",
                new BooleanParameter(&_statistics))
    .setLongDescription(R"(If you set this option to `false`, then SereneDB's
statistics gathering is turned off. Statistics gathering causes regular
background CPU activity, memory usage, and writes to the storage engine, so
using this option to turn statistics off might relieve heavily-loaded instances
a bit.)");
}

void StatisticsFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  if (_statistics) {
    // initialize counters for all HTTP request types
    ConnectionStatistics::initialize();
    RequestStatistics::initialize();
  } else {
    // turn ourselves off
    disable();
  }
}

void StatisticsFeature::start() {
  SDB_ASSERT(isEnabled());

  // don't start the thread when we are running an upgrade
  if (!server().getFeature<sdb::UpgradeFeature>().upgrading()) {
    _statistics_thread = std::make_unique<StatisticsThread>(server());

    if (!_statistics_thread->start()) {
      SDB_FATAL("xxxxx", sdb::Logger::STATISTICS,
                "could not start statistics thread");
    }
  }
}

void StatisticsFeature::stop() {
  if (_statistics_thread != nullptr) {
    _statistics_thread->beginShutdown();

    while (_statistics_thread->isRunning()) {
      std::this_thread::sleep_for(std::chrono::microseconds(10000));
    }
  }

  _statistics_thread.reset();
}

vpack::Builder StatisticsFeature::fillDistribution(
  const statistics::Distribution& dist) {
  vpack::Builder builder;
  builder.openObject();

  builder.add("sum", dist.total);
  builder.add("count", dist.count);

  builder.add("counts", vpack::Value(vpack::ValueType::Array));
  for (const auto& val : dist.counts) {
    builder.add(val);
  }
  builder.close();

  builder.close();

  return builder;
}

void StatisticsFeature::appendHistogram(
  std::string& result, const statistics::Distribution& dist,
  const std::string& label, const std::initializer_list<std::string>& les,
  bool is_integer, std::string_view globals, bool ensure_whitespace) {
  vpack::Builder tmp = fillDistribution(dist);
  vpack::Slice slc = tmp.slice();
  vpack::Slice counts = slc.get("counts");

  const auto& stat = kStatStrings.at(label);
  const auto name = stat[0];
  const auto type = stat[1];
  const auto help = stat[2];

  metrics::Metric::addInfo(result, name, help, type);
  SDB_ASSERT(les.size() == counts.length());
  size_t i = 0;
  uint64_t sum = 0;
  for (const auto& le : les) {
    sum += counts.at(i++).getNumber<uint64_t>();
    absl::StrAppend(&result, name, "_bucket{le=\"", le, "\"",
                    (globals.empty() ? "" : ","), globals, "}",
                    (ensure_whitespace ? " " : ""), sum, "\n");
  }
  absl::StrAppend(&result, name, "_count{", globals, "}",
                  (ensure_whitespace ? " " : ""), sum, "\n");
  if (is_integer) {
    uint64_t v = slc.get("sum").getNumber<uint64_t>();
    absl::StrAppend(&result, name, "_sum{", globals, "}",
                    (ensure_whitespace ? " " : ""), v, "\n");
  } else {
    double v = slc.get("sum").getNumber<double>();
    // must use std::to_string() here because it produces a different
    // string representation of large floating-point numbers than absl
    // does. absl uses scientific notation for numbers that exceed 6
    // digits, and std::to_string() doesn't.
    absl::StrAppend(&result, name, "_sum{", globals, "}",
                    (ensure_whitespace ? " " : ""), std::to_string(v), "\n");
  }
}

void StatisticsFeature::appendMetric(std::string& result,
                                     const std::string& val,
                                     const std::string& label,
                                     std::string_view globals,
                                     bool ensure_whitespace) {
  const auto& stat = kStatStrings.at(label);
  const auto name = stat[0];
  const auto type = stat[1];
  const auto help = stat[2];

  metrics::Metric::addInfo(result, name, help, type);
  metrics::Metric::addMark(result, name, globals, "");
  absl::StrAppend(&result, ensure_whitespace ? " " : "", val, "\n");
}

void StatisticsFeature::toPrometheus(std::string& result, double now,
                                     std::string_view globals,
                                     bool ensure_whitespace) {
  // these metrics should always be 0 if statistics are disabled
  SDB_ASSERT(isEnabled() || (RequestStatistics::memoryUsage() == 0 &&
                             ConnectionStatistics::memoryUsage() == 0));

  if (isEnabled()) {
    _request_statistics_memory_usage.store(RequestStatistics::memoryUsage(),
                                           std::memory_order_relaxed);
    _connection_statistics_memory_usage.store(
      ConnectionStatistics::memoryUsage(), std::memory_order_relaxed);
  }

  ProcessInfo info = GetProcessInfoSelf();
  uint64_t rss = static_cast<uint64_t>(info.resident_size);
  double rssp = 0;

  if (physical_memory::GetValue() != 0) {
    rssp = static_cast<double>(rss) /
           static_cast<double>(physical_memory::GetValue());
  }

  const ServerStatistics& server_info =
    server().getFeature<metrics::MetricsFeature>().serverStatistics();

  // processStatistics()
  appendMetric(result, std::to_string(info.minor_page_faults),
               "minorPageFaults", globals, ensure_whitespace);
  appendMetric(result, std::to_string(info.major_page_faults),
               "majorPageFaults", globals, ensure_whitespace);
  if (info.sc_clk_tck != 0) {  // prevent division by zero
    appendMetric(result,
                 std::to_string(static_cast<double>(info.user_time) /
                                static_cast<double>(info.sc_clk_tck)),
                 "userTime", globals, ensure_whitespace);
    appendMetric(result,
                 std::to_string(static_cast<double>(info.system_time) /
                                static_cast<double>(info.sc_clk_tck)),
                 "systemTime", globals, ensure_whitespace);
  }
  appendMetric(result, std::to_string(info.number_threads), "numberOfThreads",
               globals, ensure_whitespace);
  appendMetric(result, std::to_string(rss), "residentSize", globals,
               ensure_whitespace);
  appendMetric(result, std::to_string(rssp), "residentSizePercent", globals,
               ensure_whitespace);
  appendMetric(result, std::to_string(info.virtual_size), "virtualSize",
               globals, ensure_whitespace);
  appendMetric(result, std::to_string(physical_memory::GetValue()),
               "physicalSize", globals, ensure_whitespace);
  appendMetric(result, std::to_string(server_info.uptime()), "uptime", globals,
               ensure_whitespace);
  appendMetric(result, std::to_string(number_of_cores::GetValue()), "cores",
               globals, ensure_whitespace);

  CpuUsageFeature& cpu_usage = server().getFeature<CpuUsageFeature>();
  if (cpu_usage.isEnabled()) {
    auto snapshot = cpu_usage.snapshot();
    appendMetric(result, std::to_string(snapshot.userPercent()), "userPercent",
                 globals, ensure_whitespace);
    appendMetric(result, std::to_string(snapshot.systemPercent()),
                 "systemPercent", globals, ensure_whitespace);
    appendMetric(result, std::to_string(snapshot.idlePercent()), "idlePercent",
                 globals, ensure_whitespace);
    appendMetric(result, std::to_string(snapshot.iowaitPercent()),
                 "iowaitPercent", globals, ensure_whitespace);
  }

  if (isEnabled()) {
    ConnectionStatistics::Snapshot connection_stats;
    ConnectionStatistics::getSnapshot(connection_stats);

    RequestStatistics::Snapshot request_stats;
    RequestStatistics::getSnapshot(request_stats,
                                   stats::RequestStatisticsSource::kAll);

    // _client_statistics()
    appendMetric(result,
                 std::to_string(connection_stats.http_connections.get()),
                 "clientHttpConnections", globals, ensure_whitespace);
    appendHistogram(result, connection_stats.connection_time, "connectionTime",
                    {"0.01", "1.0", "60.0", "+Inf"}, false, globals,
                    ensure_whitespace);
    appendHistogram(result, request_stats.total_time, "totalTime",
                    {"0.01", "0.05", "0.1", "0.2", "0.5", "1.0", "5.0", "15.0",
                     "30.0", "+Inf"},
                    false, globals, ensure_whitespace);
    appendHistogram(result, request_stats.request_time, "requestTime",
                    {"0.01", "0.05", "0.1", "0.2", "0.5", "1.0", "5.0", "15.0",
                     "30.0", "+Inf"},
                    false, globals, ensure_whitespace);
    appendHistogram(result, request_stats.queue_time, "queueTime",
                    {"0.01", "0.05", "0.1", "0.2", "0.5", "1.0", "5.0", "15.0",
                     "30.0", "+Inf"},
                    false, globals, ensure_whitespace);
    appendHistogram(result, request_stats.io_time, "ioTime",
                    {"0.01", "0.05", "0.1", "0.2", "0.5", "1.0", "5.0", "15.0",
                     "30.0", "+Inf"},
                    false, globals, ensure_whitespace);
    appendHistogram(result, request_stats.bytes_sent, "bytesSent",
                    {"250", "1000", "2000", "5000", "10000", "+Inf"}, true,
                    globals, ensure_whitespace);
    appendHistogram(result, request_stats.bytes_received, "bytesReceived",
                    {"250", "1000", "2000", "5000", "10000", "+Inf"}, true,
                    globals, ensure_whitespace);

    RequestStatistics::Snapshot request_stats_user;
    RequestStatistics::getSnapshot(request_stats_user,
                                   stats::RequestStatisticsSource::kUser);
    appendHistogram(result, request_stats_user.bytes_sent, "bytesSentUser",
                    {"250", "1000", "2000", "5000", "10000", "+Inf"}, true,
                    globals, ensure_whitespace);
    appendHistogram(result, request_stats_user.bytes_received,
                    "bytesReceivedUser",
                    {"250", "1000", "2000", "5000", "10000", "+Inf"}, true,
                    globals, ensure_whitespace);

    // _http_statistics()
    using rest::RequestType;
    appendMetric(result, std::to_string(connection_stats.async_requests.get()),
                 "httpReqsAsync", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::DeleteReq].get()),
      "httpReqsDelete", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Get].get()),
      "httpReqsGet", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Head].get()),
      "httpReqsHead", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Options].get()),
      "httpReqsOptions", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Patch].get()),
      "httpReqsPatch", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Post].get()),
      "httpReqsPost", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Put].get()),
      "httpReqsPut", globals, ensure_whitespace);
    appendMetric(
      result,
      std::to_string(
        connection_stats.method_requests[(int)RequestType::Illegal].get()),
      "httpReqsOther", globals, ensure_whitespace);
    appendMetric(result, std::to_string(connection_stats.total_requests.get()),
                 "httpReqsTotal", globals, ensure_whitespace);
    appendMetric(
      result, std::to_string(connection_stats.total_requests_superuser.get()),
      "httpReqsSuperuser", globals, ensure_whitespace);
    appendMetric(result,
                 std::to_string(connection_stats.total_requests_user.get()),
                 "httpReqsUser", globals, ensure_whitespace);
  }
}
