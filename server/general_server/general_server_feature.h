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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "basics/result.h"
#include "general_server/async_job_manager.h"
#include "general_server/general_server.h"
#include "general_server/rest_handler_factory.h"
#include "metrics/counter.h"
#include "metrics/gauge.h"
#include "metrics/histogram.h"
#include "metrics/log_scale.h"
#include "rest_server/serened.h"

namespace sdb {

class RestServerThread;

class GeneralServerFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "GeneralServer"; }

  explicit GeneralServerFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void start() final;
  void initiateSoftShutdown() final;
  void beginShutdown() final;
  void stop() final;
  void unprepare() final;

  double keepAliveTimeout() const noexcept;
  bool handleContentEncodingForUnauthenticatedRequests() const noexcept;
  bool proxyCheck() const noexcept;
  bool returnQueueTimeHeader() const noexcept;
  std::vector<std::string> trustedProxies() const;
  const std::vector<std::string>& accessControlAllowOrigins() const;
  Result reloadTLS();
  const std::string& optionsApiPolicy() const noexcept;
  uint64_t compressResponseThreshold() const noexcept;

  std::shared_ptr<rest::RestHandlerFactory> handlerFactory() const;
  rest::AsyncJobManager& jobManager();

  void countHttp1Request(uint64_t body_size) noexcept {
    _request_body_size_http1.count(body_size);
  }

  void countHttp2Request(uint64_t body_size) noexcept {
    _request_body_size_http2.count(body_size);
  }

  void countHttp1Connection() { _http1_connections.count(); }

  void countHttp2Connection() { _http2_connections.count(); }

  uint64_t telemetricsMaxRequestsPerInterval() const noexcept {
    return _telemetrics_max_requests_per_interval;
  }

  bool isRestApiHardened() const noexcept { return _hardened_rest_api; }
  bool canAccessHardenedApi(const ExecContext& exec) const noexcept;

  metrics::Gauge<uint64_t>& current_requests_size;

 private:
  // build HTTP server(s)
  void buildServers();
  // open REST interface for listening
  void startListening();
  // define initial (minimal) REST handlers
  void defineInitialHandlers(rest::RestHandlerFactory& f);
  // define remaining REST handlers
  void defineRemainingHandlers(rest::RestHandlerFactory& f);

  double _keep_alive_timeout = 300.0;
  uint64_t _telemetrics_max_requests_per_interval;
#ifdef SDB_DEV
  bool _started_listening;
#endif
  bool _allow_early_connections;
  bool _handle_content_encoding_for_unauthenticated_requests;
  bool _return_queue_time_header;
  bool _hardened_rest_api = false;
  uint64_t _compress_response_threshold;
  std::vector<std::string> _access_control_allow_origins;
  std::string _options_api_policy;
  std::shared_ptr<rest::RestHandlerFactory> _handler_factory;
  std::unique_ptr<rest::AsyncJobManager> _job_manager;
  std::vector<std::unique_ptr<rest::GeneralServer>> _servers;
  uint64_t _num_io_threads;

#ifdef SDB_FAULT_INJECTION
  std::vector<std::string> _failure_points;
#endif

  // Some metrics about requests and connections
  metrics::Histogram<metrics::LogScale<uint64_t>>& _request_body_size_http1;
  metrics::Histogram<metrics::LogScale<uint64_t>>& _request_body_size_http2;
  metrics::Counter& _http1_connections;
  metrics::Counter& _http2_connections;
};

}  // namespace sdb
