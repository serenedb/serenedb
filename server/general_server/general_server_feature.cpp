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

#include "general_server_feature.h"

#include <absl/flags/flag.h>

#include <chrono>
#include <stdexcept>
#include <thread>

ABSL_FLAG(uint64_t, server_io_threads, 0,
          "Number of IO threads for HTTP and pg-wire connections "
          "(0 = max(1, cpu_count/4)).");
ABSL_FLAG(double, http_keep_alive_timeout, 300.0,
          "Keep-alive timeout for HTTP and pg-wire connections (in seconds; "
          "0 disables keep-alive).");
ABSL_FLAG(std::vector<std::string>, http_trusted_origin, {},
          "Trusted origin URLs for CORS requests with credentials.");
ABSL_FLAG(bool, http_return_queue_time_header, true,
          "Return the x-serene-queue-time-seconds header on every response.");
ABSL_FLAG(uint64_t, http_compress_response_threshold, 0,
          "Response body size threshold for transparent gzip/deflate "
          "compression (0 disables compression).");

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/string_utils.h"
#include "general_server/general_server.h"
#include "general_server/rest_handler_factory.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/ssl_server_feature.h"
#include "general_server/state.h"
#include "metrics/counter_builder.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/metrics_feature.h"
#include "basics/number_of_cores.h"
#include "rest/http_response.h"
#include "rest_server/endpoint_feature.h"
#include "storage_engine/engine_feature.h"

using namespace sdb::rest;
using namespace sdb::options;

namespace sdb {

struct RequestBodySizeScale {
  static metrics::LogScale<uint64_t> scale() { return {2, 64, 65536, 10}; }
};

DECLARE_HISTOGRAM(serenedb_request_body_size_http1, RequestBodySizeScale,
                  "Body size of HTTP/1.1 requests");
DECLARE_HISTOGRAM(serenedb_request_body_size_http2, RequestBodySizeScale,
                  "Body size of HTTP/2 requests");
DECLARE_COUNTER(serenedb_http1_connections_total,
                "Total number of HTTP/1.1 connections");
DECLARE_COUNTER(serenedb_http2_connections_total,
                "Total number of HTTP/2 connections");
DECLARE_GAUGE(serenedb_requests_memory_usage, uint64_t,
              "Memory consumed by incoming requests");

GeneralServerFeature::GeneralServerFeature(Server& server)
  : SerenedFeature{server, name()},
    current_requests_size(AddMetric(serenedb_requests_memory_usage{})),
#ifdef SDB_DEV
    _started_listening(false),
#endif
    _allow_early_connections(false),
    _return_queue_time_header(true),
    _compress_response_threshold(0),
    _num_io_threads(
      std::max(uint64_t(1), uint64_t(number_of_cores::GetValue() / 4))),
    _request_body_size_http1(AddMetric(serenedb_request_body_size_http1{})),
    _request_body_size_http2(AddMetric(serenedb_request_body_size_http2{})),
    _http1_connections(AddMetric(serenedb_http1_connections_total{})),
    _http2_connections(AddMetric(serenedb_http2_connections_total{})) {

  setOptional(true);
}

void GeneralServerFeature::validateOptions() {
  if (auto io = absl::GetFlag(FLAGS_server_io_threads); io > 0) {
    _num_io_threads = io;
  }
  _keep_alive_timeout = absl::GetFlag(FLAGS_http_keep_alive_timeout);
  _access_control_allow_origins = absl::GetFlag(FLAGS_http_trusted_origin);
  _return_queue_time_header = absl::GetFlag(FLAGS_http_return_queue_time_header);
  _compress_response_threshold =
    absl::GetFlag(FLAGS_http_compress_response_threshold);

  if (!_access_control_allow_origins.empty()) {
    for (auto& it : _access_control_allow_origins) {
      if (it == "*" || it == "all") {
        _access_control_allow_origins.clear();
        _access_control_allow_origins.push_back("*");
        break;
      } else if (it == "none") {
        _access_control_allow_origins.clear();
        break;
      } else if (it.ends_with('/')) {
        it = it.substr(0, it.size() - 1);
      }
    }
    _access_control_allow_origins.erase(
      std::remove_if(_access_control_allow_origins.begin(),
                     _access_control_allow_origins.end(),
                     [](const std::string& value) {
                       return basics::string_utils::Trim(value).empty();
                     }),
      _access_control_allow_origins.end());
  }
#ifdef SDB_FAULT_INJECTION
  for (const auto& it : _failure_points) {
    AddFailurePointDebugging(it);
  }
#endif
}

void GeneralServerFeature::prepare() {
  ServerState::instance()->SetMode(ServerState::Mode::Startup);

  _job_manager = std::make_unique<AsyncJobManager>();

  // create an initial, very stripped-down RestHandlerFactory.
  // this initial factory only knows a few selected RestHandlers.
  // we will later create another RestHandlerFactory that knows
  // all routes.
  auto hf = std::make_shared<RestHandlerFactory>();
  defineInitialHandlers(*hf);
  // make handler-factory read-only
  hf->seal();

  std::atomic_store(&_handler_factory, std::move(hf));

  buildServers();

  if (_allow_early_connections) {
    // open HTTP interface early if this is requested.
    startListening();
  }

#ifdef SDB_FAULT_INJECTION
  SDB_IF_FAILURE("startListeningEarly") {
    while (ShouldFailDebugging("startListeningEarly")) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
#endif
}

void GeneralServerFeature::start() {
  SDB_ASSERT(ServerState::instance()->GetMode() == ServerState::Mode::Startup);

  // create the full RestHandlerFactory that knows all the routes.
  // this will replace the previous, stripped-down RestHandlerFactory
  // instance.
  auto hf = std::make_shared<RestHandlerFactory>();

  defineInitialHandlers(*hf);
  defineRemainingHandlers(*hf);
  hf->seal();

  std::atomic_store(&_handler_factory, std::move(hf));

#ifdef SDB_DEV
  SDB_ASSERT(!_allow_early_connections || _started_listening);
#endif
  if (!_allow_early_connections) {
    // if HTTP interface is not open yet, open it now
    startListening();
  }
#ifdef SDB_DEV
  SDB_ASSERT(_started_listening);
#endif

  ServerState::instance()->SetMode(ServerState::Mode::Maintenance);
}

void GeneralServerFeature::beginShutdown() {
  for (auto& server : _servers) {
    server->stopListening();
  }
}

void GeneralServerFeature::stop() {
  _job_manager->deleteJobs(ExecContext::superuser());
  for (auto& server : _servers) {
    server->stopConnections();
  }
}

void GeneralServerFeature::unprepare() {
  for (auto& server : _servers) {
    server->stopWorking();
  }
  _servers.clear();
  _job_manager.reset();
}

double GeneralServerFeature::keepAliveTimeout() const noexcept {
  return _keep_alive_timeout;
}

bool GeneralServerFeature::returnQueueTimeHeader() const noexcept {
  return _return_queue_time_header;
}

const std::vector<std::string>&
GeneralServerFeature::accessControlAllowOrigins() const {
  return _access_control_allow_origins;
}

Result GeneralServerFeature::reloadTLS() {  // reload TLS data from disk
  Result res;
  for (auto& up : _servers) {
    Result res2 = up->reloadTLS();
    if (res2.fail()) {
      // yes, we only report the last error if there is one
      res = std::move(res2);
    }
  }
  return res;
}

uint64_t GeneralServerFeature::compressResponseThreshold() const noexcept {
  return _compress_response_threshold;
}

std::shared_ptr<rest::RestHandlerFactory> GeneralServerFeature::handlerFactory()
  const {
  return std::atomic_load_explicit(&_handler_factory,
                                   std::memory_order_relaxed);
}

rest::AsyncJobManager& GeneralServerFeature::jobManager() {
  return *_job_manager;
}

void GeneralServerFeature::buildServers() {
  EndpointFeature& endpoint =
    server().getFeature<EndpointFeature>();
  const auto& endpoint_list = endpoint.endpointList();

  // check if endpointList contains ssl featured server
  if (endpoint_list.hasSsl()) {
    if (!server().hasFeature<SslServerFeature>()) {
      SDB_FATAL(GENERAL,
                "no ssl context is known, cannot create https server, "
                "please enable SSL");
    }
    SslServerFeature& ssl = server().getFeature<SslServerFeature>();
    ssl.verifySslOptions();
  }

  _servers.emplace_back(std::make_unique<GeneralServer>(
    *this, _num_io_threads, _allow_early_connections));
}

void GeneralServerFeature::startListening() {
#ifdef SDB_DEV
  SDB_ASSERT(!_started_listening);
#endif

  EndpointFeature& endpoint =
    server().getFeature<EndpointFeature>();
  auto& endpoint_list = endpoint.endpointList();

  for (auto& server : _servers) {
    server->startListening(endpoint_list);
  }

#ifdef SDB_DEV
  _started_listening = true;
#endif
}

void GeneralServerFeature::defineInitialHandlers(rest::RestHandlerFactory& f) {}

void GeneralServerFeature::defineRemainingHandlers(
  rest::RestHandlerFactory& f) {}

}  // namespace sdb
