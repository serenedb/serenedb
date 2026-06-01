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

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

ABSL_DECLARE_FLAG(uint64_t, server_io_threads);

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
#include "basics/number_of_cores.h"
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

GeneralServerFeature::GeneralServerFeature()
  : current_requests_size(AddMetric(serenedb_requests_memory_usage{})),
    _keep_alive_timeout(absl::GetFlag(FLAGS_http_keep_alive_timeout)),
    _return_queue_time_header(
      absl::GetFlag(FLAGS_http_return_queue_time_header)),
    _compress_response_threshold(
      absl::GetFlag(FLAGS_http_compress_response_threshold)),
    _access_control_allow_origins(absl::GetFlag(FLAGS_http_trusted_origin)),
    _num_io_threads(
      std::max(uint64_t(1), uint64_t(number_of_cores::GetValue() / 4))),
    _request_body_size_http1(AddMetric(serenedb_request_body_size_http1{})),
    _request_body_size_http2(AddMetric(serenedb_request_body_size_http2{})),
    _http1_connections(AddMetric(serenedb_http1_connections_total{})),
    _http2_connections(AddMetric(serenedb_http2_connections_total{})) {
  if (auto io = absl::GetFlag(FLAGS_server_io_threads); io > 0) {
    _num_io_threads = io;
  }

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
  gInstance = this;
}

GeneralServerFeature::~GeneralServerFeature() { gInstance = nullptr; }

void GeneralServerFeature::start() {
  // Mode flips to Startup so the still-empty HTTP listener serves the
  // narrow "startup" request surface; once the server + startListening
  // complete we move to Maintenance for normal traffic.
  ServerState::instance()->SetMode(ServerState::Mode::Startup);

  _job_manager = std::make_unique<AsyncJobManager>();

  // check if endpointList contains ssl featured server
  const auto& endpoint_list = Endpoints();
  if (endpoint_list.hasSsl()) {
    SslServerFeature::instance().verifySslOptions();
  }
  _server = std::make_unique<GeneralServer>(*this, _num_io_threads);

  auto hf = std::make_shared<RestHandlerFactory>();
  hf->seal();
  {
    std::unique_lock guard{_handler_factory_mutex};
    _handler_factory = std::move(hf);
  }

  startListening();

  ServerState::instance()->SetMode(ServerState::Mode::Maintenance);
}

void GeneralServerFeature::stop() {
  // Nudge listeners off the accept loop first so stop()'s join-style
  // shutdown of connections doesn't race new accepts.
  _server->stopListening();
  _job_manager->deleteJobs(ExecContext::superuser());
  _server->stopConnections();
  _server->stopWorking();
  _server.reset();
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

uint64_t GeneralServerFeature::compressResponseThreshold() const noexcept {
  return _compress_response_threshold;
}

std::shared_ptr<rest::RestHandlerFactory> GeneralServerFeature::handlerFactory()
  const {
  std::shared_lock guard{_handler_factory_mutex};
  return _handler_factory;
}

rest::AsyncJobManager& GeneralServerFeature::jobManager() {
  return *_job_manager;
}

void GeneralServerFeature::startListening() {
  _server->startListening(Endpoints());
}

}  // namespace sdb
