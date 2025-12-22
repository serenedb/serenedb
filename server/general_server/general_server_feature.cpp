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

#include <chrono>
#include <stdexcept>
#include <thread>

#include "app/app_server.h"
#include "app/http_endpoint_provider.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/string_utils.h"
#include "general_server/authentication_feature.h"
#include "general_server/general_server.h"
#include "general_server/rest_handler_factory.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/ssl_server_feature.h"
#include "metrics/counter_builder.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/metrics_feature.h"
#include "network/network_feature.h"
#include "rest/http_response.h"
#include "rest_server/endpoint_feature.h"
#include "rest_server/upgrade_feature.h"
#include "storage_engine/engine_feature.h"

#ifdef SDB_CLUSTER
#include "agency/agency_feature.h"
#include "agency/rest_agency_handler.h"
#include "agency/rest_agency_priv_handler.h"
#include "aql/query_registry_feature.h"
#include "aql/rest_aql_handler.h"
#include "cluster/agency_callback_registry.h"
#include "cluster/backup_feature.h"
#include "cluster/cluster_feature.h"
#include "cluster/maintenance_rest_handler.h"
#include "cluster/rest_cluster_handler.h"
#include "graph/rest_traverser_handler.h"
#include "rest_handler/rest_admin_cluster_handler.h"
#include "rest_handler/rest_admin_log_handler.h"
#include "rest_handler/rest_admin_server_handler.h"
#include "rest_handler/rest_admin_statistics_handler.h"
#include "rest_handler/rest_analyzer_handler.h"
#include "rest_handler/rest_auth_handler.h"
#include "rest_handler/rest_auth_reload_handler.h"
#include "rest_handler/rest_backup_handler.h"
#include "rest_handler/rest_compact_handler.h"
#include "rest_handler/rest_cursor_handler.h"
#include "rest_handler/rest_database_handler.h"
#include "rest_handler/rest_debug_handler.h"
#include "rest_handler/rest_document_handler.h"
#include "rest_handler/rest_dump_handler.h"
#include "rest_handler/rest_endpoint_handler.h"
#include "rest_handler/rest_explain_handler.h"
#include "rest_handler/rest_handler_creator.h"
#include "rest_handler/rest_import_handler.h"
#include "rest_handler/rest_index_handler.h"
#include "rest_handler/rest_job_handler.h"
#include "rest_handler/rest_key_generators_handler.h"
#include "rest_handler/rest_metrics_handler.h"
#include "rest_handler/rest_not_impl_handler.h"
#include "rest_handler/rest_options_description_handler.h"
#include "rest_handler/rest_options_handler.h"
#include "rest_handler/rest_query_cache_handler.h"
#include "rest_handler/rest_query_handler.h"
#include "rest_handler/rest_shutdown_handler.h"
#include "rest_handler/rest_supervision_state_handler.h"
#include "rest_handler/rest_transaction_handler.h"
#include "rest_handler/rest_ttl_handler.h"
#include "rest_handler/rest_usage_metrics_handler.h"
#include "rest_handler/rest_users_handler.h"
#include "rest_handler/rest_version_handler.h"
#include "rest_handler/rest_view_handler.h"
#include "rest_handler/rest_wal_access_handler.h"
#ifdef SDB_DEV
#include "rest_handler/rest_test_handler.h"
#endif
#endif

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
    _handle_content_encoding_for_unauthenticated_requests(false),
    _return_queue_time_header(true),
    _compress_response_threshold(0),
    _options_api_policy("jwt"),
    _num_io_threads(NetworkFeature::defaultIOThreads()),
    _request_body_size_http1(AddMetric(serenedb_request_body_size_http1{})),
    _request_body_size_http2(AddMetric(serenedb_request_body_size_http2{})),
    _http1_connections(AddMetric(serenedb_http1_connections_total{})),
    _http2_connections(AddMetric(serenedb_http2_connections_total{})) {
  static_assert(
    Server::isCreatedAfter<GeneralServerFeature, metrics::MetricsFeature>());

  setOptional(true);
}

void GeneralServerFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options->addOption(
    "--server.io-threads", "The number of threads used to handle I/O.",
    new UInt64Parameter(&_num_io_threads, /*base*/ 1, /*minValue*/ 1),
    options::MakeDefaultFlags(options::Flags::Dynamic));

  options->addOption("--server.options-api",
                     "The policy for exposing the options API.",
                     new DiscreteValuesParameter<StringParameter>(
                       &_options_api_policy, {
                                               "disabled",
                                               "jwt",
                                               "admin",
                                               "public",
                                             }));

  options->addSection("http", "HTTP server features");

  options
    ->addOption("--http.keep-alive-timeout",
                "The keep-alive timeout for HTTP connections (in seconds).",
                new DoubleParameter(&_keep_alive_timeout))
    .setLongDescription(R"(Idle keep-alive connections are closed by the
server automatically when the timeout is reached. A keep-alive-timeout value of
`0` disables the keep-alive feature entirely.)");

  options->addOption(
    "--http.trusted-origin",
    "The trusted origin URLs for CORS requests with credentials.",
    new VectorParameter<StringParameter>(&_access_control_allow_origins));

  options
    ->addOption("--http.return-queue-time-header",
                "Whether to return the `x-serene-queue-time-seconds` header "
                "in all responses.",
                new BooleanParameter(&_return_queue_time_header))

    .setLongDescription(R"(The value contained in this header indicates the
current queueing/dequeuing time for requests in the scheduler (in seconds).
Client applications and drivers can use this value to control the server load
and also react on overload.)");

  options
    ->addOption("--http.compress-response-threshold",
                "The HTTP response body size from which on responses are "
                "transparently compressed in case the client asks for it.",
                new UInt64Parameter(&_compress_response_threshold))

    .setLongDescription(
      R"(Automatically compress outgoing HTTP responses with the
deflate or gzip compression format, in case the client request advertises
support for this. Compression will only happen for HTTP/1.1 and HTTP/2
connections, if the size of the uncompressed response body exceeds
the threshold value controlled by this startup option,
and if the response body size after compression is less than the original
response body size.
Using the value 0 disables the automatic response compression.")");

  options->addOption("--server.early-connections",
                     "Allow requests to a limited set of APIs early during the "
                     "server startup.",
                     new BooleanParameter(&_allow_early_connections));

#ifdef SDB_FAULT_INJECTION
  options->addOption(
    "--server.failure-point",
    "The failure point to set during server startup (requires compilation "
    "with failure points support).",
    new VectorParameter<StringParameter>(&_failure_points),
    sdb::options::MakeFlags(sdb::options::Flags::Default,
                            sdb::options::Flags::Uncommon));
#endif

  options
    ->addOption("--http.handle-content-encoding-for-unauthenticated-requests",
                "Handle Content-Encoding headers for unauthenticated requests.",
                new BooleanParameter(
                  &_handle_content_encoding_for_unauthenticated_requests))

    .setLongDescription(
      R"(If the option is set to `true`, the server will automatically
uncompress incoming HTTP requests with Content-Encodings gzip and deflate
even if the request is not authenticated.)");

  options->addOption(
    "--server.harden",
    "Lock down REST APIs that reveal version information or server "
    "internals for non-admin users.",
    new BooleanParameter(&_hardened_rest_api));
}

void GeneralServerFeature::validateOptions(std::shared_ptr<ProgramOptions>) {
  if (!_access_control_allow_origins.empty()) {
    // trim trailing slash from all members
    for (auto& it : _access_control_allow_origins) {
      if (it == "*" || it == "all") {
        // special members "*" or "all" means all origins are allowed
        _access_control_allow_origins.clear();
        _access_control_allow_origins.push_back("*");
        break;
      } else if (it == "none") {
        // "none" means no origins are allowed
        _access_control_allow_origins.clear();
        break;
      } else if (it.ends_with('/')) {
        // strip trailing slash
        it = it.substr(0, it.size() - 1);
      }
    }

    // remove empty members
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

void GeneralServerFeature::initiateSoftShutdown() {
  if (_job_manager != nullptr) {
    _job_manager->initiateSoftShutdown();
  }
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

bool GeneralServerFeature::canAccessHardenedApi(
  const ExecContext& exec) const noexcept {
  bool allow_access = !isRestApiHardened();

  if (!allow_access) {
    if (exec.isAdminUser()) {
      // also allow access if there is not authentication
      // enabled or when the user is an administrator
      allow_access = true;
    }
  }
  return allow_access;
}

double GeneralServerFeature::keepAliveTimeout() const noexcept {
  return _keep_alive_timeout;
}

bool GeneralServerFeature::handleContentEncodingForUnauthenticatedRequests()
  const noexcept {
  return _handle_content_encoding_for_unauthenticated_requests;
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

const std::string& GeneralServerFeature::optionsApiPolicy() const noexcept {
  return _options_api_policy;
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
    server().getFeature<HttpEndpointProvider, EndpointFeature>();
  const auto& endpoint_list = endpoint.endpointList();

  // check if endpointList contains ssl featured server
  if (endpoint_list.hasSsl()) {
    if (!server().hasFeature<SslServerFeature>()) {
      SDB_FATAL("xxxxx", sdb::Logger::FIXME,
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
    server().getFeature<HttpEndpointProvider, EndpointFeature>();
  auto& endpoint_list = endpoint.endpointList();

  for (auto& server : _servers) {
    server->startListening(endpoint_list);
  }

#ifdef SDB_DEV
  _started_listening = true;
#endif
}

void GeneralServerFeature::defineInitialHandlers(rest::RestHandlerFactory& f) {
#ifdef SDB_CLUSTER
  // these handlers will be available early during the server start.
  // if you want to add more handlers here, please make sure that they
  // run on the CLIENT_FAST request lane. otherwise the incoming requests
  // will still be rejected during startup, even though they are registered
  // here.
  f.addHandler("/_api/version",
               RestHandlerCreator<RestVersionHandler>::createNoData);
  f.addHandler("/_admin/version",
               RestHandlerCreator<RestVersionHandler>::createNoData);
#ifdef SDB_FAULT_INJECTION
  // This handler can be used to control failure points
  f.addPrefixHandler("/_admin/debug",
                     RestHandlerCreator<sdb::RestDebugHandler>::createNoData);
#endif
#endif
}

void GeneralServerFeature::defineRemainingHandlers(
  rest::RestHandlerFactory& f) {
#ifdef SDB_CLUSTER
  SDB_ASSERT(_job_manager != nullptr);

  AgencyFeature& agency = server().getFeature<AgencyFeature>();
  ClusterFeature& cluster = server().getFeature<ClusterFeature>();
  AuthenticationFeature& authentication =
    server().getFeature<AuthenticationFeature>();

  // ...........................................................................
  // /_api
  // ...........................................................................

  f.addPrefixHandler(                        // add handler
    RestDatabaseBaseHandler::kAnalyzerPath,  // base URL
    RestHandlerCreator<search::RestAnalyzerHandler>::createNoData  // handler
  );

  auto query_registry = QueryRegistryFeature::registry();
  f.addPrefixHandler(
    RestDatabaseBaseHandler::kCursorPath,
    RestHandlerCreator<RestCursorHandler>::createData<aql::QueryRegistry*>,
    query_registry);

  f.addPrefixHandler(RestDatabaseBaseHandler::kDatabasePath,
                     RestHandlerCreator<RestDatabaseHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kDocumentPath,
                     RestHandlerCreator<RestDocumentHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kEndpointPath,
                     RestHandlerCreator<RestEndpointHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kImportPath,
                     RestHandlerCreator<RestImportHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kIndexPath,
                     RestHandlerCreator<RestIndexHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kUsersPath,
                     RestHandlerCreator<RestUsersHandler>::createNoData);

  f.addPrefixHandler(RestDatabaseBaseHandler::kViewPath,
                     RestHandlerCreator<RestViewHandler>::createNoData);

  // This is the only handler were we need to inject
  // more than one data object. So we created the combinedRegistries
  // for it.
  f.addPrefixHandler(
    "/_api/aql",
    RestHandlerCreator<aql::RestAqlHandler>::createData<aql::QueryRegistry*>,
    query_registry);

  f.addPrefixHandler("/_api/dump",
                     RestHandlerCreator<sdb::RestDumpHandler>::createNoData);

  f.addPrefixHandler("/_api/explain",
                     RestHandlerCreator<RestExplainHandler>::createNoData);

#ifdef SDB_DEV
  f.addPrefixHandler("/_test",
                     RestHandlerCreator<RestTestHandler>::createNoData);
#endif

  f.addPrefixHandler(
    "/_api/key-generators",
    RestHandlerCreator<RestKeyGeneratorsHandler>::createNoData);

  f.addPrefixHandler("/_api/query",
                     RestHandlerCreator<RestQueryHandler>::createNoData);

  f.addPrefixHandler("/_api/query-cache",
                     RestHandlerCreator<RestQueryCacheHandler>::createNoData);

  f.addPrefixHandler("/_api/wal",
                     RestHandlerCreator<RestWalAccessHandler>::createNoData);

  if (agency.isEnabled()) {
    f.addPrefixHandler(
      RestDatabaseBaseHandler::kAgencyPath,
      RestHandlerCreator<RestAgencyHandler>::createData<consensus::Agent*>,
      agency.agent());

    f.addPrefixHandler(
      RestDatabaseBaseHandler::kAgencyPrivPath,
      RestHandlerCreator<RestAgencyPrivHandler>::createData<consensus::Agent*>,
      agency.agent());
  }

  if (cluster.isEnabled()) {
    // add "_api/cluster" handler
    f.addPrefixHandler(cluster.clusterRestPath(),
                       RestHandlerCreator<RestClusterHandler>::createNoData);
  }
  f.addPrefixHandler(
    RestDatabaseBaseHandler::kInternalTraverserPath,
    RestHandlerCreator<RestTraverserHandler>::createData<aql::QueryRegistry*>,
    query_registry);

  // And now some handlers which are registered in both /_api and /_admin
  f.addHandler("/_admin/actions",
               RestHandlerCreator<MaintenanceRestHandler>::createNoData);

  f.addHandler("/_admin/auth/reload",
               RestHandlerCreator<RestAuthReloadHandler>::createNoData);

  f.addHandler("/_admin/compact",
               RestHandlerCreator<RestCompactHandler>::createNoData);

  f.addPrefixHandler(
    "/_api/job",
    RestHandlerCreator<sdb::RestJobHandler>::createData<AsyncJobManager*>,
    _job_manager.get());

  f.addPrefixHandler("/_api/transaction",
                     RestHandlerCreator<RestTransactionHandler>::createNoData);

  f.addPrefixHandler("/_api/ttl",
                     RestHandlerCreator<RestTtlHandler>::createNoData);

  // ...........................................................................
  // /_admin
  // ...........................................................................

  f.addPrefixHandler(
    "/_admin/cluster",
    RestHandlerCreator<sdb::RestAdminClusterHandler>::createNoData);

  if (_options_api_policy != "disabled") {
    f.addHandler("/_admin/options",
                 RestHandlerCreator<RestOptionsHandler>::createNoData);
    f.addHandler(
      "/_admin/options-description",
      RestHandlerCreator<RestOptionsDescriptionHandler>::createNoData);
  }

  f.addPrefixHandler(
    "/_admin/job",
    RestHandlerCreator<sdb::RestJobHandler>::createData<AsyncJobManager*>,
    _job_manager.get());

  // further admin handlers

  f.addPrefixHandler(
    "/_admin/log", RestHandlerCreator<sdb::RestAdminLogHandler>::createNoData);

  f.addHandler(
    "/_admin/supervisionState",
    RestHandlerCreator<sdb::RestSupervisionStateHandler>::createNoData);

  f.addPrefixHandler(
    "/_admin/shutdown",
    RestHandlerCreator<sdb::RestShutdownHandler>::createNoData);

  if (authentication.isActive()) {
    f.addPrefixHandler("/_open/auth",
                       RestHandlerCreator<sdb::RestAuthHandler>::createNoData);
  }

  f.addPrefixHandler(
    "/_admin/server",
    RestHandlerCreator<sdb::RestAdminServerHandler>::createNoData);

  f.addHandler(
    "/_admin/statistics",
    RestHandlerCreator<sdb::RestAdminStatisticsHandler>::createNoData);

  f.addHandler(
    "/_admin/statistics-description",
    RestHandlerCreator<sdb::RestAdminStatisticsHandler>::createNoData);

  f.addPrefixHandler("/_admin/metrics",
                     RestHandlerCreator<sdb::RestMetricsHandler>::createNoData);

  f.addPrefixHandler(
    "/_admin/usage-metrics",
    RestHandlerCreator<sdb::RestUsageMetricsHandler>::createNoData);

  auto& backup = server().getFeature<BackupFeature>();
  if (backup.isAPIEnabled()) {
    f.addPrefixHandler(
      "/_admin/backup",
      RestHandlerCreator<sdb::RestBackupHandler>::createNoData);
  }

  f.addPrefixHandler("/", RestHandlerCreator<RestNotImplHandler>::createNoData);

  // engine specific handlers
  StorageEngine& engine = server().getFeature<EngineFeature>().engine();
  engine.addRestHandlers(f);
#endif
}

}  // namespace sdb
