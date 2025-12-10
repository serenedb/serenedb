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

#include "general_comm_task.h"

#include "app/app_server.h"
#include "auth/role_utils.h"
#include "basics/dtrace-wrapper.h"
#include "basics/encoding_utils.h"
#include "basics/hybrid_logical_clock.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "database/ticks.h"
#include "general_server/authentication_feature.h"
#include "general_server/general_server.h"
#include "general_server/general_server_feature.h"
#include "general_server/rest_handler.h"
#include "general_server/scheduler_feature.h"
#include "rest/general_request.h"
#include "rest/general_response.h"
#include "rest_server/database_context.h"

namespace sdb::rest {
namespace {

// some static URL path prefixes
constexpr std::string_view kPathPrefixApiUser = "/_api/user/";
constexpr std::string_view kPathPrefixOpen = "/_open/";

bool QueueTimeViolated(const GeneralRequest& req) {
  // check if the client sent the "x-serene-queue-time-seconds" header
  bool found = false;
  const std::string& queue_time_value =
    req.header(StaticStrings::kXSereneQueueTimeSeconds, found);
  if (found) {
    // yes, now parse the sent time value. if the value sent by client cannot be
    // parsed as a double, then it will be handled as if "0.0" was sent - i.e.
    // no queuing time restriction
    double requested_queue_time =
      basics::string_utils::DoubleDecimal(queue_time_value);
    if (requested_queue_time > 0.0) {
      SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
      // value is > 0.0, so now check the last dequeue time that the scheduler
      // reported
      double last_dequeue_time =
        static_cast<double>(
          SchedulerFeature::gScheduler->getLastLowPriorityDequeueTime()) /
        1000.0;

      if (last_dequeue_time > requested_queue_time) {
        // the log topic should actually be REQUESTS here, but the default log
        // level for the REQUESTS log topic is FATAL, so if we logged here in
        // INFO level, it would effectively be suppressed. thus we are using the
        // Scheduler's log topic here, which is somewhat related.
        SchedulerFeature::gScheduler->trackQueueTimeViolation();
        SDB_WARN(
          "xxxxx", Logger::THREADS,
          "dropping incoming request because the client-specified maximum "
          "queue time requirement (",
          requested_queue_time, "s) would be violated by current queue time (",
          last_dequeue_time, "s)");
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<catalog::Database> LookupDatabaseFromRequest(
  SerenedServer& server, GeneralRequest& req) {
  // get database name from request
  if (ServerState::instance()->IsDBServer() || req.databaseName().empty()) {
    // if no database name was specified in the request, use system database
    // name as a fallback
    req.setDatabaseName(StaticStrings::kSystemDatabase);
  }

  return server.getFeature<catalog::CatalogFeature>().Global().GetDatabase(
    req.databaseName());
}

bool ResolveRequestContext(SerenedServer& server, GeneralRequest& req) {
  auto database = LookupDatabaseFromRequest(server, req);
  // invalid database name specified, database not found etc.
  if (!database) {
    return false;
  }

  auto context = DatabaseContext::create(req, std::move(database));
  if (!context) {
    return false;
  }

  // the DatabaseContext is now responsible for releasing the database
  req.setRequestContext(std::move(context));
  // the "true" means the request is the owner of the context
  return true;
}

}  // namespace

template<SocketType T>
GeneralCommTask<T>::GeneralCommTask(GeneralServer& server, ConnectionInfo info,
                                    std::shared_ptr<AsioSocket<T>> socket)
  : GenericCommTask<T, CommTask>(server, std::move(info), socket),
    _auth(AuthenticationFeature::instance()),
    _writing(false),
    _connection_statistics(AcquireConnectionStatistics()) {
  SDB_ASSERT(_auth != nullptr);
  _connection_statistics.SET_START();
}

template<SocketType T>
void GeneralCommTask<T>::LogRequestHeaders(
  std::string_view protocol,
  const containers::FlatHashMap<std::string, std::string>& headers) const {
  std::string headers_for_logging =
    basics::string_utils::HeadersToString(headers);
  SDB_TRACE("xxxxx", Logger::REQUESTS, "\"", protocol, "-request-headers\",\"",
            std::bit_cast<size_t>(this), "\",\"", headers_for_logging, "\"");
}

template<SocketType T>
void GeneralCommTask<T>::LogRequestBody(std::string_view protocol,
                                        ContentType content_type,
                                        std::string_view body,
                                        bool is_response) const {
  std::string body_for_logging;
  if (content_type != ContentType::VPack) {
    body_for_logging = basics::string_utils::EscapeUnicode(body);
  } else {
    try {
      vpack::Slice s{reinterpret_cast<const uint8_t*>(body.data())};
      if (!s.isNone()) {
        // "none" can happen if the content-type is neither JSON nor vpack
        body_for_logging = basics::string_utils::EscapeUnicode(s.toJson());
      }
    } catch (...) {
      // cannot stringify request body
    }

    if (body_for_logging.empty() && !body.empty()) {
      body_for_logging = "potential binary data";
    }
  }

  SDB_TRACE("xxxxx", Logger::REQUESTS, "\"", protocol,
            (is_response ? "-response" : "-request"), "-body\",\"",
            std::bit_cast<size_t>(this), "\",\"",
            ContentTypeToString(content_type), "\",\"", body.size(), "\",\"",
            body_for_logging, "\"");
}

template<SocketType T>
void GeneralCommTask<T>::LogResponseHeaders(
  std::string_view protocol,
  const containers::FlatHashMap<std::string, std::string>& headers) const {
  std::string headers_for_logging =
    basics::string_utils::HeadersToString(headers);
  SDB_TRACE("xxxxx", Logger::REQUESTS, "\"", protocol, "-response-headers\",\"",
            std::bit_cast<size_t>(this), "\",\"", headers_for_logging, "\"");
}

/// decompress content
template<SocketType T>
Result GeneralCommTask<T>::HandleContentEncoding(GeneralRequest& req) {
  // TODO consider doing the decoding on the fly
  auto decode = [&](const std::string& header,
                    const std::string& encoding) -> Result {
    if (this->_auth->isActive() && !req.authenticated() &&
        !this->_general_server_feature
           .handleContentEncodingForUnauthenticatedRequests()) {
      return {ERROR_FORBIDDEN,
              "support for handling Content-Encoding headers is turned off for "
              "unauthenticated requests"};
    }

    std::string_view raw = req.rawPayload();
    uint8_t* src = reinterpret_cast<uint8_t*>(const_cast<char*>(raw.data()));
    size_t len = raw.size();
    if (encoding == StaticStrings::kEncodingGzip) {
      vpack::BufferUInt8 dst;
      if (ErrorCode error = encoding::GZipUncompress(src, len, dst);
          error != ERROR_OK) {
        return {
          error,
          "a decoding error occurred while handling Content-Encoding: gzip"};
      }
      req.setPayload(std::move(dst));
      // as we have decoded, remove the encoding header.
      // this prevents duplicate decoding
      req.removeHeader(header);
      return {};
    } else if (encoding == StaticStrings::kEncodingDeflate) {
      vpack::BufferUInt8 dst;
      if (ErrorCode error = encoding::ZLibInflate(src, len, dst);
          error != ERROR_OK) {
        return {error,
                "a decoding error occurred while handling Content-Encoding: "
                "deflate"};
      }
      req.setPayload(std::move(dst));
      // as we have decoded, remove the encoding header.
      // this prevents duplicate decoding
      req.removeHeader(header);
      return {};
    } else if (encoding == StaticStrings::kEncodingSereneLz4) {
      vpack::BufferUInt8 dst;
      if (ErrorCode r = encoding::Lz4Uncompress(src, len, dst); r != ERROR_OK) {
        return {
          r, "a decoding error occurred while handling Content-Encoding: lz4"};
      }
      req.setPayload(std::move(dst));
      // as we have decoded, remove the encoding header.
      // this prevents duplicate decoding
      req.removeHeader(header);
      return {};
    }

    return {};
  };

  bool found;
  if (const std::string& val =
        req.header(StaticStrings::kTransferEncoding, found);
      found) {
    return decode(StaticStrings::kTransferEncoding, val);
  }

  if (const std::string& val =
        req.header(StaticStrings::kContentEncoding, found);
      found) {
    return decode(StaticStrings::kContentEncoding, val);
  }
  return {};
}

template<SocketType T>
auth::TokenCache::Entry GeneralCommTask<T>::CheckAuthHeader(
  GeneralRequest& req, ServerState::Mode mode) {
  bool found;
  const std::string& auth_str =
    req.header(StaticStrings::kAuthorization, found);
  if (!found) {
    if (_auth->isActive()) {
      return auth::TokenCache::Entry::Unauthenticated();
    }
    return auth::TokenCache::Entry::Superuser();
  }

  std::string::size_type method_pos = auth_str.find(' ');
  if (method_pos == std::string::npos) {
    return auth::TokenCache::Entry::Unauthenticated();
  }

  // skip over authentication method and following whitespace
  const char* auth = auth_str.c_str() + method_pos;
  while (*auth == ' ') {
    ++auth;
  }

  SDB_DEBUG_IF("xxxxx", Logger::REQUESTS, log::GetLogRequestParameters(),
               "\"authorization-header\",\"", reinterpret_cast<size_t>(this),
               "\",SENSITIVE_DETAILS_HIDDEN");

  AuthenticationMethod auth_method = AuthenticationMethod::None;
  if (auth_str.size() >= 6) {
    if (strncasecmp(auth_str.c_str(), "basic ", 6) == 0) {
      auth_method = AuthenticationMethod::Basic;
    } else if (strncasecmp(auth_str.c_str(), "bearer ", 7) == 0) {
      auth_method = AuthenticationMethod::Jwt;
    }
  }

  req.setAuthenticationMethod(auth_method);
  if (auth_method == AuthenticationMethod::None) {
    return auth::TokenCache::Entry::Unauthenticated();
  }

  if (auth_method != AuthenticationMethod::Jwt &&
      mode == ServerState::Mode::Startup) {
    // during startup, the normal authentication is not available
    // yet. so we have to refuse all requests that do not use
    // a JWT.
    return auth::TokenCache::Entry::Unauthenticated();
  }

  auto auth_token =
    _auth->tokenCache().checkAuthentication(auth_method, mode, auth);
  req.setAuthenticated(auth_token.authenticated());
  req.setTokenExpiry(auth_token.expiry());
  req.setUser(auth_token.username());  // do copy here, so that we do not
  return auth_token;
}

/// Must be called from sendResponse, before response is rendered
template<SocketType T>
void GeneralCommTask<T>::FinishExecution(GeneralResponse& res,
                                         const std::string& origin) const {
  if (this->_is_user_request) {
    // CORS response handling - only needed on user facing coordinators
    // or single servers
    if (!origin.empty()) {
      // the request contained an Origin header. We have to send back the
      // access-control-allow-origin header now
      SDB_DEBUG("xxxxx", Logger::REQUESTS,
                "handling CORS response for origin '", origin, "'");

      // send back original value of "Origin" header
      res.setHeaderNCIfNotSet(StaticStrings::kAccessControlAllowOrigin, origin);

      // send back "Access-Control-Allow-Credentials" header
      res.setHeaderNCIfNotSet(
        StaticStrings::kAccessControlAllowCredentials,
        (AllowCorsCredentials(origin) ? "true" : "false"));

      res.setHeaderNCIfNotSet(StaticStrings::kAccessControlExposeHeaders,
                              StaticStrings::kExposedCorsHeaders);
    }

    // DB server is not user-facing, and does not need to set this header
    // use "IfNotSet" to not overwrite an existing response header
    res.setHeaderNCIfNotSet(StaticStrings::kXContentTypeOptions,
                            StaticStrings::kNoSniff);

    // CSP Headers for security.
    res.setHeaderNCIfNotSet(StaticStrings::kContentSecurityPolicy,
                            "frame-ancestors 'self'; form-action 'self';");
    res.setHeaderNCIfNotSet(
      StaticStrings::kCacheControl,
      "no-cache, no-store, must-revalidate, pre-check=0, post-check=0, "
      "max-age=0, s-maxage=0");
    res.setHeaderNCIfNotSet(StaticStrings::kPragma, "no-cache");
    res.setHeaderNCIfNotSet(StaticStrings::kExpires, "0");
    res.setHeaderNCIfNotSet(StaticStrings::kHsts,
                            "max-age=31536000 ; includeSubDomains");

    // add "x-serene-queue-time-seconds" header
    if (this->_general_server_feature.returnQueueTimeHeader()) {
      SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
      res.setHeaderNC(
        StaticStrings::kXSereneQueueTimeSeconds,
        std::to_string(
          static_cast<double>(
            SchedulerFeature::gScheduler->getLastLowPriorityDequeueTime()) /
          1000.0));
    }
  }
}

/// Push this request into the execution pipeline
template<SocketType T>
void GeneralCommTask<T>::ExecuteRequest(
  std::unique_ptr<GeneralRequest> request,
  std::unique_ptr<GeneralResponse> response, ServerState::Mode mode) {
  SDB_ASSERT(request != nullptr);
  SDB_ASSERT(response != nullptr);

  if (request == nullptr || response == nullptr) {
    SDB_THROW(ERROR_INTERNAL, "invalid object setup for ExecuteRequest");
  }

  DTRACE_PROBE1(serened, CommTaskExecuteRequest, this);

  response->setContentTypeRequested(request->contentTypeResponse());
  response->setGenerateBody(request->requestType() != RequestType::Head);

  // store the message id for error handling
  uint64_t message_id = request->messageId();

  const ContentType resp_type = request->contentTypeResponse();

  // check if "x-serene-queue-time-seconds" header was set, and its value
  // is above the current dequeing time
  if (QueueTimeViolated(*request)) {
    SendErrorResponse(ResponseCode::PreconditionFailed, resp_type, message_id,
                      ERROR_QUEUE_TIME_REQUIREMENT_VIOLATED);
    return;
  }

  bool found;
  // check for an async request (before the handler steals the request)
  const std::string& async_exec = request->header(StaticStrings::kAsync, found);

  // create a handler, this takes ownership of request and response
  auto& server = this->_server.server();
  auto factory =
    server.template getFeature<GeneralServerFeature>().handlerFactory();
  auto handler =
    factory->createHandler(server, std::move(request), std::move(response));

  // give up, if we cannot find a handler
  if (handler == nullptr) {
    SendSimpleResponse(ResponseCode::NotFound, resp_type, message_id,
                       vpack::BufferUInt8());
    return;
  }

  if (mode == ServerState::Mode::Startup) {
    // request during startup phase
    handler->SetRequestStatistics(StealRequestStatistics(message_id));
    HandleRequestStartup(std::move(handler));
    return;
  }

  // forward to correct server if necessary
  bool forwarded;
  auto res = handler->forwardRequest(forwarded);
  if (forwarded) {
    GetRequestStatistics(message_id).SET_SUPERUSER();
    std::move(res).DetachInline(
      [self(this->shared_from_this()), h(std::move(handler)),
       message_id](yaclib::Result<Result>&& /*ignored*/) -> void {
        auto gct = static_cast<GeneralCommTask<T>*>(self.get());
        gct->SendResponse(h->stealResponse(),
                          gct->StealRequestStatistics(message_id));
      });
    return;
  }
  SDB_ASSERT(res.Ready());
  if (const auto& rr = std::as_const(res).Touch(); rr && rr.Ok().fail()) {
    auto& r = rr.Ok();
    SendErrorResponse(GeneralResponse::responseCode(r.errorNumber()), resp_type,
                      message_id, r.errorNumber(), r.errorMessage());
    return;
  }

  SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
  SchedulerFeature::gScheduler->trackCreateHandlerTask();

  // asynchronous request
  if (found && (async_exec == "true" || async_exec == "store")) {
    RequestStatistics::Item stats = StealRequestStatistics(message_id);
    stats.SET_ASYNC();
    handler->SetRequestStatistics(std::move(stats));
    handler->setIsAsyncRequest();

    uint64_t job_id = 0;

    bool ok = false;
    if (async_exec == "store") {
      // persist the responses
      ok = HandleRequestAsync(std::move(handler), &job_id);
    } else {
      // don't persist the responses
      ok = HandleRequestAsync(std::move(handler));
    }

    SDB_IF_FAILURE("queueFull") {
      ok = false;
      job_id = 0;
    }

    if (ok) {
      // always return HTTP 202 Accepted
      auto resp = CreateResponse(ResponseCode::Accepted, message_id);
      if (job_id > 0) {
        // return the job id we just created
        resp->setHeaderNC(StaticStrings::kAsyncId,
                          basics::string_utils::Itoa(job_id));
      }
      SendResponse(std::move(resp), RequestStatistics::Item());
    } else {
      SendErrorResponse(ResponseCode::ServiceUnavailable, resp_type, message_id,
                        ERROR_QUEUE_FULL);
    }
  } else {
    // synchronous request
    handler->SetRequestStatistics(StealRequestStatistics(message_id));
    // HandleRequestSync adds an error response
    HandleRequestSync(std::move(handler));
  }
}

/// Must be called before calling ExecuteRequest, will send an error
/// response if execution is supposed to be aborted
template<SocketType T>
Flow GeneralCommTask<T>::PrepareExecution(
  const auth::TokenCache::Entry& auth_token, GeneralRequest& req,
  ServerState::Mode mode) {
  DTRACE_PROBE1(serened, CommTaskPrepareExecution, this);

  // Step 1: In the shutdown phase we simply return 503:
  if (this->_server.server().isStopping()) {
    this->SendErrorResponse(ResponseCode::ServiceUnavailable,
                            req.contentTypeResponse(), req.messageId(),
                            ERROR_SHUTTING_DOWN);
    return Flow::Abort;
  }

  this->_request_source = req.header(StaticStrings::kClusterCommSource);
  SDB_DEBUG_IF("xxxxx", Logger::REQUESTS, !this->_request_source.empty(),
               "\"request-source\",\"", reinterpret_cast<size_t>(this), "\",\"",
               this->_request_source, "\"");

  this->_is_user_request = std::invoke([&]() {
    auto role = ServerState::instance()->GetRole();
    if (ServerState::IsSingle(role)) {
      // single server is always user-facing
      return true;
    }
    if (ServerState::IsAgent(role) || ServerState::IsDBServer(role)) {
      // agents and DB servers are never user-facing
      return false;
    }

    SDB_ASSERT(ServerState::IsCoordinator(role));
    // coordinators are only user-facing if the request is not a
    // cluster-internal request
    return !ServerState::IsCoordinatorId(this->_request_source) &&
           !ServerState::IsDBServerId(this->_request_source);
  });

  // Step 2: Handle server-modes, i.e. bootstrap / DC2DC stunts
  const std::string& path = req.requestPath();

  bool allow_early_connections = this->_server.allowEarlyConnections();

  switch (mode) {
    case ServerState::Mode::Startup: {
      if (!allow_early_connections ||
          (_auth->isActive() && !req.authenticated())) {
        if (req.authenticationMethod() == AuthenticationMethod::Basic) {
          // HTTP basic authentication is not supported during the startup
          // phase, as we do not have any access to the database data. However,
          // we must return HTTP 503 because we cannot even verify the
          // credentials, and let the caller can try again later when the
          // authentication may be available.
          SendErrorResponse(ResponseCode::ServiceUnavailable,
                            req.contentTypeResponse(), req.messageId(),
                            ERROR_HTTP_SERVICE_UNAVAILABLE,
                            "service unavailable due to startup");
        } else {
          SendErrorResponse(ResponseCode::Unauthorized,
                            req.contentTypeResponse(), req.messageId(),
                            ERROR_FORBIDDEN);
        }
        return Flow::Abort;
      }

      // passed authentication!
      SDB_ASSERT(allow_early_connections);
      if (path == "/_api/version" || path == "/_admin/version" ||
#ifdef SDB_FAULT_INJECTION
          path.starts_with("/_admin/debug/") ||
#endif
          path == "/_admin/status") {
        return Flow::Continue;
      }
      // most routes are disallowed during startup, except the ones above.
      SendErrorResponse(ResponseCode::ServiceUnavailable,
                        req.contentTypeResponse(), req.messageId(),
                        ERROR_HTTP_SERVICE_UNAVAILABLE,
                        "service unavailable due to startup");
      return Flow::Abort;
    }

    case ServerState::Mode::Maintenance: {
      if (allow_early_connections &&
          (path == "/_api/version" || path == "/_admin/version" ||
#ifdef SDB_FAULT_INJECTION
           path.starts_with("/_admin/debug/") ||
#endif
           path == "/_admin/status")) {
        return Flow::Continue;
      }

      // In the bootstrap phase, we would like that coordinators answer the
      // following endpoints, but not yet others:
      if (!ServerState::instance()->IsCoordinator() ||
          !path.starts_with("/_api/aql")) {
        SendErrorResponse(
          ResponseCode::ServiceUnavailable, req.contentTypeResponse(),
          req.messageId(), ERROR_HTTP_SERVICE_UNAVAILABLE,
          "service unavailable due to startup or maintenance mode");
        return Flow::Abort;
      }
      break;
    }
    case ServerState::Mode::Default:
    case ServerState::Mode::Invalid:
      // no special handling required
      break;
  }

  // Step 3: Try to resolve database and use
  if (!ResolveRequestContext(this->_server.server(),
                             req)) {  // false if db not found
    if (_auth->isActive()) {
      // prevent guessing database names (issue #5030)
      auth::Level lvl = auth::Level::None;
      if (req.authenticated()) {
        // If we are authenticated and the user name is empty, then we must
        // have been authenticated with a superuser JWT token. In this case,
        // we must not check the databaseAuthLevel here.
        if (!req.user().empty()) {
          lvl = auth::DatabaseAuthLevel(req.user(), req.databaseName());
        } else {
          lvl = auth::Level::RW;
        }
      }
      if (lvl == auth::Level::None) {
        SendErrorResponse(ResponseCode::Unauthorized, req.contentTypeResponse(),
                          req.messageId(), ERROR_FORBIDDEN,
                          "not authorized to execute this request");
        return Flow::Abort;
      }
    }
    SendErrorResponse(ResponseCode::NotFound, req.contentTypeResponse(),
                      req.messageId(), ERROR_SERVER_DATABASE_NOT_FOUND);
    return Flow::Abort;
  }
  SDB_ASSERT(req.requestContext() != nullptr);

  // Step 4: Check the authentication. Will determine if the user can access
  // this path checks db permissions and contains exceptions for the
  // users API to allow logins
  if (CanAccessPath(auth_token, req) != Flow::Continue) {
    SendErrorResponse(ResponseCode::Unauthorized, req.contentTypeResponse(),
                      req.messageId(), ERROR_FORBIDDEN,
                      "not authorized to execute this request");
    return Flow::Abort;
  }

  // Step 5: Update global HLC timestamp from authenticated requests
  if (req.authenticated()) {  // TODO only from superuser ??
    // check for an HLC time stamp only with auth
    bool found;
    const std::string& time_stamp =
      req.header(StaticStrings::kHlcHeader, found);
    if (found) {
      uint64_t parsed = basics::HybridLogicalClock::decodeTimeStamp(time_stamp);
      if (parsed != 0 && parsed != UINT64_MAX) {
        NewTickHybridLogicalClock(parsed);
      }
    }
  }

  return Flow::Continue;
}

/// send error response including response body
template<SocketType T>
void GeneralCommTask<T>::SendSimpleResponse(ResponseCode code,
                                            ContentType resp_type, uint64_t mid,
                                            vpack::BufferUInt8&& buffer) {
  try {
    auto resp = CreateResponse(code, mid);
    resp->setContentType(resp_type);
    if (!buffer.empty()) {
      resp->setPayload(std::move(buffer), vpack::Options::gDefaults);
    }
    SendResponse(std::move(resp), StealRequestStatistics(mid));
  } catch (...) {
    SDB_WARN("xxxxx", Logger::REQUESTS,
             "addSimpleResponse received an exception, closing connection");
    this->Stop();
  }
}

/// send response including error response body
template<SocketType T>
void GeneralCommTask<T>::SendErrorResponse(
  ResponseCode code, ContentType resp_type, uint64_t message_id,
  ErrorCode error_num, std::string_view error_message /* = {} */) {
  vpack::BufferUInt8 buffer;
  vpack::Builder builder(buffer);
  builder.openObject();
  builder.add(StaticStrings::kError, error_num != ERROR_OK);
  builder.add(StaticStrings::kErrorNum, error_num.value());
  if (error_num != ERROR_OK) {
    if (error_message.data() == nullptr) {
      error_message = GetErrorStr(error_num);
    }
    SDB_ASSERT(error_message.data() != nullptr);
    builder.add(StaticStrings::kErrorMessage, error_message);
  }
  builder.add(StaticStrings::kCode, static_cast<int>(code));
  builder.close();

  SendSimpleResponse(code, resp_type, message_id, std::move(buffer));
}

/// deny credentialed requests or not (only CORS)
template<SocketType T>
bool GeneralCommTask<T>::AllowCorsCredentials(const std::string& origin) const {
  // default is to allow nothing
  bool allow_credentials = false;
  if (origin.empty()) {
    return allow_credentials;
  }  // else handle origin headers

  // if the request asks to allow credentials, we'll check against the
  // configured allowed list of origins
  const auto& gs =
    this->_server.server().template getFeature<GeneralServerFeature>();
  const std::vector<std::string>& access_control_allow_origins =
    gs.accessControlAllowOrigins();

  if (!access_control_allow_origins.empty()) {
    if (access_control_allow_origins[0] == "*") {
      // special case: allow everything
      allow_credentials = true;
    } else if (!origin.empty()) {
      // copy origin string
      if (origin[origin.size() - 1] == '/') {
        // strip trailing slash
        auto result = std::find(access_control_allow_origins.begin(),
                                access_control_allow_origins.end(),
                                origin.substr(0, origin.size() - 1));
        allow_credentials = (result != access_control_allow_origins.end());
      } else {
        auto result = std::find(access_control_allow_origins.begin(),
                                access_control_allow_origins.end(), origin);
        allow_credentials = (result != access_control_allow_origins.end());
      }
    } else {
      SDB_ASSERT(!allow_credentials);
    }
  }
  return allow_credentials;
}

// Handle a request during the server startup
template<SocketType T>
void GeneralCommTask<T>::HandleRequestStartup(
  std::shared_ptr<RestHandler> handler) {
  // We just injected the request pointer before calling this method
  SDB_ASSERT(handler->request() != nullptr);

  RequestLane lane = handler->determineRequestLane();
  ContentType resp_type = handler->request()->contentTypeResponse();
  uint64_t mid = handler->messageId();

  // only fast lane handlers are allowed during startup
  SDB_ASSERT(lane == RequestLane::ClientFast);
  if (lane != RequestLane::ClientFast) {
    SendErrorResponse(ResponseCode::ServiceUnavailable, resp_type, mid,
                      ERROR_HTTP_SERVICE_UNAVAILABLE,
                      "service unavailable due to startup");
    return;
  }
  // note that in addition to the CLIENT_FAST request lane, another
  // prerequisite for serving a request during startup is that the handler
  // is registered via GeneralServerFeature::defineInitialHandlers().
  // only the handlers listed there will actually be responded to.
  // requests to any other handlers will be responded to with HTTP 503.

  handler->trackQueueStart();
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "Handling startup request ",
            std::bit_cast<size_t>(this), " on path ",
            handler->request()->requestPath(), " on lane ", lane);

  handler->trackQueueEnd();
  handler->trackTaskStart();

  handler->runHandler([self = this->shared_from_this()](RestHandler* handler) {
    handler->trackTaskEnd();
    try {
      // Pass the response to the io context
      static_cast<GeneralCommTask<T>*>(self.get())
        ->SendResponse(handler->stealResponse(),
                       handler->StealRequestStatistics());
    } catch (...) {
      SDB_WARN("xxxxx", Logger::REQUESTS,
               "got an exception while sending response, closing connection");
      self->Stop();
    }
  });
}

// Execute a request by queueing it in the scheduler and having it executed via
// a scheduler worker thread eventually.
template<SocketType T>
void GeneralCommTask<T>::HandleRequestSync(
  std::shared_ptr<RestHandler> handler) {
  DTRACE_PROBE2(serened, CommTaskHandleRequestSync, this, handler.get());

  RequestLane lane = handler->determineRequestLane();
  handler->trackQueueStart();
  // We just injected the request pointer before calling this method
  SDB_ASSERT(handler->request() != nullptr);
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "Handling request ",
            std::bit_cast<size_t>(this), " on path ",
            handler->request()->requestPath(), " on lane ", lane);

  ContentType resp_type = handler->request()->contentTypeResponse();
  uint64_t mid = handler->messageId();

  // queue the operation for execution in the scheduler
  auto cb = [self = this->shared_from_this(),
             handler = std::move(handler)]() mutable {
    handler->trackQueueEnd();
    handler->trackTaskStart();

    handler->runHandler([self = std::move(self)](RestHandler* handler) {
      handler->trackTaskEnd();
      try {
        // Pass the response to the io context
        static_cast<GeneralCommTask<T>*>(self.get())
          ->SendResponse(handler->stealResponse(),
                         handler->StealRequestStatistics());
      } catch (...) {
        SDB_WARN("xxxxx", Logger::REQUESTS,
                 "got an exception while sending response, closing connection");
        self->Stop();
      }
    });
  };

  SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
  bool ok = SchedulerFeature::gScheduler->tryBoundedQueue(lane, std::move(cb));

  if (!ok) {
    SendErrorResponse(ResponseCode::ServiceUnavailable, resp_type, mid,
                      ERROR_QUEUE_FULL);
  }
}

// handle a request which came in with the x-serene-async header
template<SocketType T>
bool GeneralCommTask<T>::HandleRequestAsync(
  std::shared_ptr<RestHandler> handler, uint64_t* job_id) {
  if (this->_server.server().isStopping()) {
    return false;
  }

  RequestLane lane = handler->determineRequestLane();
  handler->trackQueueStart();

  SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);

  if (job_id != nullptr) {
    auto& job_manager = this->_server.server()
                          .template getFeature<GeneralServerFeature>()
                          .jobManager();
    try {
      // This will throw if a soft shutdown is already going on on a
      // coordinator. But this can also throw if we have an
      // out of memory situation, so we better handle this anyway.
      job_manager.initAsyncJob(handler);
    } catch (const std::exception& exc) {
      SDB_INFO("xxxxx", Logger::STARTUP,
               "Async job rejected, exception: ", exc.what());
      return false;
    }
    *job_id = handler->handlerId();

    // callback will persist the response with the AsyncJobManager
    return SchedulerFeature::gScheduler->tryBoundedQueue(
      lane, [handler = std::move(handler), manager(&job_manager)] {
        handler->trackQueueEnd();
        handler->trackTaskStart();

        handler->runHandler([manager](RestHandler* h) {
          h->trackTaskEnd();
          manager->finishAsyncJob(h);
        });
      });
  } else {
    // here the response will just be ignored
    return SchedulerFeature::gScheduler->tryBoundedQueue(
      lane, [handler = std::move(handler)] {
        handler->trackQueueEnd();
        handler->trackTaskStart();

        handler->runHandler([](RestHandler* h) { h->trackTaskEnd(); });
      });
  }
}

/// checks the access rights for a specified path
template<SocketType T>
Flow GeneralCommTask<T>::CanAccessPath(const auth::TokenCache::Entry& token,
                                       GeneralRequest& req) const {
  if (!_auth->isActive()) {
    // no authentication required at all
    return Flow::Continue;
  }

  const auto& path = req.requestPath();

  const auto& ap = token.allowedPaths();
  if (!ap.empty() && !absl::c_linear_search(ap, path)) {
    return Flow::Abort;
  }

  const bool user_authenticated = req.authenticated();
  Flow result = user_authenticated ? Flow::Continue : Flow::Abort;

  auto vc = basics::downCast<DatabaseContext>(req.requestContext());
  SDB_ASSERT(vc != nullptr);
  // deny access to database with NONE
  if (result == Flow::Continue &&
      vc->databaseAuthLevel() == auth::Level::None) {
    result = Flow::Abort;
    SDB_TRACE("xxxxx", Logger::AUTHORIZATION, "Access forbidden to ", path);
  }

  // we need to check for some special cases, where users may be allowed
  // to proceed even unauthorized
  if (result == Flow::Abort) {
#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS
    // check if we need to run authentication for this type of endpoint
    const auto& ci = req.connectionInfo();
    if (ci.endpoint_type == Endpoint::DomainType::UNIX &&
        !_auth->authenticationUnixSockets()) {
      // no authentication required for unix domain socket connections
      result = Flow::Continue;
    }
#endif

    if (result == Flow::Abort && _auth->authenticationSystemOnly()) {
      // authentication required, but only for /_api, /_admin etc.
      if ((!path.empty() && path[0] != '/') ||
          (path.size() > 1 && path[1] != '_')) {
        result = Flow::Continue;
        vc->forceSuperuser();
        SDB_TRACE("xxxxx", Logger::AUTHORIZATION, "Upgrading rights for ",
                  path);
      }
    }

    if (result == Flow::Abort) {
      const std::string& username = req.user();

      if (path == "/" || path.starts_with(kPathPrefixOpen) ||
          path == "/_admin/server/availability") {
        // mop: these paths are always callable...they will be able to check
        // req.user when it could be validated
        result = Flow::Continue;
        vc->forceSuperuser();
      } else if (user_authenticated && path == "/_api/cluster/endpoints") {
        // allow authenticated users to access cluster/endpoints
        result = Flow::Continue;
        // vc->forceReadOnly();
      } else if (req.requestType() == RequestType::Post && !username.empty() &&
                 path.starts_with(
                   absl::StrCat(kPathPrefixApiUser, username, "/"))) {
        // simon: unauthorized users should be able to call
        // `/_api/user/<name>` to check their passwords
        result = Flow::Continue;
        vc->forceReadOnly();
      } else if (user_authenticated && path.starts_with(kPathPrefixApiUser)) {
        result = Flow::Continue;
      }
    }
  }

  return result;
}

/// handle an OPTIONS request
template<SocketType T>
void GeneralCommTask<T>::ProcessCorsOptions(std::unique_ptr<GeneralRequest> req,
                                            const std::string& origin) {
  auto resp = CreateResponse(ResponseCode::Ok, req->messageId());
  resp->setHeaderNCIfNotSet(StaticStrings::kAllow, StaticStrings::kCorsMethods);

  if (!origin.empty()) {
    SDB_DEBUG("xxxxx", Logger::REQUESTS, "got CORS preflight request");
    const std::string_view allow_headers = basics::string_utils::Trim(
      req->header(StaticStrings::kAccessControlRequestHeaders));

    // send back which HTTP methods are allowed for the resource
    // we'll allow all
    resp->setHeaderNCIfNotSet(StaticStrings::kAccessControlAllowMethods,
                              StaticStrings::kCorsMethods);

    if (!allow_headers.empty()) {
      // allow all extra headers the client requested
      // we don't verify them here. the worst that can happen is that the
      // client sends some broken headers and then later cannot access the data
      // on the server. that's a client problem.
      resp->setHeaderNCIfNotSet(StaticStrings::kAccessControlAllowHeaders,
                                allow_headers);

      SDB_TRACE("xxxxx", Logger::REQUESTS,
                "client requested validation of the following headers: ",
                allow_headers);
    }

    // set caching time (hard-coded value)
    resp->setHeaderNCIfNotSet(StaticStrings::kAccessControlMaxAge,
                              StaticStrings::kN1800);
  }

  // discard request and send response
  SendResponse(std::move(resp), StealRequestStatistics(req->messageId()));
}

template<SocketType T>
void GeneralCommTask<T>::SetRequestStatistics(uint64_t id,
                                              RequestStatistics::Item&& stat) {
  std::lock_guard guard{_statistics_mutex};
  _statistics_map.insert_or_assign(id, std::move(stat));
}

template<SocketType T>
ConnectionStatistics::Item GeneralCommTask<T>::AcquireConnectionStatistics() {
  ConnectionStatistics::Item stat;
  if (this->_server.server()
        .template getFeature<StatisticsFeature>()
        .isEnabled()) {
    // only acquire a new item if the statistics are enabled.
    stat = ConnectionStatistics::acquire();
  }
  return stat;
}

template<SocketType T>
RequestStatistics::ItemView GeneralCommTask<T>::AcquireRequestStatistics(
  uint64_t id) {
  RequestStatistics::Item stat;
  if (this->_server.server()
        .template getFeature<StatisticsFeature>()
        .isEnabled()) {
    // only acquire a new item if the statistics are enabled.
    stat = RequestStatistics::acquire();
  }

  std::lock_guard guard(_statistics_mutex);
  return _statistics_map.insert_or_assign(id, std::move(stat)).first->second;
}

template<SocketType T>
RequestStatistics::ItemView GeneralCommTask<T>::GetRequestStatistics(
  uint64_t id) {
  std::lock_guard guard(_statistics_mutex);
  return _statistics_map[id];
}

template<SocketType T>
RequestStatistics::Item GeneralCommTask<T>::StealRequestStatistics(
  uint64_t id) {
  RequestStatistics::Item result;
  std::lock_guard guard(_statistics_mutex);

  auto iter = _statistics_map.find(id);
  if (iter != _statistics_map.end()) {
    result = std::move(iter->second);
    _statistics_map.erase(iter);
  }

  return result;
}

template class GeneralCommTask<SocketType::Tcp>;
template class GeneralCommTask<SocketType::Ssl>;
template class GeneralCommTask<SocketType::Unix>;

}  // namespace sdb::rest
