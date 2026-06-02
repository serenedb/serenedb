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
#include "basics/encoding_utils.h"
#include "basics/lifecycle.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "database/ticks.h"
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
        SDB_WARN(
          GENERAL,
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
  app::AppServer& server, GeneralRequest& req) {
  // get database name from request
  if (req.databaseName().empty()) {
    // if no database name was specified in the request, use system database
    // name as a fallback
    req.setDatabaseName(StaticStrings::kDefaultDatabase);
  }

  return catalog::CatalogFeature::instance()
    .Global()
    .GetCatalogSnapshot()
    ->GetDatabase(req.databaseName());
}

bool ResolveRequestContext(app::AppServer& server, GeneralRequest& req) {
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
    _writing(false) {}

template<SocketType T>
void GeneralCommTask<T>::LogRequestHeaders(
  std::string_view protocol,
  const containers::FlatHashMap<std::string, std::string>& headers) const {
  std::string headers_for_logging =
    basics::string_utils::HeadersToString(headers);
  SDB_TRACE(HTTP, "\"", protocol, "-request-headers\",\"",
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

  SDB_TRACE(HTTP, "\"", protocol, (is_response ? "-response" : "-request"),
            "-body\",\"", std::bit_cast<size_t>(this), "\",\"",
            ContentTypeToString(content_type), "\",\"", body.size(), "\",\"",
            body_for_logging, "\"");
}

template<SocketType T>
void GeneralCommTask<T>::LogResponseHeaders(
  std::string_view protocol,
  const containers::FlatHashMap<std::string, std::string>& headers) const {
  std::string headers_for_logging =
    basics::string_utils::HeadersToString(headers);
  SDB_TRACE(HTTP, "\"", protocol, "-response-headers\",\"",
            std::bit_cast<size_t>(this), "\",\"", headers_for_logging, "\"");
}

/// decompress content
template<SocketType T>
Result GeneralCommTask<T>::HandleContentEncoding(GeneralRequest& req) {
  // TODO consider doing the decoding on the fly
  auto decode = [&](const std::string& header,
                    const std::string& encoding) -> Result {
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
  GeneralRequest& req, ServerState::Mode /*mode*/) {
  // Auth is intentionally absent until post-RBAC. Every HTTP request
  // is treated as Superuser; the Authorization header (if any) is
  // ignored.
  auto entry = auth::TokenCache::Entry::Superuser();
  req.setAuthenticated(true);
  req.setUser(entry.username());
  return entry;
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
      SDB_DEBUG(HTTP, "handling CORS response for origin '", origin, "'");

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
  auto factory = GeneralServerFeature::instance().handlerFactory();
  auto handler =
    factory->createHandler(server, std::move(request), std::move(response));

  // give up, if we cannot find a handler
  if (handler == nullptr) {
    SendSimpleResponse(ResponseCode::NotFound, resp_type, message_id,
                       vpack::BufferUInt8());
    return;
  }

  if (mode == ServerState::Mode::Startup) {
    HandleRequestStartup(std::move(handler));
    return;
  }

  // forward to correct server if necessary
  bool forwarded;
  auto res = handler->forwardRequest(forwarded);
  if (forwarded) {
    std::move(res).DetachInline(
      [self(this->shared_from_this()),
       h(std::move(handler))](yaclib::Result<Result>&& /*ignored*/) -> void {
        auto gct = static_cast<GeneralCommTask<T>*>(self.get());
        gct->SendResponse(h->stealResponse());
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

  // asynchronous request
  if (found && (async_exec == "true" || async_exec == "store")) {
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
      SendResponse(std::move(resp));
    } else {
      SendErrorResponse(ResponseCode::ServiceUnavailable, resp_type, message_id,
                        ERROR_QUEUE_FULL);
    }
  } else {
    // synchronous request -- HandleRequestSync adds an error response
    HandleRequestSync(std::move(handler));
  }
}

/// Must be called before calling ExecuteRequest, will send an error
/// response if execution is supposed to be aborted
template<SocketType T>
Flow GeneralCommTask<T>::PrepareExecution(
  const auth::TokenCache::Entry& /*auth_token*/, GeneralRequest& req,
  ServerState::Mode /*mode*/) {
  if (lifecycle::IsStopping()) {
    this->SendErrorResponse(ResponseCode::ServiceUnavailable,
                            req.contentTypeResponse(), req.messageId(),
                            ERROR_SHUTTING_DOWN);
    return Flow::Abort;
  }
  this->_is_user_request = true;  // single-node: every request is user-facing

  if (!ResolveRequestContext(this->_server.server(), req)) {
    SendErrorResponse(ResponseCode::NotFound, req.contentTypeResponse(),
                      req.messageId(), ERROR_SERVER_DATABASE_NOT_FOUND);
    return Flow::Abort;
  }
  SDB_ASSERT(req.requestContext() != nullptr);
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
    SendResponse(std::move(resp));
  } catch (...) {
    SDB_WARN(HTTP,
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
  const auto& gs = GeneralServerFeature::instance();
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
  SDB_DEBUG(HTTP, "Handling startup request ", std::bit_cast<size_t>(this),
            " on path ", handler->request()->requestPath(), " on lane ", lane);

  handler->trackQueueEnd();
  handler->trackTaskStart();

  handler->runHandler([self = this->shared_from_this()](RestHandler* handler) {
    handler->trackTaskEnd();
    try {
      // Pass the response to the io context
      static_cast<GeneralCommTask<T>*>(self.get())
        ->SendResponse(handler->stealResponse());
    } catch (...) {
      SDB_WARN(HTTP,
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
  RequestLane lane = handler->determineRequestLane();
  handler->trackQueueStart();
  // We just injected the request pointer before calling this method
  SDB_ASSERT(handler->request() != nullptr);
  SDB_DEBUG(HTTP, "Handling request ", std::bit_cast<size_t>(this), " on path ",
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
          ->SendResponse(handler->stealResponse());
      } catch (...) {
        SDB_WARN(HTTP,
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
  if (lifecycle::IsStopping()) {
    return false;
  }

  RequestLane lane = handler->determineRequestLane();
  handler->trackQueueStart();

  SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);

  if (job_id != nullptr) {
    auto& job_manager = GeneralServerFeature::instance().jobManager();
    try {
      // This will throw if a soft shutdown is already going on on a
      // coordinator. But this can also throw if we have an
      // out of memory situation, so we better handle this anyway.
      job_manager.initAsyncJob(handler);
    } catch (const std::exception& exc) {
      SDB_INFO(STARTUP, "Async job rejected, exception: ", exc.what());
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
Flow GeneralCommTask<T>::CanAccessPath(const auth::TokenCache::Entry& /*token*/,
                                       GeneralRequest& /*req*/) const {
  // Auth is intentionally absent until post-RBAC; every path is reachable.
  return Flow::Continue;
}

/// handle an OPTIONS request
template<SocketType T>
void GeneralCommTask<T>::ProcessCorsOptions(std::unique_ptr<GeneralRequest> req,
                                            const std::string& origin) {
  auto resp = CreateResponse(ResponseCode::Ok, req->messageId());
  resp->setHeaderNCIfNotSet(StaticStrings::kAllow, StaticStrings::kCorsMethods);

  if (!origin.empty()) {
    SDB_DEBUG(HTTP, "got CORS preflight request");
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

      SDB_TRACE(HTTP, "client requested validation of the following headers: ",
                allow_headers);
    }

    // set caching time (hard-coded value)
    resp->setHeaderNCIfNotSet(StaticStrings::kAccessControlMaxAge, "1800");
  }

  // discard request and send response
  SendResponse(std::move(resp));
}

template class GeneralCommTask<SocketType::Tcp>;
template class GeneralCommTask<SocketType::Ssl>;
template class GeneralCommTask<SocketType::Unix>;

}  // namespace sdb::rest
