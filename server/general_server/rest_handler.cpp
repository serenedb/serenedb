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

#include "rest_handler.h"

#include <absl/strings/str_cat.h>
#include <fuerte/jwt.h>
#include <vpack/exception.h>

#include <source_location>

#include "app/app_server.h"
#include "auth/token_cache.h"
#include "basics/debugging.h"
#include "basics/dtrace-wrapper.h"
#include "basics/logger/logger.h"
#include "basics/recursive_locker.h"
#include "catalog/identifiers/transaction_id.h"
#include "database/ticks.h"
#include "general_server/authentication_feature.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "network/methods.h"
#include "network/network_feature.h"
#include "network/utils.h"
#include "rest/general_request.h"
#include "rest/http_response.h"
#include "statistics/request_statistics.h"
#include "utils/exec_context.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::rest;

RestHandler::RestHandler(SerenedServer& server, GeneralRequest* request,
                         GeneralResponse* response)
  : _request(request),
    _response(response),
    _server(server),
    _statistics(),
    _handler_id(0),
    _state(HandlerState::Prepare),
    _tracked_as_ongoing_low_prio(false),
    _lane(RequestLane::Undefined),
    _canceled(false) {
  if (server.hasFeature<GeneralServerFeature>() &&
      server.isEnabled<GeneralServerFeature>()) {
    _current_requests_size_tracker = metrics::GaugeCounterGuard<uint64_t>{
      server.getFeature<GeneralServerFeature>().current_requests_size,
      _request->memoryUsage()};
  }
}

RestHandler::~RestHandler() {
  if (_tracked_as_ongoing_low_prio) {
    // someone forgot to call trackTaskEnd
    SDB_ASSERT(PriorityRequestLane(determineRequestLane()) ==
               RequestPriority::Low);
    SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
    SchedulerFeature::gScheduler->trackEndOngoingLowPriorityTask();
  }
}

void RestHandler::assignHandlerId() { _handler_id = NewServerSpecificTick(); }

uint64_t RestHandler::messageId() const {
  uint64_t message_id = 0UL;
  auto req = _request.get();
  auto res = _response.get();
  if (req) {
    message_id = req->messageId();
  } else if (res) {
    message_id = res->messageId();
  } else {
    SDB_WARN("xxxxx", Logger::COMMUNICATION,
             "could not find corresponding request/response");
  }

  return message_id;
}

RequestLane RestHandler::determineRequestLane() {
  if (_lane != RequestLane::Undefined) {
    return _lane;
  }

  bool found = false;
  _request->header(StaticStrings::kXSereneFrontend, found);
  if (found || _request->requestPath() == "/") {
    return _lane = RequestLane::ClientUi;
  }

  _lane = lane();
  SDB_ASSERT(_lane != RequestLane::Undefined);
  if (PriorityRequestLane(_lane) != RequestPriority::Low) {
    return _lane;
  }

  // If this is a low-priority request, check if it contains a transaction id,
  // but is not the start of an AQL query or streaming transaction.
  // If we find out that the request is part of an already ongoing transaction,
  // we can now increase its priority, so that ongoing transactions can proceed.
  // However, we don't want to prioritize the start of new transactions here.
  found = false;
  const auto& value = _request->header(StaticStrings::kTransactionId, found);
  if (found) {
    auto tid = TransactionId::none();
    size_t pos = 0;
    try {
      tid = TransactionId{std::stoull(value, &pos, 10)};
    } catch (...) {
    }
    if (tid.isSet() && !value.ends_with(" begin") && !value.ends_with(" aql")) {
      // increase request priority from previously LOW to now MED.
      _lane = RequestLane::Continuation;
    }
  }
  return _lane;
}

void RestHandler::trackQueueStart() noexcept {
  SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
  _statistics.SET_QUEUE_START();
}

void RestHandler::trackQueueEnd() noexcept { _statistics.SET_QUEUE_END(); }

void RestHandler::trackTaskStart() noexcept {
  SDB_ASSERT(!_tracked_as_ongoing_low_prio);

  if (PriorityRequestLane(determineRequestLane()) == RequestPriority::Low) {
    SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
    SchedulerFeature::gScheduler->trackBeginOngoingLowPriorityTask();
    _tracked_as_ongoing_low_prio = true;
  }
}

void RestHandler::trackTaskEnd() noexcept {
  if (_tracked_as_ongoing_low_prio) {
    SDB_ASSERT(PriorityRequestLane(determineRequestLane()) ==
               RequestPriority::Low);
    SDB_ASSERT(SchedulerFeature::gScheduler != nullptr);
    SchedulerFeature::gScheduler->trackEndOngoingLowPriorityTask();
    _tracked_as_ongoing_low_prio = false;

    // update the time the last low priority item spent waiting in the queue.

    // the queueing time is in ms
    uint64_t queue_time_ms =
      static_cast<uint64_t>(_statistics.ELAPSED_WHILE_QUEUED() * 1000.0);
    SchedulerFeature::gScheduler->setLastLowPriorityDequeueTime(queue_time_ms);
  }
}

RequestStatistics::Item&& RestHandler::StealRequestStatistics() {
  return std::move(_statistics);
}

void RestHandler::SetRequestStatistics(RequestStatistics::Item&& stat) {
  _statistics = std::move(stat);
}

yaclib::Future<Result> RestHandler::forwardRequest(bool& forwarded) {
  forwarded = false;
  if (!ServerState::instance()->IsCoordinator()) {
    return yaclib::MakeFuture<Result>();
  }

  ResultOr forward_result = forwardingTarget();
  if (!forward_result) {
    return yaclib::MakeFuture(std::move(forward_result).error());
  }

  auto& forward_content = *forward_result;
  std::string server_id = std::get<0>(forward_content);
  bool remove_header = std::get<1>(forward_content);

  if (remove_header) {
    _request->removeHeader(StaticStrings::kAuthorization);
    _request->setUser("");
  }

  if (server_id.empty()) {
    // no need to actually forward
    return yaclib::MakeFuture<Result>();
  }

  NetworkFeature& nf = server().getFeature<NetworkFeature>();
  network::ConnectionPool* pool = nf.pool();
  if (pool == nullptr) {
    // nullptr happens only during controlled shutdown
    generateError(rest::ResponseCode::ServiceUnavailable, ERROR_SHUTTING_DOWN,
                  "shutting down server");
    return yaclib::MakeFuture<Result>(ERROR_SHUTTING_DOWN);
  }
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "forwarding request ",
            _request->messageId(), " to ", server_id);

  forwarded = true;

  const std::string& dbname = _request->databaseName();

  std::map<std::string, std::string> headers{_request->headers().begin(),
                                             _request->headers().end()};

  // always remove HTTP "Connection" header, so that we don't relay
  // "Connection: Close" or "Connection: Keep-Alive" or such
  headers.erase(StaticStrings::kConnection);

  if (headers.find(StaticStrings::kAuthorization) == headers.end()) {
    // No authorization header is set.
    // In this case, we have to produce a proper JWT token as authorization:
    auto auth = AuthenticationFeature::instance();
    if (auth != nullptr && auth->isActive()) {
      // when in superuser mode, username is empty
      // in this case ClusterComm will add the default superuser token
      const std::string& username = _request->user();
      if (!username.empty()) {
        headers.emplace(
          StaticStrings::kAuthorization,
          "bearer " + fuerte::jwt::GenerateUserToken(
                        auth->tokenCache().jwtSecret(), username));
      }
    }
  }

  network::RequestOptions options;
  options.database = dbname;
  options.timeout = network::Timeout(900);

  // if the type is unset JSON is used
  options.content_type = rest::ContentTypeToString(_request->contentType());

  options.accept_type =
    rest::ContentTypeToString(_request->contentTypeResponse());

  for (const auto& i : _request->values()) {
    options.param(i.first, i.second);
  }

  auto request_type = fuerte::FromString(
    GeneralRequest::translateMethod(_request->requestType()));

  std::string_view res_payload = _request->rawPayload();
  vpack::BufferUInt8 payload(res_payload.size());
  payload.append(res_payload.data(), res_payload.size());

  nf.trackForwardedRequest();

  auto future = network::SendRequestRetry(
    pool, "server:" + server_id, request_type, _request->requestPath(),
    std::move(payload), options, std::move(headers));
  auto cb = [this, server_id, self = shared_from_this()](
              network::Response&& response) -> Result {
    auto res = network::FuerteToSereneErrorCode(response);
    if (res != ERROR_OK) {
      generateError(res);
      return res;
    }

    resetResponse(static_cast<rest::ResponseCode>(response.statusCode()));
    _response->setContentType(
      fuerte::ToString(response.response().contentType()));

    HttpResponse* http_response = dynamic_cast<HttpResponse*>(_response.get());
    if (_response == nullptr) {
      SDB_THROW(ERROR_INTERNAL, "invalid response type");
    }
    http_response->body().Impl() = response.response().payloadAsString();

    const auto& result_headers = response.response().messageHeader().meta();
    for (const auto& it : result_headers) {
      if (it.first == "http/1.1") {
        // never forward this header, as the HTTP response code was already set
        // via "resetResponse" above
        continue;
      }
      _response->setHeader(it.first, it.second);
    }
    _response->setHeaderNC(StaticStrings::kRequestForwardedTo, server_id);

    return {};
  };
  return std::move(future).ThenInline(cb);
}

void RestHandler::handleExceptionPtr(std::exception_ptr eptr) noexcept try {
  auto build_exception = [this](ErrorCode code, std::string message,
                                std::source_location location) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "maintainer mode: ", message);
#endif
    Exception err(code, std::move(message), location);
    handleError(err);
  };

  try {
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const Exception& ex) {
    build_exception(
      ex.code(), absl::StrCat("caught exception in ", name(), ": ", ex.what()),
      std::source_location::current());
  } catch (const vpack::Exception& ex) {
    const bool is_parse_error =
      (ex.errorCode() == vpack::Exception::kParseError ||
       ex.errorCode() == vpack::Exception::kUnexpectedControlCharacter);
    build_exception(
      is_parse_error ? ERROR_HTTP_CORRUPTED_JSON : ERROR_INTERNAL,
      absl::StrCat("caught vpack error in ", name(), ": ", ex.what()),
      std::source_location::current());
  } catch (const std::bad_alloc& ex) {
    build_exception(
      ERROR_OUT_OF_MEMORY,
      absl::StrCat("caught memory exception in ", name(), ": ", ex.what()),
      std::source_location::current());
  } catch (const std::exception& ex) {
    build_exception(
      ERROR_INTERNAL,
      absl::StrCat("caught exception in ", name(), ": ", ex.what()),
      std::source_location::current());
  } catch (...) {
    build_exception(ERROR_INTERNAL,
                    absl::StrCat("caught unknown exception in ", name()),
                    std::source_location::current());
  }
} catch (...) {
  // we can only get here if putting together an error response or an
  // error log message failed with an exception. there is nothing we
  // can do here to signal this problem.
}

void RestHandler::runHandlerStateMachine() {
  // _execution_mutex has to be locked here
  SDB_ASSERT(_send_response_callback);

  while (true) {
    switch (_state) {
      case HandlerState::Prepare:
        prepareEngine();
        break;

      case HandlerState::Execute: {
        executeEngine(/*isContinue*/ false);
        if (_state == HandlerState::Paused) {
          shutdownExecute(false);
          SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
                    "Pausing rest handler execution ",
                    std::bit_cast<size_t>(this));
          return;  // stop state machine
        }
        break;
      }

      case HandlerState::Continued: {
        executeEngine(/*isContinue*/ true);
        if (_state == HandlerState::Paused) {
          shutdownExecute(/*isFinalized*/ false);
          SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
                    "Pausing rest handler execution ",
                    std::bit_cast<size_t>(this));
          return;  // stop state machine
        }
        break;
      }

      case HandlerState::Paused:
        SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
                  "Resuming rest handler execution ",
                  std::bit_cast<size_t>(this));
        _state = HandlerState::Continued;
        break;

      case HandlerState::Finalize:
        _statistics.SET_REQUEST_END();

        // shutdownExecute is noexcept
        shutdownExecute(true);  // may not be moved down

        _state = HandlerState::Done;

        // compress response if required
        compressResponse();
        // Callback may stealStatistics!
        _send_response_callback(this);
        break;

      case HandlerState::Failed:
        _statistics.SET_REQUEST_END();
        // Callback may stealStatistics!
        _send_response_callback(this);

        shutdownExecute(false);
        return;

      case HandlerState::Done:
        return;
    }
  }
}

void RestHandler::prepareEngine() {
  // set end immediately so we do not get negative statistics
  _statistics.SET_REQUEST_START_END();

  if (_canceled) {
    _state = HandlerState::Failed;

    Exception err(ERROR_REQUEST_CANCELED, std::source_location::current());
    handleError(err);
    return;
  }

  try {
    prepareExecute(false);
    _state = HandlerState::Execute;
    return;
  } catch (const Exception& ex) {
    handleError(ex);
  } catch (const std::exception& ex) {
    Exception err(ERROR_INTERNAL, ex.what(), std::source_location::current());
    handleError(err);
  } catch (...) {
    Exception err(ERROR_INTERNAL, std::source_location::current());
    handleError(err);
  }

  _state = HandlerState::Failed;
}

void RestHandler::prepareExecute(bool is_continue) {}

void RestHandler::shutdownExecute(bool is_finalized) noexcept {}

/// Execute the rest handler state machine. Retry the wakeup,
/// returns true if _state == PAUSED, false otherwise
bool RestHandler::wakeupHandler() {
  std::lock_guard lock{_execution_mutex};
  if (_state == HandlerState::Paused) {
    runHandlerStateMachine();
  }
  return _state == HandlerState::Paused;
}

void RestHandler::executeEngine(bool is_continue) {
  DTRACE_PROBE1(serened, RestHandlerExecuteEngine, this);
  try {
    RestStatus result = RestStatus::Done;
    if (is_continue) {
      // only need to run prepareExecute() again when we are continuing
      // otherwise prepareExecute() was already run in the PREPARE phase
      prepareExecute(true);
      result = continueExecute();
    } else {
      result = execute();
    }

    if (result == RestStatus::Waiting) {
      _state = HandlerState::Paused;  // wait for someone to continue the state
                                      // machine
      return;
    }

    if (_response == nullptr) {
      Exception err(ERROR_INTERNAL, "no response received from handler",
                    std::source_location::current());
      handleError(err);
    }

    _state = HandlerState::Finalize;
    return;
  } catch (const Exception& ex) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "maintainer mode: caught exception in ", name(), ": ", ex.what());
#endif
    handleError(ex);
  } catch (const vpack::Exception& ex) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "maintainer mode: caught vpack exception in ", name(), ": ",
             ex.what());
#endif
    const bool is_parse_error =
      (ex.errorCode() == vpack::Exception::kParseError ||
       ex.errorCode() == vpack::Exception::kUnexpectedControlCharacter);
    Exception err(is_parse_error ? ERROR_HTTP_CORRUPTED_JSON : ERROR_INTERNAL,
                  absl::StrCat("VPack error: ", ex.what()),
                  std::source_location::current());
    handleError(err);
  } catch (const std::bad_alloc& ex) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "maintainer mode: caught memory exception in ", name(), ": ",
             ex.what());
#endif
    Exception err(ERROR_OUT_OF_MEMORY, ex.what(),
                  std::source_location::current());
    handleError(err);
  } catch (const std::exception& ex) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "maintainer mode: caught exception in ", name(), ": ", ex.what());
#endif
    Exception err(ERROR_INTERNAL, ex.what(), std::source_location::current());
    handleError(err);
  } catch (...) {
#ifdef SDB_DEV
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "maintainer mode: caught unknown exception in ", name());
#endif
    Exception err(ERROR_INTERNAL, std::source_location::current());
    handleError(err);
  }

  _state = HandlerState::Failed;
}

void RestHandler::generateError(rest::ResponseCode code, ErrorCode error_number,
                                const std::string_view error_message) {
  resetResponse(code);

  if (_request->requestType() != rest::RequestType::Head) {
    vpack::BufferUInt8 buffer;
    vpack::Builder builder(buffer);
    try {
      builder.add(vpack::Value(vpack::ValueType::Object));
      builder.add(StaticStrings::kCode, static_cast<int>(code));
      builder.add(StaticStrings::kError, true);
      builder.add(StaticStrings::kErrorMessage, error_message);
      builder.add(StaticStrings::kErrorNum, error_number.value());
      builder.close();

      if (_request != nullptr) {
        _response->setContentType(_request->contentTypeResponse());
      }
      _response->setPayload(std::move(buffer), vpack::Options::gDefaults);
    } catch (...) {
      // exception while generating error
    }
  }
}

void RestHandler::compressResponse() {
  if (_is_async_request) {
    // responses to async requests are currently not compressed
    return;
  }

  rest::ResponseCompressionType rct = _response->compressionAllowed();
  if (rct == rest::ResponseCompressionType::NoCompression) {
    // compression explicitly disabled for the response
    return;
  }

  if (_request->acceptEncoding() == rest::EncodingType::Unset) {
    // client hasn't asked for compression
    return;
  }

  size_t body_size = _response->bodySize();
  if (body_size == 0) {
    // response body size of 0 does not need any compression
    return;
  }

  uint64_t threshold =
    server().getFeature<GeneralServerFeature>().compressResponseThreshold();

  if (threshold == 0) {
    // opted out of compression by configuration
    return;
  }

  // check if response is eligible for compression
  if (body_size < threshold) {
    // compression not necessary
    return;
  }

  if (_response->headers().contains(StaticStrings::kContentEncoding)) {
    // response is already content-encoded
    return;
  }

  SDB_ASSERT(body_size > 0);
  SDB_ASSERT(_request->acceptEncoding() != rest::EncodingType::Unset);

  switch (_request->acceptEncoding()) {
    case rest::EncodingType::Deflate:
      // the resulting compressed response body may be larger than the
      // uncompressed input size. in this case we are not returning the
      // compressed response body, but the original, uncompressed body.
      if (_response->ZLibDeflate(/*onlyIfSmaller*/ true) == ERROR_OK) {
        _response->setHeaderNC(StaticStrings::kContentEncoding,
                               StaticStrings::kEncodingDeflate);
      }
      break;

    case rest::EncodingType::GZip:
      // the resulting compressed response body may be larger than the
      // uncompressed input size. in this case we are not returning the
      // compressed response body, but the original, uncompressed body.
      if (_response->GZipCompress(/*onlyIfSmaller*/ true) == ERROR_OK) {
        _response->setHeaderNC(StaticStrings::kContentEncoding,
                               StaticStrings::kEncodingGzip);
      }
      break;

    case rest::EncodingType::Lz4:
      // the resulting compressed response body may be larger than the
      // uncompressed input size. in this case we are not returning the
      // compressed response body, but the original, uncompressed body.
      if (_response->Lz4Compress(/*onlyIfSmaller*/ true) == ERROR_OK) {
        _response->setHeaderNC(StaticStrings::kContentEncoding,
                               StaticStrings::kEncodingSereneLz4);
      }
      break;

    default:
      break;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// generates an error
////////////////////////////////////////////////////////////////////////////////

void RestHandler::generateError(rest::ResponseCode code,
                                ErrorCode error_number) {
  const auto message = GetErrorStr(error_number);

  if (message.data() != nullptr) {
    generateError(code, error_number, message);
  } else {
    generateError(code, error_number, "unknown error");
  }
}

// generates an error
void RestHandler::generateError(const sdb::Result& r) {
  ResponseCode code = GeneralResponse::responseCode(r.errorNumber());
  generateError(code, r.errorNumber(), r.errorMessage());
}

RestStatus RestHandler::waitForFuture(yaclib::Future<>&& f) {
  if (f.Ready()) {                            // fast-path out
    std::ignore = std::move(f).Touch().Ok();  // just throw the error upwards
    return RestStatus::Done;
  }
  SDB_ASSERT(_execution_counter == 0);
  _execution_counter = 2;
  std::move(f).DetachInline(
    [self = shared_from_this()](yaclib::Result<>&& t) -> void {
      if (t.State() == yaclib::ResultState::Exception) {
        self->handleExceptionPtr(std::move(t).Exception());
      }
      if (--self->_execution_counter == 0) {
        self->wakeupHandler();
      }
    });
  return --_execution_counter == 0 ? RestStatus::Done : RestStatus::Waiting;
}

RestStatus RestHandler::waitForFuture(yaclib::Future<RestStatus>&& f) {
  if (f.Ready()) {                     // fast-path out
    return std::move(f).Touch().Ok();  // just throw the error upwards
  }
  SDB_ASSERT(_execution_counter == 0);
  _execution_counter = 2;
  std::move(f).DetachInline(
    [self = shared_from_this()](yaclib::Result<RestStatus>&& t) -> void {
      if (t.State() == yaclib::ResultState::Exception) {
        self->handleExceptionPtr(std::move(t).Exception());
        self->_followup_rest_status = RestStatus::Done;
      } else {
        self->_followup_rest_status = std::move(t).Ok();
        if (self->_followup_rest_status == RestStatus::Waiting) {
          return;  // rest handler will be woken up externally
        }
      }
      if (--self->_execution_counter == 0) {
        self->wakeupHandler();
      }
    });
  return --_execution_counter == 0 ? _followup_rest_status
                                   : RestStatus::Waiting;
}

void RestHandler::resetResponse(rest::ResponseCode code) {
  SDB_ASSERT(_response != nullptr);
  _response->reset(code);
}

yaclib::Future<> RestHandler::executeAsync() {
  SDB_THROW(ERROR_NOT_IMPLEMENTED);
}

RestStatus RestHandler::execute() { return waitForFuture(executeAsync()); }

void RestHandler::runHandler(
  std::function<void(rest::RestHandler*)> response_callback) {
  SDB_ASSERT(_state == HandlerState::Prepare);
  _send_response_callback = std::move(response_callback);
  std::lock_guard guard(_execution_mutex);
  runHandlerStateMachine();
}
