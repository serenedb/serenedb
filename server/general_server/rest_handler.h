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

#include <atomic>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>

#include "basics/common.h"
#include "basics/result_or.h"
#include "general_server/request_lane.h"
#include "metrics/gauge_counter_guard.h"
#include "rest/general_response.h"
#include "statistics/request_statistics.h"

namespace sdb {
namespace app {

class AppServer;
}
namespace basics {

class Exception;
}

class GeneralRequest;
class RequestStatistics;
class Result;

enum class RestStatus {
  Done,
  Waiting,
  Fail,
};

namespace rest {

class RestHandler : public std::enable_shared_from_this<RestHandler> {
  friend class CommTask;

  RestHandler(const RestHandler&) = delete;
  RestHandler& operator=(const RestHandler&) = delete;

 public:
  RestHandler(SerenedServer&, GeneralRequest*, GeneralResponse*);
  virtual ~RestHandler();

  void assignHandlerId();
  uint64_t handlerId() const noexcept { return _handler_id; }
  uint64_t messageId() const;

  /// called when the handler is queued for execution in the scheduler
  void trackQueueStart() noexcept;

  /// called when the handler is dequeued in the scheduler
  void trackQueueEnd() noexcept;

  /// called when the handler execution is started
  void trackTaskStart() noexcept;

  /// called when the handler execution is finalized
  void trackTaskEnd() noexcept;

  const GeneralRequest* request() const { return _request.get(); }
  GeneralResponse* response() const { return _response.get(); }
  std::unique_ptr<GeneralResponse> stealResponse() {
    return std::move(_response);
  }

  SerenedServer& server() noexcept { return _server; }
  const SerenedServer& server() const noexcept { return _server; }

  [[nodiscard]] const RequestStatistics::Item& requestStatistics()
    const noexcept {
    return _statistics;
  }
  [[nodiscard]] RequestStatistics::Item&& StealRequestStatistics();
  void SetRequestStatistics(RequestStatistics::Item&& stat);

  void setIsAsyncRequest() noexcept { _is_async_request = true; }

  /// Execute the rest handler state machine
  void runHandler(std::function<void(rest::RestHandler*)> response_callback);

  /// Execute the rest handler state machine. Retry the wakeup,
  /// returns true if _state == PAUSED, false otherwise
  bool wakeupHandler();

  /// forwards the request to the appropriate server
  yaclib::Future<Result> forwardRequest(bool& forwarded);

  void handleExceptionPtr(std::exception_ptr) noexcept;

 public:
  // rest handler name for debugging and logging
  virtual const char* name() const = 0;

  // what lane to use for this request
  virtual RequestLane lane() const = 0;

  RequestLane determineRequestLane();

  virtual void prepareExecute(bool is_continue);
  virtual RestStatus execute();
  virtual yaclib::Future<> executeAsync();
  virtual RestStatus continueExecute() { return RestStatus::Done; }
  virtual void shutdownExecute(bool is_finalized) noexcept;

  // you might need to implment this in your handler
  // if it will be executed in an async job
  virtual void cancel() { _canceled.store(true); }

  virtual void handleError(const basics::Exception&) = 0;

 protected:
  /// determines the possible forwarding target for this request
  ///
  /// This method will be called to determine if the request should be
  /// forwarded to another server, and if so, which server. If it should be
  /// handled by this server, the method should return an empty string.
  /// Otherwise, this method should return a valid short name for the
  /// target server.
  /// std::string -> empty string or valid short name
  /// boolean -> should auth header and user be removed in that request
  virtual ResultOr<std::pair<std::string, bool>> forwardingTarget() {
    return {std::pair{std::string{}, false}};
  }

  void resetResponse(rest::ResponseCode);

  void generateError(rest::ResponseCode code, ErrorCode error_number,
                     std::string_view error_message);

  // generates an error
  void generateError(rest::ResponseCode code, ErrorCode error_number);

  // generates an error
  void generateError(const sdb::Result&);

  [[nodiscard]] RestStatus waitForFuture(yaclib::Future<>&& f);
  [[nodiscard]] RestStatus waitForFuture(yaclib::Future<RestStatus>&& f);

  enum class HandlerState : uint8_t {
    Prepare = 0,
    Execute,
    Paused,
    Continued,
    Finalize,
    Done,
    Failed,
  };

  /// handler state machine
  HandlerState state() const { return _state; }

 private:
  void runHandlerStateMachine();

  void prepareEngine();
  /// Executes the RestHandler
  ///        May set the state to PAUSED, FINALIZE or FAILED
  ///        If isContinue == true it will call continueExecute()
  ///        otherwise execute() will be called
  void executeEngine(bool is_continue);
  void compressResponse();

 protected:
  std::unique_ptr<GeneralRequest> _request;
  std::unique_ptr<GeneralResponse> _response;
  SerenedServer& _server;
  RequestStatistics::Item _statistics;

 private:
  mutable absl::Mutex _execution_mutex;
  mutable std::atomic_uint8_t _execution_counter{0};
  mutable RestStatus _followup_rest_status;

  std::function<void(rest::RestHandler*)> _send_response_callback;

  uint64_t _handler_id;

  HandlerState _state;
  // whether or not we have tracked this task as ongoing.
  // can only be true during handler execution, and only for
  // low priority tasks
  bool _tracked_as_ongoing_low_prio;

  // whether or not the handler handles a request for the async
  // job api (/_api/job) or the batch API (/_api/batch)
  bool _is_async_request = false;

  RequestLane _lane;

 protected:
  metrics::GaugeCounterGuard<uint64_t> _current_requests_size_tracker;

  std::atomic<bool> _canceled;
};

}  // namespace rest
}  // namespace sdb
