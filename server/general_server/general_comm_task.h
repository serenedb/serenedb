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

#include "general_server/comm_task.h"
#include "general_server/generic_comm_task.h"

namespace sdb::rest {

inline constexpr size_t kMaximalBodySize = 1024 * 1024 * 1024;  // 1024 MB
inline constexpr double kWriteTimeout = 300.0;

enum class Flow : bool { Continue = true, Abort = false };
//
// The flow of events is as follows:
//
// (1) The start() method is called, each subclass is responsible for reading
//     data from the socket.
//
// (2) As soon as the task detects that it has read a complete request,
//     it must create an instance of a sub-class of `GeneralRequest` and
//     `GeneralResponse`. Then it must call `ExecuteRequest(...)` to start the
//     execution of the request.
//
// (3) `ExecuteRequest(...)` will create a handler. A handler is responsible for
//     executing the request. It will take the `request` instance and executes a
//     plan to generate a response. It is possible, that one request generates a
//     response and still does some work afterwards. It is even possible, that a
//     request generates a push stream.
//
//     As soon as a response is available, `sendResponse()` will be called.
//     which must be implemented in the sub-class.
//     It will be called with an response object and an indicator if
//     an error has occurred.
//
//     It is the responsibility of the sub-class to govern what is supported.
//     For example, HTTP will only support one active request executing at a
//     time until the final response has been send out.
//
//     VPack on the other hand, allows multiple active requests. Partial
//     responses are identified by a request id.
//
// (4) Error handling: In case of an error `addErrorResponse()` will be
//     called. This will call `sendResponse()` with an error indicator, which in
//     turn will end the responding request.
//
template<SocketType T>
class GeneralCommTask : public GenericCommTask<T, CommTask> {
 public:
  GeneralCommTask(GeneralServer& server, ConnectionInfo,
                  std::shared_ptr<AsioSocket<T>>);

  ~GeneralCommTask() override = default;

  /// send the response to the client.
  virtual void SendResponse(std::unique_ptr<GeneralResponse>,
                            RequestStatistics::Item) = 0;
  void SetRequestStatistics(uint64_t id, RequestStatistics::Item&& stat);

 protected:
  [[nodiscard]] ConnectionStatistics::Item AcquireConnectionStatistics();
  [[nodiscard]] RequestStatistics::ItemView AcquireRequestStatistics(
    uint64_t id);
  [[nodiscard]] RequestStatistics::ItemView GetRequestStatistics(uint64_t id);
  [[nodiscard]] RequestStatistics::Item StealRequestStatistics(uint64_t id);

  virtual std::unique_ptr<GeneralResponse> CreateResponse(
    rest::ResponseCode, uint64_t message_id) = 0;

  /// handle an OPTIONS request, will send response
  void ProcessCorsOptions(std::unique_ptr<GeneralRequest> req,
                          const std::string& origin);

  void HandleRequestStartup(std::shared_ptr<RestHandler>);
  void HandleRequestSync(std::shared_ptr<RestHandler>);
  bool HandleRequestAsync(std::shared_ptr<RestHandler>,
                          uint64_t* job_id = nullptr);

  bool AllowCorsCredentials(const std::string& origin) const;

  /// check authentication headers
  auth::TokenCache::Entry CheckAuthHeader(GeneralRequest& request,
                                          ServerState::Mode mode);

  /// Must be called before calling ExecuteRequest, will add an error
  /// response if execution is supposed to be aborted
  Flow PrepareExecution(const auth::TokenCache::Entry&, GeneralRequest&,
                        ServerState::Mode mode);

  /// Push this request into the execution pipeline
  void ExecuteRequest(std::unique_ptr<GeneralRequest> request,
                      std::unique_ptr<GeneralResponse> response,
                      ServerState::Mode mode);

  /// Must be called from sendResponse, before response is rendered
  void FinishExecution(GeneralResponse&, const std::string& cors) const;

  /// send response including error response body
  void SendErrorResponse(rest::ResponseCode, rest::ContentType,
                         uint64_t message_id, ErrorCode error_num,
                         std::string_view error_message = {});

  /// send simple response including response body
  void SendSimpleResponse(rest::ResponseCode, rest::ContentType,
                          uint64_t message_id, vpack::BufferUInt8&&);

  /// decompress content
  Result HandleContentEncoding(GeneralRequest&);

  void LogRequestHeaders(
    std::string_view protocol,
    const containers::FlatHashMap<std::string, std::string>& headers) const;
  void LogRequestBody(std::string_view protocol,
                      sdb::rest::ContentType content_type,
                      std::string_view body, bool is_response = false) const;
  void LogResponseHeaders(
    std::string_view protocol,
    const containers::FlatHashMap<std::string, std::string>& headers) const;

  bool _writing;
  ConnectionStatistics::Item _connection_statistics;

 private:
  ////////////////////////////////////////////////////////////////////////////////
  /// checks the access rights for a specified path, includes automatic
  ///        exceptions for /_api/users to allow logins without authorization
  ////////////////////////////////////////////////////////////////////////////////
  Flow CanAccessPath(const auth::TokenCache::Entry&, GeneralRequest&) const;

  mutable absl::Mutex _statistics_mutex;
  containers::FlatHashMap<uint64_t, RequestStatistics::Item> _statistics_map;
};

}  // namespace sdb::rest
