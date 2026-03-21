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

#include <fuerte/message.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "basics/result.h"
#include "basics/result_or.h"
#include "basics/static_strings.h"
#include "general_server/request_lane.h"
#include "network/connection_pool.h"
#include "network/types.h"
#include "utils/coro_helper.h"

namespace vpack {

class Slice;

}  // namespace vpack
namespace sdb {

struct ShardID;

namespace network {

class ConnectionPool;

/// Response data structure
struct Response {
  // create an empty (failed) response
  Response() noexcept;

  Response(DestinationId&& destination, fuerte::Error error,
           std::unique_ptr<fuerte::Request>&& request,
           std::unique_ptr<fuerte::Response>&& response) noexcept;

  ~Response() = default;

  Response(Response&& other) noexcept = default;
  Response& operator=(Response&& other) noexcept = default;
  Response(const Response& other) = delete;
  Response& operator=(const Response& other) = delete;

  bool hasRequest() const noexcept { return _request != nullptr; }
  bool hasResponse() const noexcept { return _response != nullptr; }

  /// return a reference to the request object. will throw an exception
  /// if there is no valid request!
  fuerte::Request& request() const;

  /// return a reference to the response object. will throw an exception
  /// if there is no valid response!
  fuerte::Response& response() const;

#ifdef SDB_GTEST
  /// inject a different response - only use this from tests!
  void setResponse(std::unique_ptr<fuerte::Response> response);
#endif

  /// steal the response from here. this may return a unique_ptr
  /// containing a nullptr. it is the caller's responsibility to check that.
  [[nodiscard]] std::unique_ptr<fuerte::Response> stealResponse() noexcept;

  [[nodiscard]] bool ok() const noexcept {
    return fuerte::Error::NoError == this->error;
  }

  [[nodiscard]] bool fail() const noexcept { return !ok(); }

  // returns a slice of the payload if there was no error
  [[nodiscard]] vpack::Slice slice() const noexcept;

  [[nodiscard]] size_t payloadSize() const noexcept;

  fuerte::StatusCode statusCode() const noexcept;

  /// Build a Result that contains
  ///   - no error if everything went well, otherwise
  ///   - the error from the body, if available, otherwise
  ///   - the HTTP error, if available, otherwise
  ///   - the fuerte error, if there was a connectivity error.
  Result combinedResult() const;

  ResultOr<ShardID> destinationShard() const;  /// shardId or empty
  [[nodiscard]] std::string serverId() const;  /// server ID

 public:
  DestinationId destination;
  fuerte::Error error;

 private:
  std::unique_ptr<fuerte::Request> _request;
  std::unique_ptr<fuerte::Response> _response;
};

static_assert(std::is_nothrow_move_constructible<Response>::value, "");
using FutureRes = yaclib::Future<Response>;

static constexpr Timeout kTimeoutDefault = Timeout(120.0);

// Container for optional (often defaulted) parameters
struct RequestOptions {
  std::string database;
  std::string content_type;  // uses vpack by default
  std::string accept_type;   // uses vpack by default
  fuerte::StringMap parameters;
  Timeout timeout = kTimeoutDefault;
  // retry if answer is "datasource not found"
  bool retry_not_found = false;
  // do not use Scheduler queue
  bool skip_scheduler = false;
  // send x-serene-hlc header with outgoing request, so that that peer can
  // update its own HLC value to at least the value of our HLC
  bool send_hlc_header = true;
  // transparently handle content-encoding. enabling this will automatically
  // uncompress responses that have the `Content-Encoding: gzip|deflate` header
  // set.
  bool handle_content_encoding = true;
  // allow to compress the request
  bool allow_compression = true;
  RequestLane continuation_lane = RequestLane::Continuation;

  // Normally this is empty, if it is set to the ID of a server in the
  // cluster, we will direct a read operation to a shard not as usual to
  // the leader, but rather to the server given here. This is read for
  // the "allowDirtyReads" options when we want to read from followers.
  std::string override_destination;

  template<typename K, typename V>
  RequestOptions& param(K&& key, V&& val) {
    SDB_ASSERT(!std::string_view{val}.empty());  // cannot parse it on receiver
    this->parameters.insert_or_assign(std::forward<K>(key),
                                      std::forward<V>(val));
    return *this;
  }
};

/// send a request to a given destination
/// This method must not throw under penalty of ...
FutureRes SendRequest(ConnectionPool* pool, DestinationId destination,
                      fuerte::RestVerb type, std::string path,
                      vpack::BufferUInt8 payload = {},
                      const RequestOptions& options = {}, Headers headers = {});

FutureRes SendRequest(ConnectionPool& pool, DestinationId destination,
                      fuerte::RestVerb type, std::string path,
                      std::span<uint8_t> payload,
                      const RequestOptions& options = {}, Headers headers = {});

/// send a request to a given destination, retry under certain conditions
/// a retry will be triggered if the connection was lost our could not be
/// established optionally a retry will be performed in the case of a "not
/// found" response until timeout is exceeded. This method must not throw
/// under penalty of ...
/// Note that we cannot automatically retry if the connection broke in
/// the middle of the request or if a timeout has happened, since then
/// we cannot know if the request has been sent and executed or not.
FutureRes SendRequestRetry(ConnectionPool* pool, DestinationId destination,
                           fuerte::RestVerb type, std::string path,
                           vpack::BufferUInt8 payload = {},
                           const RequestOptions& options = {},
                           Headers headers = {});

using Sender = std::function<FutureRes(const DestinationId&, fuerte::RestVerb,
                                       const std::string&, vpack::BufferUInt8,
                                       const RequestOptions& options, Headers)>;

}  // namespace network
}  // namespace sdb
