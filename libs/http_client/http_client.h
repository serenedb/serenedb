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
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/string_buffer.h"
#include "rest/common_defines.h"
#include "rest/general_request.h"

namespace sdb {
namespace app {

class CommunicationFeaturePhase;
}

namespace httpclient {

class HttpResult;
class GeneralClientConnection;

struct HttpClientParams {
  friend class HttpClient;

  HttpClientParams(double request_timeout, bool warn,
                   bool add_content_length = true)
    : _request_timeout(request_timeout),
      _warn(warn),
      _add_content_length(add_content_length),
      _location_rewriter({nullptr, nullptr}) {}

  //////////////////////////////////////////////////////////////////////////////
  /// leave connection open on destruction
  //////////////////////////////////////////////////////////////////////////////

  void keepConnectionOnDestruction(bool b) noexcept {
    _keep_connection_on_destruction = b;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// enable or disable keep-alive
  //////////////////////////////////////////////////////////////////////////////

  void setKeepAlive(bool value) noexcept { _keep_alive = value; }

  //////////////////////////////////////////////////////////////////////////////
  /// expose SereneDB via user-agent?
  //////////////////////////////////////////////////////////////////////////////

  void setExposeSereneDB(bool value) noexcept { _expose_serene_db = value; }

  void setCompressRequestThreshold(uint64_t value) noexcept {
    _compress_request_threshold = value;
  }

  void setAllowCompressedResponses(bool value) noexcept {
    _allow_compressed_responses = value;
  }

  void setMaxRetries(size_t s) noexcept { _max_retries = s; }

  size_t getMaxRetries() const noexcept { return _max_retries; }

  void setRetryWaitTime(uint64_t wt) noexcept { _retry_wait_time = wt; }

  uint64_t getRetryWaitTime() const noexcept { return _retry_wait_time; }

  void setRetryMessage(std::string_view m) { _retry_message = m; }

  double getRequestTimeout() const noexcept { return _request_timeout; }

  void setRequestTimeout(double value) noexcept { _request_timeout = value; }

  void setMaxPacketSize(size_t ms) noexcept { _max_packet_size = ms; }

  //////////////////////////////////////////////////////////////////////////////
  /// sets username and password
  ///
  /// prefix                         prefix for sending username and
  /// password
  /// username                       username
  /// password                       password
  //////////////////////////////////////////////////////////////////////////////

  void setJwt(std::string_view jwt) { _jwt = jwt; }

  // sets username and password
  void setUserNamePassword(std::string_view prefix, std::string_view username,
                           std::string_view password);

  //////////////////////////////////////////////////////////////////////////////
  /// allows rewriting locations
  //////////////////////////////////////////////////////////////////////////////

  void setLocationRewriter(const void* data,
                           std::string (*func)(const void*, std::string_view)) {
    _location_rewriter.data = data;
    _location_rewriter.func = func;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// set the value for max packet size
  //////////////////////////////////////////////////////////////////////////////

  static void setDefaultMaxPacketSize(size_t value) { gMaxPacketSize = value; }

 private:
  double _request_timeout;

  // flag whether or not we keep the connection on destruction
  bool _keep_connection_on_destruction = false;

  bool _warn;

  bool _add_content_length = true;

  bool _keep_alive = true;

  bool _expose_serene_db = true;

  // if true, then the HttpClient will advertise via the HTTP header
  // "accept-encoding: deflate" that it supports handling compressed response
  // bodies.
  bool _allow_compressed_responses = true;

  size_t _max_retries = 3;

  uint64_t _retry_wait_time = 1 * 1000 * 1000;

  std::string _retry_message;

  size_t _max_packet_size = HttpClientParams::gMaxPacketSize;

  // compress request bodies if their size is >= this value. disabled by
  // default.
  uint64_t _compress_request_threshold = 0;

  std::string _basic_auth;

  std::string _jwt;

  //////////////////////////////////////////////////////////////////////////////
  /// struct for rewriting location URLs
  //////////////////////////////////////////////////////////////////////////////

  struct {
    const void* data;
    std::string (*func)(const void*, std::string_view);
  } _location_rewriter;

  // default value for max packet size
  static size_t gMaxPacketSize;
};

////////////////////////////////////////////////////////////////////////////////
/// simple http client
////////////////////////////////////////////////////////////////////////////////

class HttpClient {
 private:
  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;

 public:
  //////////////////////////////////////////////////////////////////////////////
  /// state of the connection
  //////////////////////////////////////////////////////////////////////////////

  enum RequestState {
    kInConnect,
    kInWrite,
    kInReadHeader,
    kInReadBody,
    kInReadChunkedHeader,
    kInReadChunkedBody,
    kFinished,
    kDead
  };

  HttpClient(std::unique_ptr<GeneralClientConnection>&,
             const HttpClientParams&);
  HttpClient(GeneralClientConnection*, const HttpClientParams&);
  ~HttpClient();

  /// allow the HttpClient to reuse/recycle the result.
  /// the HttpClient will assume ownership for it
  void recycleResult(std::unique_ptr<HttpResult> result);

  void setInterrupted(bool value);

  RequestState state() const { return _state; }

  //////////////////////////////////////////////////////////////////////////////
  /// invalidates the connection used by the client
  /// this may be called from other objects that are responsible for managing
  /// connections. after this method has been called, the client must not be
  /// used for any further HTTP operations, but should be destroyed instantly.
  //////////////////////////////////////////////////////////////////////////////

  void invalidateConnection() { _connection = nullptr; }

  //////////////////////////////////////////////////////////////////////////////
  /// checks if the connection is open
  //////////////////////////////////////////////////////////////////////////////

  bool isConnected();

  //////////////////////////////////////////////////////////////////////////////
  /// checks if the connection is open
  //////////////////////////////////////////////////////////////////////////////

  void disconnect();

  //////////////////////////////////////////////////////////////////////////////
  /// returns a string representation of the connection endpoint
  //////////////////////////////////////////////////////////////////////////////

  std::string getEndpointSpecification() const;

  //////////////////////////////////////////////////////////////////////////////
  /// close connection, go to state IN_CONNECT and clear the input
  /// buffer. This is used to organize a retry of the connection.
  //////////////////////////////////////////////////////////////////////////////

  void close();

  //////////////////////////////////////////////////////////////////////////////
  /// make an http request, creating a new HttpResult object
  /// the caller has to delete the result object
  /// this version does not allow specifying custom headers
  /// if the request fails because of connection problems, the request will be
  /// retried until it either succeeds (at least no connection problem) or there
  /// have been _max_retries retries
  //////////////////////////////////////////////////////////////////////////////

  HttpResult* retryRequest(
    rest::RequestType, std::string_view, std::string_view,
    const containers::FlatHashMap<std::string, std::string>&);

  //////////////////////////////////////////////////////////////////////////////
  /// make an http request, creating a new HttpResult object
  /// the caller has to delete the result object
  /// this version does not allow specifying custom headers
  /// if the request fails because of connection problems, the request will be
  /// retried until it either succeeds (at least no connection problem) or there
  /// have been _max_retries retries
  //////////////////////////////////////////////////////////////////////////////

  HttpResult* retryRequest(rest::RequestType, std::string_view,
                           std::string_view);

  //////////////////////////////////////////////////////////////////////////////
  /// make an http request, creating a new HttpResult object
  /// the caller has to delete the result object
  /// this version does not allow specifying custom headers
  //////////////////////////////////////////////////////////////////////////////

  HttpResult* request(rest::RequestType, std::string_view, std::string_view);

  //////////////////////////////////////////////////////////////////////////////
  /// make an http request, actual worker function
  /// the caller has to delete the result object
  /// this version allows specifying custom headers
  //////////////////////////////////////////////////////////////////////////////

  HttpResult* request(rest::RequestType, std::string_view, std::string_view,
                      const containers::FlatHashMap<std::string, std::string>&);

  //////////////////////////////////////////////////////////////////////////////
  /// returns the current error message
  //////////////////////////////////////////////////////////////////////////////

  const auto& getErrorMessage() const { return _error_message; }

  //////////////////////////////////////////////////////////////////////////////
  /// register and dump an error message
  //////////////////////////////////////////////////////////////////////////////

  void setErrorMessage(std::string_view message, bool force_warn = false) {
    _error_message = message;

    if (_params._warn || force_warn) {
      SDB_WARN("xxxxx", sdb::Logger::HTTPCLIENT, "", _error_message);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  /// register an error message
  //////////////////////////////////////////////////////////////////////////////

  void setErrorMessage(std::string_view message, ErrorCode error);

  //////////////////////////////////////////////////////////////////////////////
  /// checks whether an error message is already there
  //////////////////////////////////////////////////////////////////////////////

  bool haveErrorMessage() const { return _error_message.size() > 0; }

  //////////////////////////////////////////////////////////////////////////////
  /// fetch the version from the server
  //////////////////////////////////////////////////////////////////////////////

  std::string getServerVersion(ErrorCode* error_code = nullptr);

  //////////////////////////////////////////////////////////////////////////////
  /// extract an error message from a response
  //////////////////////////////////////////////////////////////////////////////

  std::string getHttpErrorMessage(const HttpResult* result,
                                  ErrorCode* error_code = nullptr);

  HttpClientParams& params() { return _params; }

  /// Thread-safe check abortion status
  bool isAborted() const noexcept {
    return _aborted.load(std::memory_order_acquire);
  }

  /// Thread-safe set abortion status
  void setAborted(bool value) noexcept;

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// make a http request, creating a new HttpResult object
  /// the caller has to delete the result object
  /// this version allows specifying custom headers
  //////////////////////////////////////////////////////////////////////////////

  HttpResult* doRequest(
    rest::RequestType, std::string_view, std::string_view,
    const containers::FlatHashMap<std::string, std::string>&);

  //////////////////////////////////////////////////////////////////////////////
  /// initialize the connection
  //////////////////////////////////////////////////////////////////////////////

  void handleConnect();

  //////////////////////////////////////////////////////////////////////////////
  /// clearReadBuffer, clears the read buffer as well as the result
  //////////////////////////////////////////////////////////////////////////////

  void clearReadBuffer();

  //////////////////////////////////////////////////////////////////////////////
  /// rewrite a location URL
  //////////////////////////////////////////////////////////////////////////////

  std::string rewriteLocation(std::string_view location) {
    if (_params._location_rewriter.func != nullptr) {
      return _params._location_rewriter.func(_params._location_rewriter.data,
                                             location);
    }

    return std::string{location};
  }

  //////////////////////////////////////////////////////////////////////////////
  /// set the type of the result
  //////////////////////////////////////////////////////////////////////////////

  void setResultType(bool have_sent_request);

  //////////////////////////////////////////////////////////////////////////////
  /// set the request
  ///
  /// method                         request method
  /// location                       request uri
  /// body                           request body
  /// bodyLength                     size of body
  /// headerFields                   list of header fields
  //////////////////////////////////////////////////////////////////////////////

  ErrorCode setRequest(
    rest::RequestType method, std::string_view location, std::string_view body,
    const containers::FlatHashMap<std::string, std::string>& header_fields);

  //////////////////////////////////////////////////////////////////////////////
  /// process (a part of) the http header, the data is
  /// found in _read_buffer starting at _read_buffer_offset until
  /// _read_buffer.length().
  //////////////////////////////////////////////////////////////////////////////

  void processHeader();

  //////////////////////////////////////////////////////////////////////////////
  /// process (a part of) the body, read the http body by content length
  /// Note that when this is called, the content length of the body has always
  /// been set, either by finding a value in the HTTP header or by reading
  /// from the network until nothing more is found. The data is found in
  /// _read_buffer starting at _read_buffer_offset until _read_buffer.length().
  //////////////////////////////////////////////////////////////////////////////

  void processBody();

  //////////////////////////////////////////////////////////////////////////////
  /// process the chunk size of the next chunk (i.e. the chunk header),
  /// this is called when processing the body of a chunked transfer. The
  /// data is found in _read_buffer at position _read_buffer_offset until
  /// _read_buffer.length().
  /// Note that this method and processChunkedBody() call each other when
  /// they complete, counting on the fact that in a single transfer the
  /// number of chunks found is not so large to run into deep recursion
  /// problems.
  //////////////////////////////////////////////////////////////////////////////

  void processChunkedHeader();

  //////////////////////////////////////////////////////////////////////////////
  /// process the next chunk (i.e. the chunk body), this is called when
  /// processing the body of a chunked transfer. The data is found in
  /// _read_buffer at position _read_buffer_offset until _read_buffer.length().
  /// Note that this method and processChunkedHeader() call each other when
  /// they complete, counting on the fact that in a single transfer the
  /// number of chunks found is not so large to run into deep recursion
  /// problems.
  //////////////////////////////////////////////////////////////////////////////

  void processChunkedBody();

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// connection used (TCP or SSL connection)
  //////////////////////////////////////////////////////////////////////////////

  GeneralClientConnection* _connection;

  // flag whether or not to delete the connection on destruction
  bool _delete_connection_on_destruction = false;

  //////////////////////////////////////////////////////////////////////////////
  /// connection parameters
  //////////////////////////////////////////////////////////////////////////////
  HttpClientParams _params;

  //////////////////////////////////////////////////////////////////////////////
  /// write buffer
  //////////////////////////////////////////////////////////////////////////////

  sdb::basics::StringBuffer _write_buffer;

  //////////////////////////////////////////////////////////////////////////////
  /// read buffer
  //////////////////////////////////////////////////////////////////////////////

  sdb::basics::StringBuffer _read_buffer;

  //////////////////////////////////////////////////////////////////////////////
  /// read buffer offset
  ///
  /// _state == IN_READ_BODY:
  ///     points to the beginning of the body
  ///
  /// _state == IN_READ_HEADER:
  ///     points to the beginning of the next header line
  ///
  /// _state == FINISHED:
  ///     points to the beginning of the next request
  ///
  /// _state == IN_READ_CHUNKED_HEADER:
  ///     points to the beginning of the next size line
  ///
  /// _state == IN_READ_CHUNKED_BODY:
  ///     points to the beginning of the next body
  //////////////////////////////////////////////////////////////////////////////

  size_t _read_buffer_offset;

  RequestState _state;

  size_t _written;

  std::string _error_message;

  uint32_t _next_chunked_size;

  rest::RequestType _method;

  std::unique_ptr<HttpResult> _result;

  std::atomic<bool> _aborted;

  std::string _hostname;

  // reference to communication feature phase (populated only once for
  // the entire lifetime of the HttpClient, as the repeated feature
  // lookup may be expensive otherwise)
  app::CommunicationFeaturePhase& _comm;
};

}  // namespace httpclient
}  // namespace sdb
