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

#include "http_client.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "app/app_server.h"
#include "app/communication_phase.h"
#include "basics/containers/small_vector.h"
#include "basics/encoding_utils.h"
#include "basics/error_code.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_buffer.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "basics/system-functions.h"
#include "endpoint/endpoint.h"
#include "http_client/general_client_connection.h"
#include "http_client/http_result.h"
#include "rest/general_response.h"

using namespace sdb;
using namespace sdb::basics;

namespace sdb::httpclient {
namespace {

using InflateFunc = ErrorCode (*)(const uint8_t* uncompressed,
                                  size_t uncompressed_length,
                                  StringBuffer& compressed);

ErrorCode Inflate(InflateFunc func, std::string_view in, StringBuffer& out,
                  size_t skip) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(in.data());
  size_t length = in.size();

  if (length < skip) {
    length = 0;
  } else {
    p += skip;
    length -= skip;
  }

  return func(p, length, out);
}

}  // namespace

void HttpClientParams::setUserNamePassword(std::string_view prefix,
                                           std::string_view username,
                                           std::string_view password) {
  SDB_ASSERT(prefix == "/");
  _basic_auth = absl::Base64Escape(absl::StrCat(username, ":", password));
}

/// default value for max packet size
size_t HttpClientParams::gMaxPacketSize = 512 * 1024 * 1024;

HttpClient::HttpClient(GeneralClientConnection* connection,
                       const HttpClientParams& params)
  : _connection(connection),
    _delete_connection_on_destruction(false),
    _params(params),
    _read_buffer_offset(0),
    _state(kInConnect),
    _written(0),
    _error_message(""),
    _next_chunked_size(0),
    _method(rest::RequestType::Get),
    _aborted(false),
    _comm(_connection->comm()) {
  SDB_ASSERT(connection != nullptr);

  if (_connection->isConnected()) {
    _state = kFinished;
  }
  // calculate hostname only once. it will remain
  // the same for the entire lifetime of the HttpClient
  // object.
  _hostname = _connection->GetEndpoint()->host();
}

HttpClient::HttpClient(std::unique_ptr<GeneralClientConnection>& connection,
                       const HttpClientParams& params)
  : HttpClient(connection.get(), params) {
  _delete_connection_on_destruction = true;
  connection.release();
}

HttpClient::~HttpClient() {
  // connection may have been invalidated by other objects
  if (_connection != nullptr) {
    if (!_params._keep_connection_on_destruction ||
        !_connection->isConnected()) {
      _connection->disconnect();
    }

    if (_delete_connection_on_destruction) {
      delete _connection;
    }
  }
}

// -----------------------------------------------------------------------------
// public methods
// -----------------------------------------------------------------------------

void HttpClient::recycleResult(std::unique_ptr<HttpResult> result) {
  _result = std::move(result);
}

void HttpClient::setAborted(bool value) noexcept {
  _aborted.store(value, std::memory_order_release);
  setInterrupted(value);
}

void HttpClient::setInterrupted(bool value) {
  if (_connection != nullptr) {
    _connection->setInterrupted(value);
  }
}

bool HttpClient::isConnected() { return _connection->isConnected(); }

void HttpClient::disconnect() { _connection->disconnect(); }

std::string HttpClient::getEndpointSpecification() const {
  return _connection == nullptr ? "unknown"
                                : _connection->getEndpointSpecification();
}

////////////////////////////////////////////////////////////////////////////////
/// close connection
////////////////////////////////////////////////////////////////////////////////

void HttpClient::close() {
  // ensure connection has not yet been invalidated
  SDB_ASSERT(_connection != nullptr);

  _connection->disconnect();
  _state = kInConnect;

  clearReadBuffer();
}

////////////////////////////////////////////////////////////////////////////////
/// send out a request, creating a new HttpResult object
/// this version does not allow specifying custom headers
/// if the request fails because of connection problems, the request will be
/// retried until it either succeeds (at least no connection problem) or there
/// have been _max_retries retries
////////////////////////////////////////////////////////////////////////////////

HttpResult* HttpClient::retryRequest(rest::RequestType method,
                                     std::string_view location,
                                     std::string_view body) {
  return retryRequest(method, location, body, {});
}

////////////////////////////////////////////////////////////////////////////////
/// send out a request, creating a new HttpResult object
/// this version does not allow specifying custom headers
/// if the request fails because of connection problems, the request will be
/// retried until it either succeeds (at least no connection problem) or there
/// have been _max_retries retries
////////////////////////////////////////////////////////////////////////////////

HttpResult* HttpClient::retryRequest(
  rest::RequestType method, std::string_view location, std::string_view body,
  const containers::FlatHashMap<std::string, std::string>& headers) {
  std::unique_ptr<HttpResult> result;
  size_t tries = 0;

  while (true) {
    SDB_ASSERT(result == nullptr);

    result.reset(doRequest(method, location, body, headers));

    if (result != nullptr && result->isComplete()) {
      break;
    }

    result.reset();

    if (tries++ >= _params._max_retries) {
      SDB_WARN("xxxxx", sdb::Logger::HTTPCLIENT, "", _params._retry_message,
               " - no retries left",
               (_error_message.empty() ? std::string("")
                                       : std::string(" - ") + _error_message));
      break;
    }

    auto& server = _connection->server();
    if (server.isStopping()) {
      // abort this client, will also lead to exiting this loop next
      setAborted(true);
    }

    if (isAborted()) {
      break;
    }

    if (!_params._retry_message.empty()) {
      SDB_WARN("xxxxx", sdb::Logger::HTTPCLIENT, "", _params._retry_message,
               " - retries left: ", (_params._max_retries - tries),
               (_error_message.empty() ? std::string("")
                                       : std::string(" - ") + _error_message));
    }

    // 1 microsecond == 10^-6 seconds
    std::this_thread::sleep_for(
      std::chrono::microseconds(_params._retry_wait_time));
  }

  return result.release();
}

////////////////////////////////////////////////////////////////////////////////
/// send out a request, creating a new HttpResult object
/// this version does not allow specifying custom headers
////////////////////////////////////////////////////////////////////////////////

HttpResult* HttpClient::request(rest::RequestType method,
                                std::string_view location,
                                std::string_view body) {
  return doRequest(method, location, body, {});
}

////////////////////////////////////////////////////////////////////////////////
/// send out a request, creating a new HttpResult object
/// this version allows specifying custom headers
////////////////////////////////////////////////////////////////////////////////

HttpResult* HttpClient::request(
  rest::RequestType method, std::string_view location, std::string_view body,
  const containers::FlatHashMap<std::string, std::string>& headers) {
  return doRequest(method, location, body, headers);
}

////////////////////////////////////////////////////////////////////////////////
/// send out a request, worker function
////////////////////////////////////////////////////////////////////////////////

HttpResult* HttpClient::doRequest(
  rest::RequestType method, std::string_view location, std::string_view body,
  const containers::FlatHashMap<std::string, std::string>& headers) {
  // ensure connection has not yet been invalidated
  SDB_ASSERT(_connection != nullptr);
  if (isAborted()) {
    return nullptr;
  }

  // ensure that result is empty
  if (_result == nullptr) {
    // create a new result
    _result = std::make_unique<HttpResult>();
  } else {
    _result->clear();
  }

  SDB_ASSERT(_result != nullptr);

  absl::Cleanup result_guard = [this]() noexcept { _result.reset(); };

  // reset error message
  _error_message.clear();

  // set body
  auto res = setRequest(method, rewriteLocation(location), body, headers);
  if (res != ERROR_OK) {
    this->close();
    _state = kDead;
    setErrorMessage("Got unexpected error while setting up request");
    return nullptr;
  }

  // ensure state
  SDB_ASSERT(_state == kInConnect || _state == kInWrite);

  // respect timeout
  double end_time = utilities::GetMicrotime() + _params._request_timeout;
  double remaining_time = _params._request_timeout;

  bool have_sent_request = false;

  while (_state < kFinished && remaining_time > 0.0) {
    // Note that this loop can either be left by timeout or because
    // a connect did not work (which sets the _state to DEAD). In all
    // other error conditions we call close() which resets the state
    // to IN_CONNECT and tries a reconnect. This is important because
    // it is always possible that we are called with a connection that
    // has already been closed by the other side. This leads to the
    // strange effect that the write (if it is small enough) proceeds
    // but the following read runs into an error. In that case we try
    // to reconnect one and then give up if this does not work.
    switch (_state) {
      case (kInConnect): {
        handleConnect();
        // If this goes wrong, _state is set to DEAD
        break;
      }

      case (kInWrite): {
        size_t bytes_written = 0;

        SDB_ASSERT(_write_buffer.size() >= _written);
        SetError(ERROR_OK);

        bool res = _connection->handleWrite(
          remaining_time,
          static_cast<const void*>(_write_buffer.data() + _written),
          _write_buffer.size() - _written, &bytes_written);

        if (!res) {
          setErrorMessage("Error writing to '" +
                          _connection->GetEndpoint()->specification() + "' '" +
                          _connection->getErrorDetails() + "'");
          this->close();  // this sets _state to IN_CONNECT for a retry
        } else {
          _written += bytes_written;

          if (_written == _write_buffer.size()) {
            _state = kInReadHeader;
            have_sent_request = true;
          }
        }

        break;
      }

      case (kInReadHeader):
      case (kInReadBody):
      case (kInReadChunkedHeader):
      case (kInReadChunkedBody): {
        SetError(ERROR_OK);

        // we need to notice if the other side has closed the connection:
        bool connection_closed;

        bool res = _connection->handleRead(remaining_time, _read_buffer,
                                           connection_closed);

        // If there was an error, then we are doomed:
        if (!res) {
          setErrorMessage("Error reading from: '" +
                          _connection->GetEndpoint()->specification() + "' '" +
                          _connection->getErrorDetails() + "'");

          if (_connection->isInterrupted()) {
            this->close();
            setErrorMessage("Command locally aborted");
            return nullptr;
          }
          this->close();  // this sets the state to IN_CONNECT for a retry
          SDB_DEBUG("xxxxx", sdb::Logger::HTTPCLIENT, _error_message);

          std::this_thread::sleep_for(std::chrono::milliseconds(5));
          break;
        }

        // no error

        if (connection_closed) {
          // write might have succeeded even if the server has closed
          // the connection, this will then show up here with us being
          // in state IN_READ_HEADER but nothing read.
          if (_state == kInReadHeader && 0 == _read_buffer.size()) {
            this->close();
            _state = kDead;
            setErrorMessage("Connection closed by remote");
            break;
          }

          if (_state == kInReadHeader) {
            processHeader();
          }

          if (_state == kInReadBody) {
            if (!_result->hasContentLength()) {
              // If we are reading the body and no content length was
              // found in the header, then we must read until no more
              // progress is made (but without an error), this then means
              // that the server has closed the connection and we must
              // process the body one more time:
              _result->setContentLength(_read_buffer.size() -
                                        _read_buffer_offset);
            }
            processBody();
          }

          if (_state != kFinished) {
            // If the body was not fully found we give up:
            this->close();
            _state = kDead;
            setErrorMessage("Got unexpected response from remote");
          }

          break;
        }

        // the connection is still alive:
        switch (_state) {
          case (kInReadHeader):
            processHeader();
            break;

          case (kInReadBody):
            processBody();
            break;

          case (kInReadChunkedHeader):
            processChunkedHeader();
            break;

          case (kInReadChunkedBody):
            processChunkedBody();
            break;

          default:
            break;
        }

        break;
      }

      default:
        break;
    }

    if (!_comm.IsCommAllowed()) {
      setErrorMessage("Command locally aborted");
      return nullptr;
    }

    remaining_time = end_time - utilities::GetMicrotime();
    if (isAborted()) {
      setErrorMessage("Client request aborted");
      break;
    }
  }

  if (_state < kFinished && _error_message.empty()) {
    setErrorMessage("Request timeout reached");
    _result->setHttpReturnCode(static_cast<int>(ResponseCode::GatewayTimeout));
  }

  // set result type in getResult()
  setResultType(have_sent_request);

  // this method always returns a raw pointer to the result.
  // the caller must take ownership.
  return _result.release();
}

// -----------------------------------------------------------------------------
// private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// initialize the connection
////////////////////////////////////////////////////////////////////////////////

void HttpClient::handleConnect() {
  // ensure connection has not yet been invalidated
  SDB_ASSERT(_connection != nullptr);

  if (!_connection->connect()) {
    setErrorMessage("Could not connect to '" +
                    _connection->GetEndpoint()->specification() + "' '" +
                    _connection->getErrorDetails() + "'");
    _state = kDead;
  } else {
    // can write now
    _state = kInWrite;
    _written = 0;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// clearReadBuffer, clears the read buffer as well as the result
////////////////////////////////////////////////////////////////////////////////

void HttpClient::clearReadBuffer() {
  _read_buffer.clear();
  _read_buffer_offset = 0;

  if (_result) {
    _result->clear();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// set the type of the result
////////////////////////////////////////////////////////////////////////////////

void HttpClient::setResultType(bool have_sent_request) {
  switch (_state) {
    case kInWrite:
      _result->setResultType(HttpResult::ResultType::WriteError);
      break;

    case kInReadHeader:
    case kInReadBody:
    case kInReadChunkedHeader:
    case kInReadChunkedBody:
      _result->setResultType(HttpResult::ResultType::ReadError);
      break;

    case kFinished:
      _result->setResultType(HttpResult::ResultType::Complete);
      break;

    case kInConnect:
    default: {
      _result->setResultType(HttpResult::ResultType::CouldNotConnect);
      if (!haveErrorMessage()) {
        setErrorMessage("Could not connect");
      }
      break;
    }
  }

  if (haveErrorMessage() && _result->getHttpReturnMessage().empty()) {
    _result->setHttpReturnMessage(_error_message);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// prepare a request
////////////////////////////////////////////////////////////////////////////////

ErrorCode HttpClient::setRequest(
  rest::RequestType method, std::string_view location, std::string_view body,
  const containers::FlatHashMap<std::string, std::string>& headers) {
  // clear read-buffer (no pipelining!)
  _read_buffer_offset = 0;
  _read_buffer.clear();

  // set HTTP method
  _method = method;

  // now fill the write buffer
  _write_buffer.clear();

  // append method
  std::string_view mth = GeneralRequest::translateMethod(method);
  _write_buffer.PushStr(mth);
  _write_buffer.PushChr(' ');

  // append location
  if (!location.starts_with('/')) {
    _write_buffer.PushChr('/');
  }
  _write_buffer.append(location);

  // append protocol
  _write_buffer.PushStr(" HTTP/1.1\r\n");

  // append hostname
  SDB_DEBUG("xxxxx", Logger::HTTPCLIENT, "request to ", _hostname, ": ",
            GeneralRequest::translateMethod(method), " ", location);

  _write_buffer.PushStr("Host: ");
  _write_buffer.PushStr(_hostname);
  _write_buffer.PushStr("\r\n");

  if (_params._keep_alive) {
    _write_buffer.append("Connection: Keep-Alive\r\n");
  } else {
    _write_buffer.PushStr("Connection: Close\r\n");
  }

  if (_params._expose_serene_db) {
    _write_buffer.PushStr("User-Agent: SereneDB\r\n");
  }

  // basic authorization
  using ExclusionType = std::pair<size_t, size_t>;
  containers::SmallVector<ExclusionType, 4> exclusions;
  size_t pos = 0;
  if (!_params._jwt.empty()) {
    _write_buffer.PushStr("Authorization: bearer ");
    pos = _write_buffer.size();
    _write_buffer.PushStr(_params._jwt);
    exclusions.emplace_back(pos, _write_buffer.size());
    _write_buffer.PushStr("\r\n");
  } else if (!_params._basic_auth.empty()) {
    _write_buffer.PushStr("Authorization: Basic ");
    pos = _write_buffer.size();
    _write_buffer.PushStr(_params._basic_auth);
    exclusions.emplace_back(pos, _write_buffer.size());
    _write_buffer.PushStr("\r\n");
  }

  bool found_accept_encoding = false;
  bool found_content_encoding = false;
  bool found_content_length = false;
  for (const auto& header : headers) {
    if (!found_content_length &&
        absl::EqualsIgnoreCase(StaticStrings::kContentLength, header.first)) {
      found_content_length = true;
      continue;  // skip content-length header
    }
    if (!found_content_encoding &&
        absl::EqualsIgnoreCase(StaticStrings::kContentEncoding, header.first)) {
      found_content_encoding = true;
    }
    if (!found_accept_encoding && _params._allow_compressed_responses &&
        absl::EqualsIgnoreCase(StaticStrings::kAcceptEncoding, header.first)) {
      found_accept_encoding = true;
    }
    _write_buffer.PushStr(header.first);
    _write_buffer.PushStr(": ");
    if (absl::EqualsIgnoreCase(StaticStrings::kAuthorization, header.first)) {
      pos = _write_buffer.size();
      _write_buffer.PushStr(header.second);
      exclusions.emplace_back(pos, _write_buffer.size());
    } else {
      _write_buffer.PushStr(header.second);
    }
    _write_buffer.PushStr("\r\n");
  }

  if (!found_accept_encoding && _params._allow_compressed_responses) {
    _write_buffer.PushStr("Accept-Encoding: deflate\r\n");
  }

  // compress request body if we are asked for it, but only if
  // no explicit "content-encoding" header was already set
  auto body_length = body.size();
  bool compress_body = !found_content_encoding &&
                       _params._compress_request_threshold > 0 &&
                       _params._compress_request_threshold <= body_length;
  std::string compressed;
  if (compress_body) {
    auto res = encoding::ZLibDeflate(
      reinterpret_cast<const uint8_t*>(body.data()), body_length, compressed);
    if (res != ERROR_OK) {
      return res;
    }
    if (compressed.size() >= body_length) {
      // bad: compressed request body is larger than uncompressed body.
      // in this case we give up on the compression
      compress_body = false;
    } else {
      // good: compression resulted in some smaller request body
      body_length = compressed.size();
      _write_buffer.PushStr("Content-Encoding: deflate\r\n");
    }
  }

  if (method != rest::RequestType::Get && _params._add_content_length) {
    _write_buffer.PushStr("Content-Length: ");
    _write_buffer.PushU64(body_length);
    _write_buffer.PushStr("\r\n");
  }

  // end of headers
  _write_buffer.PushStr("\r\n");

  if (!body.empty()) {
    if (compress_body) {
      SDB_ASSERT(body_length == compressed.size());
      _write_buffer.PushStr(compressed);
    } else {
      // don't write compressed body
      _write_buffer.PushStr({body.data(), body_length});
    }
  }

  if (exclusions.empty()) {
    SDB_TRACE("xxxxx", sdb::Logger::HTTPCLIENT,
              "request: ", _write_buffer.Impl());
    // namespace issue
  } else {
    pos = 0;
    for (size_t i = 0; i < exclusions.size(); ++i) {
      SDB_TRACE(
        "xxxxx", sdb::Logger::HTTPCLIENT, "request: ",
        std::string_view(_write_buffer.data() + pos, exclusions[i].first - pos),
        "SENSITIVE_DETAILS_HIDDEN");
      pos = exclusions[i].second;
    }
    SDB_TRACE(
      "xxxxx", sdb::Logger::HTTPCLIENT, "request: ",
      std::string_view(_write_buffer.data() + pos, _write_buffer.size() - pos));
  }

  if (_state == kDead) {
    _connection->resetNumConnectRetries();
  }

  // close connection to reset all read and write buffers
  if (_state != kFinished) {
    this->close();
  }

  // we are connected, start with writing
  if (_connection->isConnected()) {
    _state = kInWrite;
    _written = 0;
  } else {
    // connect to server
    _state = kInConnect;
  }

  SDB_ASSERT(_state == kInConnect || _state == kInWrite);
  return ERROR_OK;
}

// -----------------------------------------------------------------------------
// private methods
// -----------------------------------------------------------------------------

void HttpClient::processHeader() {
  SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());
  size_t remain = _read_buffer.size() - _read_buffer_offset;
  const char* ptr = _read_buffer.data() + _read_buffer_offset;
  const char* pos = static_cast<const char*>(memchr(ptr, '\n', remain));

  // We enforce the following invariants:
  //   ptr = _read_buffer.c_str() + _read_buffer_offset
  //   _read_buffer.length() >= _read_buffer_offset
  //   remain = _read_buffer.length() - _read_buffer_offset
  while (pos) {
    SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());
    SDB_ASSERT(ptr == _read_buffer.data() + _read_buffer_offset);
    SDB_ASSERT(remain == _read_buffer.size() - _read_buffer_offset);

    if (pos > ptr && *(pos - 1) == '\r') {
      // adjust eol position
      --pos;
    }

    // end of header found
    if (*ptr == '\r' || *ptr == '\n' || *ptr == '\0') {
      size_t len = pos - ptr;
      _read_buffer_offset += len + 1;
      SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());

      ptr += len + 1;
      remain -= len + 1;

      if (*pos == '\r') {
        // adjust offset if line ended with \r\n
        ++_read_buffer_offset;
        SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());

        ptr++;
        remain--;
      }

      // handle chunks
      if (_result->isChunked()) {
        _state = kInReadChunkedHeader;
        processChunkedHeader();
        return;
      }

      else if (_result->getHttpReturnCode() == 204 &&
               _result->hasContentLength() &&
               _result->getContentLength() != 0) {
        _result->setResultType(HttpResult::ResultType::Complete);
        _state = kFinished;
        // always disconnect - some servers include a response body
        _connection->disconnect();
        return;
      }

      // no content-length header in response
      else if (!_result->hasContentLength()) {
        _state = kInReadBody;
        processBody();
        return;
      }

      // no body
      else if (_result->hasContentLength() &&
               _result->getContentLength() == 0) {
        _result->setResultType(HttpResult::ResultType::Complete);
        _state = kFinished;

        if (!_params._keep_alive) {
          _connection->disconnect();
        }
        return;
      }

      // found content-length header in response
      else if (_result->hasContentLength() && _result->getContentLength() > 0) {
        if (_result->getContentLength() > _params._max_packet_size) {
          std::string error_message(
            "ignoring HTTP response with 'Content-Length' bigger than max "
            "packet size (");
          error_message += std::to_string(_result->getContentLength()) + " > " +
                           std::to_string(_params._max_packet_size) + ")";
          setErrorMessage(error_message, true);

          // reset connection
          this->close();
          _state = kDead;

          return;
        }

        _state = kInReadBody;
        processBody();
        return;
      } else {
        SDB_ASSERT(false);
      }

      break;
    }

    // we have found more header fields
    else {
      size_t len = pos - ptr;
      _result->addHeaderField(ptr, len);

      if (*pos == '\r') {
        // adjust length if line ended with \r\n
        // (header was already added so no harm is done)
        ++len;
      }

      // account for \n
      ptr += len + 1;
      _read_buffer_offset += len + 1;
      SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());

      remain -= (len + 1);

      SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());
      SDB_ASSERT(ptr == _read_buffer.data() + _read_buffer_offset);
      SDB_ASSERT(remain == _read_buffer.size() - _read_buffer_offset);
      pos = static_cast<const char*>(memchr(ptr, '\n', remain));
    }
  }
}

void HttpClient::processBody() {
  // HEAD requests may be responded to without a body...
  if (_method == rest::RequestType::Head) {
    _result->setResultType(HttpResult::ResultType::Complete);
    _state = kFinished;

    if (!_params._keep_alive) {
      _connection->disconnect();
    }

    return;
  }

  // we need to wait for a close, if content length is unknown
  if (!_result->hasContentLength()) {
    return;
  }

  // we need to wait for more data
  if (_read_buffer.size() - _read_buffer_offset < _result->getContentLength()) {
    return;
  }

  // body is compressed using deflate. inflate it
  if (_result->getEncodingType() == rest::EncodingType::Deflate) {
    // TODO(mbkkt) why ignore?
    std::ignore = Inflate(&encoding::ZLibInflate, _read_buffer.Impl(),
                          _result->getBody(), _read_buffer_offset);
  } else if (_result->getEncodingType() == rest::EncodingType::GZip) {
    // TODO(mbkkt) why ignore?
    std::ignore = Inflate(&encoding::GZipUncompress, _read_buffer.Impl(),
                          _result->getBody(), _read_buffer_offset);
  } else if (_result->getEncodingType() == rest::EncodingType::Lz4) {
    // TODO(mbkkt) why ignore?
    std::ignore = Inflate(&encoding::Lz4Uncompress, _read_buffer.Impl(),
                          _result->getBody(), _read_buffer_offset);
  } else {
    // body is not compressed
    // Note that if we are here, then
    // _result->getContentLength() <= _read_buffer.length()-_read_buffer_offset
    _result->getBody().append(
      {_read_buffer.data() + _read_buffer_offset, _result->getContentLength()});
  }

  _read_buffer_offset += _result->getContentLength();
  SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());

  _result->setResultType(HttpResult::ResultType::Complete);
  _state = kFinished;

  if (!_params._keep_alive) {
    _connection->disconnect();
  }
}

void HttpClient::processChunkedHeader() {
  size_t remain = _read_buffer.size() - _read_buffer_offset;
  const char* ptr = _read_buffer.data() + _read_buffer_offset;
  const char* pos = static_cast<const char*>(memchr(ptr, '\n', remain));

  // not yet finished, newline is missing
  if (pos == nullptr) {
    return;
  }

  // adjust eol position
  if (pos > ptr && *(pos - 1) == '\r') {
    --pos;
  }

  size_t len = pos - (_read_buffer.data() + _read_buffer_offset);
  std::string line(_read_buffer.data() + _read_buffer_offset, len);
  string_utils::TrimInPlace(line);

  _read_buffer_offset += (len + 1);

  // adjust offset if line ended with \r\n
  if (*pos == '\r') {
    ++_read_buffer_offset;
    SDB_ASSERT(_read_buffer_offset <= _read_buffer.size());
  }

  // empty lines are an error
  if (line.empty() || line[0] == '\r') {
    setErrorMessage("found invalid Content-Length", true);
    // reset connection
    this->close();
    _state = kDead;

    return;
  }

  uint32_t content_length;

  try {
    content_length = static_cast<uint32_t>(std::stol(line, nullptr, 16));
  } catch (...) {
    setErrorMessage("found invalid Content-Length", true);
    // reset connection
    this->close();
    _state = kDead;

    return;
  }

  // failed: too many bytes
  if (content_length > _params._max_packet_size) {
    std::string error_message = absl::StrCat(
      "ignoring HTTP response with 'Content-Length' bigger than max packet "
      "size (",
      content_length, " > ", _params._max_packet_size, ")");
    setErrorMessage(error_message, true);
    // reset connection
    this->close();
    _state = kDead;

    return;
  }

  _state = kInReadChunkedBody;
  _next_chunked_size = content_length;

  processChunkedBody();
}

void HttpClient::processChunkedBody() {
  // HEAD requests may be responded to without a body...
  if (_method == rest::RequestType::Head) {
    _result->setResultType(HttpResult::ResultType::Complete);
    _state = kFinished;

    if (!_params._keep_alive) {
      _connection->disconnect();
    }

    return;
  }

  if (_read_buffer.size() - _read_buffer_offset >= _next_chunked_size + 2) {
    // last chunk length was 0, therefore we are finished
    if (_next_chunked_size == 0) {
      _result->setResultType(HttpResult::ResultType::Complete);

      _state = kFinished;

      if (!_params._keep_alive) {
        _connection->disconnect();
      }

      return;
    }

    if (_result->getEncodingType() == rest::EncodingType::Deflate) {
      // TODO(mbkkt) why ignore?
      std::ignore = Inflate(&encoding::ZLibInflate, _read_buffer.Impl(),
                            _result->getBody(), _read_buffer_offset);
    } else if (_result->getEncodingType() == rest::EncodingType::GZip) {
      // TODO(mbkkt) why ignore?
      std::ignore = Inflate(&encoding::GZipUncompress, _read_buffer.Impl(),
                            _result->getBody(), _read_buffer_offset);
    } else if (_result->getEncodingType() == rest::EncodingType::Lz4) {
      // TODO(mbkkt) why ignore?
      std::ignore = Inflate(&encoding::Lz4Uncompress, _read_buffer.Impl(),
                            _result->getBody(), _read_buffer_offset);
    } else {
      _result->getBody().PushStr({_read_buffer.data() + _read_buffer_offset,
                                  static_cast<size_t>(_next_chunked_size)});
    }

    _read_buffer_offset += (size_t)_next_chunked_size + 2;

    _state = kInReadChunkedHeader;
    processChunkedHeader();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// extract an error message from a response
////////////////////////////////////////////////////////////////////////////////

std::string HttpClient::getHttpErrorMessage(const HttpResult* result,
                                            ErrorCode* error_code) {
  if (error_code != nullptr) {
    *error_code = ERROR_OK;
  }

  const sdb::basics::StringBuffer& body = result->getBody();
  std::string details;

  try {
    auto builder = vpack::Parser::fromJson(
      reinterpret_cast<const uint8_t*>(body.data()), body.size());

    vpack::Slice slice = builder->slice();
    if (slice.isObject()) {
      vpack::Slice msg = slice.get(StaticStrings::kErrorMessage);
      int error_num = slice.get(StaticStrings::kErrorNum).getNumber<int>();

      if (msg.isString() && !msg.isEmptyString() && error_num > 0) {
        if (error_code != nullptr) {
          *error_code = ErrorCode{error_num};
        }
        details =
          absl::StrCat(": SereneError ", error_num, ": ", msg.stringView());
      }
    }
  } catch (...) {
    // don't rethrow here. we'll respond with an error message anyway
  }

  return absl::StrCat("got error from server: HTTP ",
                      result->getHttpReturnCode(), " (",
                      result->getHttpReturnMessage(), ")", details);
}

void HttpClient::setErrorMessage(std::string_view message, ErrorCode error) {
  if (error != ERROR_OK) {
    _error_message = absl::StrCat(message, ": ", GetErrorStr(error));
  } else {
    setErrorMessage(message);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// fetch the version from the server
////////////////////////////////////////////////////////////////////////////////

std::string HttpClient::getServerVersion(ErrorCode* error_code) {
  if (error_code != nullptr) {
    *error_code = ERROR_INTERNAL;
  }

  std::unique_ptr<HttpResult> response(
    request(rest::RequestType::Get, "/_api/version", {}));

  if (response == nullptr || !response->isComplete()) {
    return "";
  }

  if (response->getHttpReturnCode() ==
      static_cast<int>(rest::ResponseCode::Ok)) {
    // default value
    std::string version = "serene";

    try {
      std::shared_ptr<vpack::Builder> builder = response->getBodyVPack();

      vpack::Slice slice = builder->slice();
      if (slice.isObject()) {
        if (auto server = slice.get("server");
            server.isString() && server.stringView() == "serene") {
          // "server" value is a string and its content is "serene"
          vpack::Slice v = slice.get("version");
          if (v.isString()) {
            version = v.stringView();
          }
        }
      }

      if (error_code != nullptr) {
        *error_code = ERROR_OK;
      }
      return version;
    } catch (const std::exception& ex) {
      setErrorMessage(ex.what(), false);
      return "";
    } catch (...) {
      setErrorMessage("Unable to parse server response", false);
      return "";
    }
  }

  if (response->wasHttpError()) {
    std::string msg = getHttpErrorMessage(response.get(), error_code);
    setErrorMessage(msg, false);
  }
  _connection->disconnect();

  return "";
}

}  // namespace sdb::httpclient
