////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018-2019 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Jan Christoph Uhde
/// @author Ewout Prangsma
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <fuerte/asio_ns.h>
#include <fuerte/types.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "basics/buffer.h"

namespace sdb::fuerte {

inline constexpr std::string_view kFuAcceptKey = "accept";
inline constexpr std::string_view kFuAuthorizationKey = "authorization";
inline constexpr std::string_view kFuContentLengthKey = "content-length";
inline constexpr std::string_view kFuContentTypeKey = "content-type";
inline constexpr std::string_view kFuContentEncodingKey = "content-encoding";
inline constexpr std::string_view kFuKeepAliveKey = "keep-alive";

struct MessageHeader {
  // Header metadata helpers#
  template<typename K, typename V>
  void addMeta(K&& key, V&& value) {
    if (kFuAcceptKey == key) {
      _accept_type = ToContentType(value);
      if (_accept_type != ContentType::Custom) {
        return;
      }
    } else if (kFuContentTypeKey == key) {
      _content_type = ToContentType(value);
      if (_content_type != ContentType::Custom) {
        return;
      }
    } else if (kFuContentEncodingKey == key) {
      _content_encoding = ToContentEncoding(value);
    }
    this->_meta.emplace(std::forward<K>(key), std::forward<V>(value));
  }

  void setMeta(StringMap);
  const StringMap& meta() const { return _meta; }

  // Get value for header metadata key, returns empty string if not found.
  const std::string& metaByKey(const std::string& key) const {
    bool unused;
    return this->metaByKey(key, unused);
  }
  const std::string& metaByKey(const std::string& key, bool& found) const;

  void removeMeta(const std::string& key) { _meta.erase(key); }

  ContentEncoding contentEncoding() const { return _content_encoding; }
  void contentEncoding(ContentEncoding encoding) noexcept {
    _content_encoding = encoding;
  }

  // content type accessors
  ContentType contentType() const { return _content_type; }
  void contentType(ContentType type) { _content_type = type; }
  void contentType(const std::string& type) {
    addMeta(kFuContentTypeKey, type);
  }

 protected:
  StringMap _meta;  /// Header meta data (equivalent to HTTP headers)
  ContentType _content_type = ContentType::Unset;
  ContentType _accept_type = ContentType::VPack;
  ContentEncoding _content_encoding = ContentEncoding::Identity;
};

struct RequestHeader final : public MessageHeader {
  /// Database that is the target of the request
  std::string database;

  /// Local path of the request (without "/_db/" prefix)
  std::string path;

  /// Query parameters
  StringMap parameters;

  /// HTTP method
  RestVerb rest_verb = RestVerb::Illegal;

  // accept header accessors
  ContentType acceptType() const { return _accept_type; }
  void acceptType(ContentType type) { _accept_type = type; }
  void acceptType(const std::string& type);

  /// @brief analyze path and split into components
  /// strips /_db/<name> prefix, sets db name and fills parameters
  void parseSerenePath(std::string_view path);
};

struct ResponseHeader final : public MessageHeader {
  friend class Response;

  /// Response code
  StatusCode response_code = kStatusUndefined;

  MessageType responseType() const { return MessageType::Response; }
};

// Message is base class for message being send to (Request) or
// from (Response) a server.
class Message {
 protected:
  Message() : _timestamp(std::chrono::steady_clock::now()) {}
  virtual ~Message() = default;

 public:
  /// Message type
  virtual MessageType type() const = 0;
  virtual const MessageHeader& messageHeader() const = 0;

  ///////////////////////////////////////////////
  // get payload
  ///////////////////////////////////////////////

  /// get slices if the content-type is vpack
  virtual std::vector<vpack::Slice> slices() const = 0;
  virtual asio_ns::const_buffer payload() const = 0;
  virtual size_t payloadSize() const = 0;
  std::string payloadAsString() const {
    const auto p = payload();
    return {static_cast<const char*>(p.data()), asio_ns::buffer_size(p)};
  }

  /// get the content as a slice
  vpack::Slice slice() const {
    auto slices = this->slices();
    if (!slices.empty()) {
      return slices[0];
    }
    return vpack::Slice::noneSlice();
  }

  /// content-encoding header type
  ContentEncoding contentEncoding() const;

  /// content-type header accessors
  ContentType contentType() const;

  bool isContentTypeJSON() const;
  bool isContentTypeVPack() const;
  bool isContentTypeHtml() const;
  bool isContentTypeText() const;

  std::chrono::steady_clock::time_point timestamp() const { return _timestamp; }
  // set timestamp when it was sent
  void timestamp(std::chrono::steady_clock::time_point t) { _timestamp = t; }

 private:
  std::chrono::steady_clock::time_point _timestamp;
};

// Request contains the message send to a server in a request.
class Request final : public Message {
 public:
  static constexpr std::chrono::milliseconds kDefaultTimeout =
    std::chrono::seconds(300);

  Request(RequestHeader message_header = RequestHeader())
    : header(std::move(message_header)), _timeout(kDefaultTimeout) {}

  /// @brief request header
  RequestHeader header;

  MessageType type() const override { return MessageType::Request; }
  const MessageHeader& messageHeader() const override { return header; }
  void setFuzzReqHeader(std::string fuzz_header) {
    _fuzz_req_header = std::move(fuzz_header);
  }
  std::optional<std::string> getFuzzReqHeader() const {
    return _fuzz_req_header;
  }
  bool getFuzzerReq() const noexcept { return _fuzz_req_header.has_value(); }

  ///////////////////////////////////////////////
  // header accessors
  ///////////////////////////////////////////////

  // accept header accessors
  ContentType acceptType() const;

  ///////////////////////////////////////////////
  // add payload
  ///////////////////////////////////////////////
  void addVPack(const vpack::Slice slice);
  void addVPack(const vpack::BufferUInt8& buffer);
  void addVPack(vpack::BufferUInt8&& buffer);
  void addBinary(const uint8_t* data, size_t length);

  ///////////////////////////////////////////////
  // get payload
  ///////////////////////////////////////////////

  /// @brief get vpack slices contained in request
  /// only valid iff the data was added via addVPack
  std::vector<vpack::Slice> slices() const override;
  vpack::BufferUInt8& payloadForModification() { return _payload; }
  asio_ns::const_buffer payload() const override;
  size_t payloadSize() const override;
  vpack::BufferUInt8&& moveBuffer() && { return std::move(_payload); }

  // get timeout, 0 means no timeout
  inline std::chrono::milliseconds timeout() const { return _timeout; }
  // set timeout
  void timeout(std::chrono::milliseconds timeout) { _timeout = timeout; }

  // Sending time accounting:
  void setTimeQueued() noexcept {
    _time_queued = std::chrono::steady_clock::now();
  }
  void setTimeAsyncWrite() noexcept {
    _time_async_write = std::chrono::steady_clock::now();
  }
  void setTimeSent() noexcept { _time_sent = std::chrono::steady_clock::now(); }
  std::chrono::steady_clock::time_point timeQueued() const noexcept {
    return _time_queued;
  }
  std::chrono::steady_clock::time_point timeAsyncWrite() const noexcept {
    return _time_async_write;
  }
  std::chrono::steady_clock::time_point timeSent() const noexcept {
    return _time_sent;
  }

 private:
  vpack::BufferUInt8 _payload;
  std::chrono::milliseconds _timeout;
  std::optional<std::string> _fuzz_req_header = std::nullopt;
  std::chrono::steady_clock::time_point _time_queued;
  std::chrono::steady_clock::time_point _time_async_write;
  std::chrono::steady_clock::time_point _time_sent;
};

// Response contains the message resulting from a request to a server.
class Response : public Message {
 public:
  Response(ResponseHeader req_header = ResponseHeader())
    : header(std::move(req_header)), _payload_offset(0) {}

  Response(const Response&) = delete;
  Response& operator=(const Response&) = delete;

  /// @brief request header
  ResponseHeader header;

  MessageType type() const override { return header.responseType(); }
  const MessageHeader& messageHeader() const override { return header; }
  ///////////////////////////////////////////////
  // get / check status
  ///////////////////////////////////////////////

  // statusCode returns the (HTTP) status code for the request (200==OK).
  StatusCode statusCode() const noexcept { return header.response_code; }

  // checkStatus returns true if the statusCode equals one of the given valid
  // code, false otherwise.
  bool checkStatus(std::initializer_list<StatusCode> valid_status_codes) const {
    auto actual = statusCode();
    for (auto code : valid_status_codes) {
      if (code == actual) {
        return true;
      }
    }
    return false;
  }
  // assertStatus throw an exception if the statusCode does not equal one of the
  // given valid codes.
  void assertStatus(std::initializer_list<StatusCode> valid_status_codes) {
    if (!checkStatus(valid_status_codes)) {
      throw std::runtime_error("invalid status " +
                               std::to_string(statusCode()));
    }
  }

  ///////////////////////////////////////////////
  // get/set payload
  ///////////////////////////////////////////////

  /// @brief validates and returns VPack response. Only valid for vpack
  std::vector<vpack::Slice> slices() const override;
  asio_ns::const_buffer payload() const override;
  size_t payloadSize() const override;
  std::shared_ptr<vpack::BufferUInt8> copyPayload() const;
  std::shared_ptr<vpack::BufferUInt8> stealPayload();

  void setPayload(vpack::BufferUInt8&& buffer, size_t offset) {
    _payload_offset = offset;
    _payload = std::move(buffer);
  }

 private:
  vpack::BufferUInt8 _payload;
  size_t _payload_offset;
};

}  // namespace sdb::fuerte
