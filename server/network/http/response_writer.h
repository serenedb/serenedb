////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <string_view>
#include <yaclib/async/future.hpp>

#include "basics/debugging.h"
#include "basics/message_buffer.h"

namespace sdb::network::http {

// The session side of the writer: backpressure and teardown state. The
// session task implements this; handlers only see HttpResponseWriter.
class ResponseSink {
 public:
  virtual ~ResponseSink() = default;
  // Park until the unsent committed bytes drop below the send high-water (or
  // the client is gone). Handlers call this between large body pieces.
  virtual yaclib::Future<> Drain() = 0;
  virtual bool Broken() const noexcept = 0;
};

std::string_view ReasonPhrase(int status) noexcept;

// The ONLY way a handler produces output: head + body written straight into
// the session's send buffer (zero copy -- the one memcpy is into the wire
// buffer itself). Bodies are either known-length (ContentLength head) or
// chunked (no length up front); chunked framing reserves a fixed-width hex
// size header in the buffer and patches it when the chunk seals, so
// serializers write payload bytes directly into the buffer between
// BeginChunk/EndChunk -- no intermediate chunk buffer.
class HttpResponseWriter {
 public:
  HttpResponseWriter(message::Buffer& send, ResponseSink& sink, bool keep_alive,
                     bool head_only)
    : _send{send},
      _sink{sink},
      _keep_alive{keep_alive},
      _head_only{head_only} {}

  bool HeadWritten() const noexcept { return _state != State::kIdle; }
  bool Finished() const noexcept { return _state == State::kFinished; }
  bool KeepAlive() const noexcept { return _keep_alive; }

  // --- one-shot responses -------------------------------------------------
  void Json(int status, std::string_view body) {
    Fixed(status, "application/json", body);
  }

  void Text(int status, std::string_view body) {
    Fixed(status, "text/plain", body);
  }

  void Error(int status, std::string_view error_label) {
    Json(status, absl::StrCat(R"({"error":")", error_label, R"("})"));
  }

  void Fixed(int status, std::string_view content_type, std::string_view body,
             std::string_view extra_headers = {}) {
    WriteHead(status, content_type, body.size(), extra_headers);
    // WriteHead leaves kFixedBody only when a body is actually expected; for
    // HEAD and bodiless statuses (1xx/204/304) it goes straight to kFinished.
    if (_state == State::kFixedBody) {
      _send.WriteUncommitted(body);
    }
    _send.Commit(true);
    _state = State::kFinished;
  }

  // --- known-length streamed body ----------------------------------------
  void WriteHead(int status, std::string_view content_type,
                 uint64_t content_length, std::string_view extra_headers = {}) {
    const bool bodiless =
      EncodeHead(status, content_type, &content_length, extra_headers);
    _state = State::kFixedBody;
    _remaining = (_head_only || bodiless) ? 0 : content_length;
    if (_remaining == 0) {
      _state = State::kFinished;
    }
  }

  void Write(std::string_view data) {
    if (_head_only) {
      return;
    }
    switch (_state) {
      case State::kFixedBody:
        SDB_ASSERT(data.size() <= _remaining);
        _send.WriteUncommitted(data);
        _remaining -= data.size();
        if (_remaining == 0) {
          _state = State::kFinished;
        }
        _send.Commit(false);
        return;
      case State::kChunkedBody:
        if (!data.empty()) {
          BeginChunk();
          _send.WriteUncommitted(data);
          EndChunk();
        }
        return;
      default:
        SDB_ASSERT(false);
    }
  }

  // --- chunked streamed body ----------------------------------------------
  void WriteHeadChunked(int status, std::string_view content_type,
                        std::string_view extra_headers = {}) {
    EncodeHead(status, content_type, nullptr, extra_headers);
    _state = State::kChunkedBody;
  }

  // Between BeginChunk/EndChunk the raw buffer is exposed: serializers write
  // payload bytes directly (WriteObject and friends), then the seal patches
  // the reserved fixed-width hex length. HEAD requests skip body bytes but
  // keep the same control flow.
  void BeginChunk() {
    SDB_ASSERT(_state == State::kChunkedBody && _chunk_header == nullptr);
    if (_head_only) {
      return;
    }
    _chunk_header = _send.GetContiguousData(kChunkHeaderLen);
    _chunk_start = _send.GetUncommittedSize();
  }

  message::Buffer& Body() noexcept { return _send; }

  void EndChunk() {
    if (_head_only) {
      return;
    }
    SDB_ASSERT(_chunk_header != nullptr);
    const size_t payload = _send.GetUncommittedSize() - _chunk_start;
    // A zero-size chunk would terminate the body (RFC 9112 7.1); callers
    // must write payload between Begin/End.
    SDB_ASSERT(payload != 0);
    // Fixed-width hex: leading zeros are legal in chunk-size, which is what
    // makes the reserve-then-patch framing possible.
    static constexpr char kHex[] = "0123456789abcdef";
    for (int i = 0; i < 8; ++i) {
      _chunk_header[i] = kHex[(payload >> ((7 - i) * 4)) & 0xF];
    }
    _chunk_header[8] = '\r';
    _chunk_header[9] = '\n';
    _chunk_header = nullptr;
    _send.WriteUncommitted("\r\n");
    _send.Commit(false);
  }

  yaclib::Future<> Drain() { return _sink.Drain(); }
  bool Broken() const noexcept { return _sink.Broken(); }

  // Terminates the response. For chunked bodies writes the last-chunk; for
  // fixed bodies asserts the promised length was written.
  void Finish() {
    switch (_state) {
      case State::kChunkedBody:
        SDB_ASSERT(_chunk_header == nullptr);
        if (!_head_only) {
          _send.WriteUncommitted("0\r\n\r\n");
        }
        _send.Commit(true);
        _state = State::kFinished;
        return;
      case State::kFinished:
        _send.Commit(true);
        return;
      case State::kFixedBody:
        SDB_ASSERT(_remaining == 0);
        _state = State::kFinished;
        _send.Commit(true);
        return;
      case State::kIdle:
        SDB_ASSERT(false);
    }
  }

 private:
  enum class State : uint8_t {
    kIdle,
    kFixedBody,
    kChunkedBody,
    kFinished,
  };

  static constexpr size_t kChunkHeaderLen = 10;  // "XXXXXXXX\r\n"

  // Returns whether the status is bodiless (1xx/204/304); the caller must then
  // emit no body and no framing length.
  bool EncodeHead(int status, std::string_view content_type,
                  const uint64_t* content_length,
                  std::string_view extra_headers) {
    SDB_ASSERT(_state == State::kIdle);
    // 1xx/204/304 carry neither a body nor framing length (RFC 9112 6.1).
    const bool bodiless =
      status == 204 || status == 304 || (status >= 100 && status < 200);
    std::string head =
      absl::StrCat("HTTP/1.1 ", status, " ", ReasonPhrase(status),
                   "\r\nContent-Type: ", content_type);
    if (bodiless) {
      // no Content-Length, no Transfer-Encoding
    } else if (content_length != nullptr) {
      absl::StrAppend(&head, "\r\nContent-Length: ", *content_length);
    } else {
      absl::StrAppend(&head, "\r\nTransfer-Encoding: chunked");
    }
    absl::StrAppend(&head,
                    "\r\nConnection: ", _keep_alive ? "keep-alive" : "close",
                    "\r\n", extra_headers, "\r\n");
    _send.WriteUncommitted(head);
    return bodiless;
  }

  message::Buffer& _send;
  ResponseSink& _sink;
  uint8_t* _chunk_header = nullptr;
  size_t _chunk_start = 0;
  uint64_t _remaining = 0;
  State _state = State::kIdle;
  bool _keep_alive;
  bool _head_only;
};

}  // namespace sdb::network::http
