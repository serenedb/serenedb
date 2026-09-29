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
#include <iresearch/utils/debugging.hpp>
#include <optional>
#include <string_view>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/task.hpp>

#include "network/http/common.h"
#include "network/http/compression.h"
#include "server/utils/message_buffer.h"

namespace sdb::network::http {

// The session side of the writer: backpressure and teardown state. The
// session task implements this; handlers only see HttpResponseWriter.
class ResponseSink {
 public:
  virtual ~ResponseSink() = default;
  // Park until the unsent committed bytes drop below the send high-water (or
  // the client is gone). Handlers call this between large body pieces.
  virtual yaclib::Task<> Drain() = 0;
  virtual bool Broken() const noexcept = 0;
};

std::string_view ReasonPhrase(HttpStatus status) noexcept;

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

  // Headers emitted on EVERY response from this writer (e.g. CORS). Must be
  // complete "Name: value\r\n" lines; set before the handler writes its head.
  void SetExtraHeaders(std::string_view headers) noexcept { _extra = headers; }

  // The content-coding negotiated from Accept-Encoding; set before the head.
  // Bodies are compressed unless the response is HEAD, bodiless, or a fixed
  // body under kMinCompressBytes.
  void SetContentCoding(const ContentCoding& coding) noexcept {
    _coding = &coding;
  }

  // --- one-shot responses -------------------------------------------------
  void Json(HttpStatus status, std::string_view body) {
    Fixed(status, kJsonContentType, body);
  }

  void Text(HttpStatus status, std::string_view body) {
    Fixed(status, "text/plain", body);
  }

  void Error(HttpStatus status, std::string_view error_label) {
    Json(status, absl::StrCat(R"({"error":")", error_label, R"("})"));
  }

  void Fixed(HttpStatus status, std::string_view content_type,
             std::string_view body, std::string_view extra_headers = {}) {
    // `_encoder != nullptr` means `body` IS the compressed bytes, handed back
    // by the branch below; without that guard an incompressible body (whose
    // encoding is never smaller) would re-enter here forever.
    if (_encoder == nullptr && ShouldEncode(status, body.size())) {
      std::string compressed;
      auto encoder = _coding->make();
      encoder->Encode(body, true,
                      [&](std::string_view out) { compressed.append(out); });
      if (compressed.size() < body.size()) {
        _encoder = std::move(encoder);
        Fixed(status, content_type, compressed, extra_headers);
        return;
      }
      // Encoding made this body bigger; send it as-is. Clearing the coding
      // drops the Content-Encoding header and stops WriteHead below from
      // taking the compress-and-chunk path for the same body.
      _coding = nullptr;
    }
    WriteHead(status, content_type, body.size(), extra_headers);
    // WriteHead (via EncodeHead) already committed the head. Append the body
    // (if one is expected) and flush.
    message::Writer writer{_send};
    if (_state == State::kFixedBody) {
      writer.Write(body);
    }
    writer.Commit(true);
    _state = State::kFinished;
    _encoder.reset();
  }

  // --- known-length streamed body ----------------------------------------
  // A compressed body has no known length up front, so it is framed chunked.
  void WriteHead(HttpStatus status, std::string_view content_type,
                 uint64_t content_length, std::string_view extra_headers = {}) {
    if (_encoder == nullptr && ShouldEncode(status, content_length)) {
      WriteHeadChunked(status, content_type, extra_headers);
      return;
    }
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
      case State::kFixedBody: {
        SDB_ASSERT(data.size() <= _remaining);
        message::Writer writer{_send};
        writer.Write(data);
        _remaining -= data.size();
        if (_remaining == 0) {
          _state = State::kFinished;
        }
        writer.Commit(false);
        return;
      }
      case State::kChunkedBody:
        if (_encoder != nullptr) {
          EncodeChunks(data, false);
        } else if (!data.empty()) {
          WriteChunk(data);
        }
        return;
      default:
        SDB_ASSERT(false);
    }
  }

  // --- chunked streamed body ----------------------------------------------
  // A compressed stream buffers inside the codec, so Write() no longer maps
  // one-to-one onto chunks the client can consume: bytes leave when a codec
  // block fills or at Finish(). Incremental-delivery endpoints (SSE and the
  // like) must not run on a listener with compression enabled.
  void WriteHeadChunked(HttpStatus status, std::string_view content_type,
                        std::string_view extra_headers = {}) {
    if (_encoder == nullptr && ShouldEncode(status, kMinCompressBytes)) {
      _encoder = _coding->make();
    }
    EncodeHead(status, content_type, nullptr, extra_headers);
    _state = State::kChunkedBody;
  }

  // Between BeginChunk/EndChunk the raw buffer is exposed: serializers write
  // payload bytes directly (WriteObject and friends), then the seal patches
  // the reserved fixed-width hex length. HEAD requests skip body bytes but
  // keep the same control flow.
  void BeginChunk() {
    SDB_ASSERT(_encoder == nullptr);
    BeginRawChunk();
  }

  void EndChunk() { EndRawChunk(); }

  yaclib::Task<> Drain() { return _sink.Drain(); }
  bool Broken() const noexcept { return _sink.Broken(); }

  // Terminates the response. For chunked bodies writes the last-chunk; for
  // fixed bodies asserts the promised length was written.
  void Finish() {
    switch (_state) {
      case State::kChunkedBody: {
        SDB_ASSERT(!_chunk.has_value());
        if (_encoder != nullptr) {
          EncodeChunks({}, true);
          _encoder.reset();
        }
        message::Writer writer{_send};
        if (!_head_only) {
          writer.Write("0\r\n\r\n");
        }
        writer.Commit(true);
        _state = State::kFinished;
        return;
      }
      case State::kFinished:
        message::Writer{_send}.Commit(true);
        return;
      case State::kFixedBody:
        SDB_ASSERT(_remaining == 0);
        _state = State::kFinished;
        message::Writer{_send}.Commit(true);
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

  static bool Bodiless(HttpStatus status) noexcept {
    // 1xx/204/304 carry neither a body nor framing length (RFC 9112 6.1).
    const auto code = std::to_underlying(status);
    return status == HttpStatus::NoContent ||
           status == HttpStatus::NotModified || (code >= 100 && code < 200);
  }

  bool ShouldEncode(HttpStatus status, uint64_t body_size) const noexcept {
    return _coding != nullptr && !_head_only && !Bodiless(status) &&
           body_size >= kMinCompressBytes;
  }

  void WriteChunk(std::string_view data) {
    BeginRawChunk();
    _chunk->Write(data);
    EndRawChunk();
  }

  void BeginRawChunk() {
    SDB_ASSERT(_state == State::kChunkedBody && !_chunk.has_value());
    if (_head_only) {
      return;
    }
    _chunk.emplace(_send);
    _chunk_header = _chunk->Alloc(kChunkHeaderLen);
    _chunk_start = _chunk->Written();
  }

  void EndRawChunk() {
    if (_head_only) {
      return;
    }
    SDB_ASSERT(_chunk.has_value());
    const size_t payload = _chunk->Written() - _chunk_start;
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
    _chunk->Write("\r\n");
    _chunk->Commit(false);
    _chunk.reset();
  }

  void EncodeChunks(std::string_view data, bool finish) {
    if (data.empty() && !finish) {
      return;
    }
    _encoder->Encode(data, finish, [&](std::string_view out) {
      if (!out.empty() && !_head_only) {
        WriteChunk(out);
      }
    });
  }

  // Returns whether the status is bodiless (1xx/204/304); the caller must then
  // emit no body and no framing length.
  bool EncodeHead(HttpStatus status, std::string_view content_type,
                  const uint64_t* content_length,
                  std::string_view extra_headers) {
    SDB_ASSERT(_state == State::kIdle);
    const auto code = std::to_underlying(status);
    const bool bodiless = Bodiless(status);
    std::string head =
      absl::StrCat("HTTP/1.1 ", code, " ", ReasonPhrase(status),
                   "\r\nContent-Type: ", content_type);
    if (bodiless) {
      // no Content-Length, no Transfer-Encoding
    } else if (content_length != nullptr) {
      absl::StrAppend(&head, "\r\nContent-Length: ", *content_length);
    } else {
      absl::StrAppend(&head, "\r\nTransfer-Encoding: chunked");
    }
    if (_encoder != nullptr) {
      absl::StrAppend(&head, "\r\nContent-Encoding: ", _coding->token,
                      "\r\nVary: Accept-Encoding");
    }
    absl::StrAppend(&head,
                    "\r\nConnection: ", _keep_alive ? "keep-alive" : "close",
                    "\r\n", _extra, extra_headers, "\r\n");
    message::Writer writer{_send};
    writer.Write(head);
    writer.Commit(false);
    return bodiless;
  }

  message::Buffer& _send;
  ResponseSink& _sink;
  const ContentCoding* _coding = nullptr;
  std::unique_ptr<ContentEncoder> _encoder;
  // The in-progress chunk's Writer (live between BeginChunk and EndChunk).
  std::optional<message::Writer> _chunk;
  uint8_t* _chunk_header = nullptr;
  size_t _chunk_start = 0;
  uint64_t _remaining = 0;
  State _state = State::kIdle;
  bool _keep_alive;
  bool _head_only;
  std::string_view _extra;
};

}  // namespace sdb::network::http
