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

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
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
  void Json(HttpStatus status, std::string_view body);
  void Text(HttpStatus status, std::string_view body);
  void Error(HttpStatus status, std::string_view error_label);
  void Fixed(HttpStatus status, std::string_view content_type,
             std::string_view body, std::string_view extra_headers = {});

  // --- known-length streamed body ----------------------------------------
  // A compressed body has no known length up front, so it is framed chunked.
  void WriteHead(HttpStatus status, std::string_view content_type,
                 uint64_t content_length, std::string_view extra_headers = {});
  void Write(std::string_view data);

  // --- chunked streamed body ----------------------------------------------
  // A compressed stream buffers inside the codec, so Write() no longer maps
  // one-to-one onto chunks the client can consume: bytes leave when a codec
  // block fills or at Finish(). Incremental-delivery endpoints (SSE and the
  // like) must not run on a listener with compression enabled.
  void WriteHeadChunked(HttpStatus status, std::string_view content_type,
                        std::string_view extra_headers = {});

  // Between BeginChunk/EndChunk the raw buffer is exposed: serializers write
  // payload bytes directly (WriteObject and friends), then the seal patches
  // the reserved fixed-width hex length. HEAD requests skip body bytes but
  // keep the same control flow.
  void BeginChunk();
  void EndChunk();

  yaclib::Task<> Drain() { return _sink.Drain(); }
  bool Broken() const noexcept { return _sink.Broken(); }

  // Terminates the response. For chunked bodies writes the last-chunk; for
  // fixed bodies asserts the promised length was written.
  void Finish();

 private:
  enum class State : uint8_t {
    kIdle,
    kFixedBody,
    kChunkedBody,
    kFinished,
  };

  struct Scratch;

  static constexpr size_t kChunkHeaderLen = 10;  // "XXXXXXXX\r\n"

  static bool Bodiless(HttpStatus status) noexcept;
  static void WriteNumber(message::Writer& writer, uint64_t value);

  bool ShouldEncode(HttpStatus status, uint64_t body_size) const noexcept;
  void WriteChunk(std::string_view data);
  void BeginRawChunk();
  void EndRawChunk();
  void EncodeChunks(std::string_view data, bool finish);
  // Returns whether the status is bodiless (1xx/204/304); the caller must then
  // emit no body and no framing length.
  bool EncodeHead(HttpStatus status, std::string_view content_type,
                  const uint64_t* content_length,
                  std::string_view extra_headers);

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
