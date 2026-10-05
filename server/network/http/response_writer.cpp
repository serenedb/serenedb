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

#include "network/http/response_writer.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <iresearch/utils/debugging.hpp>
#include <string>
#include <utility>

#include "server/utils/thread_local_pool.h"

namespace sdb::network::http {

std::string_view ReasonPhrase(HttpStatus status) noexcept {
  switch (std::to_underlying(status)) {
    case 100:
      return "Continue";
    case 200:
      return "OK";
    case 201:
      return "Created";
    case 204:
      return "No Content";
    case 301:
      return "Moved Permanently";
    case 304:
      return "Not Modified";
    case 400:
      return "Bad Request";
    case 401:
      return "Unauthorized";
    case 403:
      return "Forbidden";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 406:
      return "Not Acceptable";
    case 408:
      return "Request Timeout";
    case 409:
      return "Conflict";
    case 413:
      return "Content Too Large";
    case 415:
      return "Unsupported Media Type";
    case 417:
      return "Expectation Failed";
    case 429:
      return "Too Many Requests";
    case 431:
      return "Request Header Fields Too Large";
    case 500:
      return "Internal Server Error";
    case 501:
      return "Not Implemented";
    case 503:
      return "Service Unavailable";
    default:
      return std::to_underlying(status) < 400 ? "OK" : "Error";
  }
}

namespace {

class WriterOutput final : public EncodeOutput {
 public:
  explicit WriterOutput(message::Writer& writer) : _writer{writer} {}

  void Write(size_t capacity, absl::FunctionRef<size_t(uint8_t*)> fill) final {
    _writer.Write(capacity, [&](uint8_t* out) { return fill(out); });
  }

 private:
  message::Writer& _writer;
};

}  // namespace

struct HttpResponseWriter::Scratch {
  static constexpr size_t kMaxRetainedBytes = 1 << 20;

  bool Reset() noexcept {
    bytes.clear();
    if (bytes.capacity() > kMaxRetainedBytes) {
      bytes.shrink_to_fit();
    }
    return true;
  }

  std::string bytes;
};

void HttpResponseWriter::Json(HttpStatus status, std::string_view body) {
  Fixed(status, kJsonContentType, body);
}

void HttpResponseWriter::Text(HttpStatus status, std::string_view body) {
  Fixed(status, "text/plain", body);
}

void HttpResponseWriter::Error(HttpStatus status,
                               std::string_view error_label) {
  Json(status, absl::StrCat(R"({"error":")", error_label, R"("})"));
}

void HttpResponseWriter::Fixed(HttpStatus status, std::string_view content_type,
                               std::string_view body,
                               std::string_view extra_headers) {
  // `_encoder != nullptr` means `body` IS the compressed bytes, handed back
  // by the branch below; without that guard an incompressible body (whose
  // encoding is never smaller) would re-enter here forever.
  if (_encoder == nullptr && ShouldEncode(status, body.size())) {
    Pooled<Scratch> scratch;
    auto& compressed = scratch->bytes;
    auto encoder = _coding->make(_level);
    encoder->EncodeAll(body, compressed);
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

void HttpResponseWriter::WriteHead(HttpStatus status,
                                   std::string_view content_type,
                                   uint64_t content_length,
                                   std::string_view extra_headers) {
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

void HttpResponseWriter::Write(std::string_view data) {
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

void HttpResponseWriter::WriteHeadChunked(HttpStatus status,
                                          std::string_view content_type,
                                          std::string_view extra_headers) {
  if (_encoder == nullptr && ShouldEncode(status, kMinCompressBytes)) {
    _encoder = _coding->make(_level);
  }
  EncodeHead(status, content_type, nullptr, extra_headers);
  _state = State::kChunkedBody;
}

void HttpResponseWriter::BeginChunk() {
  SDB_ASSERT(_encoder == nullptr);
  BeginRawChunk();
}

void HttpResponseWriter::EndChunk() { EndRawChunk(); }

void HttpResponseWriter::Finish() {
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

bool HttpResponseWriter::Bodiless(HttpStatus status) noexcept {
  // 1xx/204/304 carry neither a body nor framing length (RFC 9112 6.1).
  const auto code = std::to_underlying(status);
  return status == HttpStatus::NoContent || status == HttpStatus::NotModified ||
         (code >= 100 && code < 200);
}

bool HttpResponseWriter::ShouldEncode(HttpStatus status,
                                      uint64_t body_size) const noexcept {
  return _coding != nullptr && !_head_only && !Bodiless(status) &&
         body_size >= kMinCompressBytes;
}

void HttpResponseWriter::WriteChunk(std::string_view data) {
  BeginRawChunk();
  _chunk->Write(data);
  EndRawChunk();
}

void HttpResponseWriter::BeginRawChunk() {
  SDB_ASSERT(_state == State::kChunkedBody && !_chunk.has_value());
  if (_head_only) {
    return;
  }
  _chunk.emplace(_send);
  _chunk_header = _chunk->Alloc(kChunkHeaderLen);
  _chunk_start = _chunk->Written();
}

void HttpResponseWriter::EndRawChunk() {
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

void HttpResponseWriter::EncodeChunks(std::string_view data, bool finish) {
  SDB_ASSERT(!_head_only);
  if (data.empty() && !finish) {
    return;
  }
  BeginRawChunk();
  WriterOutput output{*_chunk};
  _encoder->Encode(data, finish, output);
  if (_chunk->Written() == _chunk_start) {
    _chunk.reset();
    _chunk_header = nullptr;
    return;
  }
  EndRawChunk();
}

bool HttpResponseWriter::EncodeHead(HttpStatus status,
                                    std::string_view content_type,
                                    const uint64_t* content_length,
                                    std::string_view extra_headers) {
  SDB_ASSERT(_state == State::kIdle);
  const bool bodiless = Bodiless(status);
  message::Writer writer{_send};
  writer.Write("HTTP/1.1 ");
  WriteNumber(writer, std::to_underlying(status));
  writer.Write(" ");
  writer.Write(ReasonPhrase(status));
  writer.Write("\r\nContent-Type: ");
  writer.Write(content_type);
  if (bodiless) {
    // no Content-Length, no Transfer-Encoding
  } else if (content_length != nullptr) {
    writer.Write("\r\nContent-Length: ");
    WriteNumber(writer, *content_length);
  } else {
    writer.Write("\r\nTransfer-Encoding: chunked");
  }
  if (_encoder != nullptr) {
    writer.Write("\r\nContent-Encoding: ");
    writer.Write(_coding->token);
    writer.Write("\r\nVary: Accept-Encoding");
  }
  writer.Write("\r\nConnection: ");
  writer.Write(_keep_alive ? std::string_view{"keep-alive"}
                           : std::string_view{"close"});
  writer.Write("\r\n");
  writer.Write(_extra);
  writer.Write(extra_headers);
  writer.Write("\r\n");
  writer.Commit(false);
  return bodiless;
}

void HttpResponseWriter::WriteNumber(message::Writer& writer, uint64_t value) {
  writer.Write(absl::numbers_internal::kFastToBufferSize, [&](uint8_t* out) {
    auto* begin = reinterpret_cast<char*>(out);
    return static_cast<size_t>(
      absl::numbers_internal::FastIntToBuffer(value, begin) - begin);
  });
}

}  // namespace sdb::network::http
