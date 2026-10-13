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

#include <cstring>
#include <iresearch/utils/debugging.hpp>
#include <string>
#include <utility>

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
  if (ShouldEncode(status, body.size()) &&
      FixedEncoded(status, content_type, body, extra_headers)) {
    return;
  }
  message::Writer writer{_send};
  const uint64_t content_length = body.size();
  EncodeHead(writer, status, content_type, &content_length, false,
             extra_headers);
  if (!_head_only && !Bodiless(status)) {
    writer.Write(body);
  }
  writer.Commit(true);
  _state = State::kFinished;
}

bool HttpResponseWriter::FixedEncoded(HttpStatus status,
                                      std::string_view content_type,
                                      std::string_view body,
                                      std::string_view extra_headers) {
  ContentEncoder encoder{_coding, _level};
  message::Writer writer{_send};
  const uint64_t content_length = body.size();
  const auto length = EncodeHead(writer, status, content_type, &content_length,
                                 true, extra_headers);
  const size_t head = writer.Written();
  encoder.EncodeAll(body, writer);
  const size_t size = writer.Written() - head;
  if (size >= body.size()) {
    return false;
  }
  PatchNumber(length, size);
  writer.Commit(true);
  _state = State::kFinished;
  return true;
}

void HttpResponseWriter::WriteHead(HttpStatus status,
                                   std::string_view content_type,
                                   uint64_t content_length,
                                   std::string_view extra_headers) {
  if (ShouldEncode(status, content_length)) {
    WriteHeadChunked(status, content_type, extra_headers);
    return;
  }
  message::Writer writer{_send};
  EncodeHead(writer, status, content_type, &content_length, false,
             extra_headers);
  writer.Commit(false);
  _state = State::kFixedBody;
  _remaining = (_head_only || Bodiless(status)) ? 0 : content_length;
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
      if (_encoder) {
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
  if (ShouldEncode(status, kMinCompressBytes)) {
    _encoder.emplace(_coding, _level);
  }
  message::Writer writer{_send};
  EncodeHead(writer, status, content_type, nullptr, _encoder.has_value(),
             extra_headers);
  writer.Commit(false);
  _state = State::kChunkedBody;
}

void HttpResponseWriter::BeginChunk() {
  SDB_ASSERT(!_encoder);
  BeginRawChunk();
}

void HttpResponseWriter::EndChunk() { EndRawChunk(); }

void HttpResponseWriter::Finish() {
  switch (_state) {
    case State::kChunkedBody: {
      SDB_ASSERT(!_chunk.has_value());
      if (_encoder) {
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
  return _coding != ContentCoding::Identity && !_head_only &&
         !Bodiless(status) && body_size >= kMinCompressBytes;
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
  _encoder->Encode(data, finish, *_chunk);
  if (_chunk->Written() == _chunk_start) {
    _chunk.reset();
    _chunk_header = nullptr;
    return;
  }
  EndRawChunk();
}

std::span<uint8_t> HttpResponseWriter::EncodeHead(
  message::Writer& writer, HttpStatus status, std::string_view content_type,
  const uint64_t* content_length, bool encoded,
  std::string_view extra_headers) {
  SDB_ASSERT(_state == State::kIdle);
  std::span<uint8_t> length;
  writer.Write("HTTP/1.1 ");
  WriteNumber(writer, std::to_underlying(status));
  writer.Write(" ");
  writer.Write(ReasonPhrase(status));
  writer.Write("\r\nContent-Type: ");
  writer.Write(content_type);
  if (Bodiless(status)) {
    // no Content-Length, no Transfer-Encoding
  } else if (content_length != nullptr) {
    writer.Write("\r\nContent-Length: ");
    length = WriteNumber(writer, *content_length);
  } else {
    writer.Write("\r\nTransfer-Encoding: chunked");
  }
  if (encoded) {
    writer.Write("\r\nContent-Encoding: ");
    writer.Write(ContentCodingToken(_coding));
    writer.Write("\r\nVary: Accept-Encoding");
  }
  if (_keep_alive) {
    writer.Write("\r\nConnection: keep-alive\r\n");
  } else {
    writer.Write("\r\nConnection: close\r\n");
  }
  writer.Write(_extra);
  writer.Write(extra_headers);
  writer.Write("\r\n");
  return length;
}

std::span<uint8_t> HttpResponseWriter::WriteNumber(message::Writer& writer,
                                                   uint64_t value) {
  std::span<uint8_t> number;
  writer.Write(absl::numbers_internal::kFastToBufferSize, [&](uint8_t* out) {
    auto* begin = reinterpret_cast<char*>(out);
    const auto size = static_cast<size_t>(
      absl::numbers_internal::FastIntToBuffer(value, begin) - begin);
    number = {out, size};
    return size;
  });
  return number;
}

void HttpResponseWriter::PatchNumber(std::span<uint8_t> field, uint64_t value) {
  char digits[absl::numbers_internal::kFastToBufferSize];
  const auto size = static_cast<size_t>(
    absl::numbers_internal::FastIntToBuffer(value, digits) - digits);
  SDB_ASSERT(size <= field.size());
  const size_t pad = field.size() - size;
  std::memset(field.data(), ' ', pad);
  std::memcpy(field.data() + pad, digits, size);
}

}  // namespace sdb::network::http
