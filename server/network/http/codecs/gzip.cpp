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

#include <zlib.h>

#include <iresearch/utils/string_utils.hpp>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://www.zlib.net/manual.html#Advanced : windowBits 15 + 16 selects the
// gzip wrapper around deflate.
struct DeflateState {
  DeflateState() {
    const int rc = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      ThrowCodecError("gzip", zError(rc));
    }
  }

  ~DeflateState() { deflateEnd(&stream); }

  bool Reset() noexcept { return deflateReset(&stream) == Z_OK; }

  z_stream stream{};
  std::array<uint8_t, kOutBlock> out;
};

struct InflateState {
  InflateState() {
    if (inflateInit2(&stream, 16 + MAX_WBITS) != Z_OK) {
      ThrowCodecError("gzip", "cannot initialize the decoder");
    }
  }

  ~InflateState() { inflateEnd(&stream); }

  bool Reset() noexcept { return inflateReset(&stream) == Z_OK; }

  z_stream stream{};
  std::array<uint8_t, kOutBlock> out;
};

class GzipEncoder final : public ContentEncoder {
 public:
  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& stream = _state->stream;
    auto& out = _state->out;
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    const int flush = finish ? Z_FINISH : Z_NO_FLUSH;
    do {
      stream.next_out = out.data();
      stream.avail_out = static_cast<uInt>(out.size());
      const int rc = deflate(&stream, flush);
      // Z_BUF_ERROR only reports "no progress possible", which is expected
      // once the input is drained; anything else is fatal.
      if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
        ThrowCodecError("gzip", zError(rc));
      }
      const size_t produced = out.size() - stream.avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(out.data()), produced});
      }
      if (rc == Z_BUF_ERROR) {
        break;
      }
    } while (stream.avail_out == 0);
    if (stream.avail_in != 0) {
      ThrowCodecError("gzip", "input not consumed");
    }
  }

  void EncodeAll(std::string_view in, std::string& out) override {
    auto& stream = _state->stream;
    irs::utils::StrResize(out, deflateBound(&stream, in.size()));
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    stream.next_out = reinterpret_cast<Bytef*>(out.data());
    stream.avail_out = static_cast<uInt>(out.size());
    if (deflate(&stream, Z_FINISH) != Z_STREAM_END) {
      ThrowCodecError("gzip", "output exceeds the deflate bound");
    }
    out.resize(out.size() - stream.avail_out);
  }

 private:
  Pooled<DeflateState> _state;
};

class GzipDecoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& stream = _state->stream;
    auto& out = _state->out;
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    for (;;) {
      if (_done) {
        if (stream.avail_in == 0) {
          break;
        }
        if (inflateReset(&stream) != Z_OK) {
          ThrowCorrupt("gzip", "cannot start the next member");
        }
        _done = false;
      }
      stream.next_out = out.data();
      stream.avail_out = static_cast<uInt>(out.size());
      const int rc = inflate(&stream, Z_NO_FLUSH);
      const size_t produced = out.size() - stream.avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(out.data()), produced});
      }
      if (rc == Z_STREAM_END) {
        _done = true;
        continue;
      }
      if (rc == Z_BUF_ERROR) {
        break;
      }
      if (rc != Z_OK) {
        ThrowCorrupt("gzip", stream.msg ? stream.msg : zError(rc));
      }
      if (stream.avail_in == 0 && stream.avail_out != 0) {
        break;
      }
    }
    if (finish && !_done) {
      ThrowCorrupt("gzip", "truncated body");
    }
  }

 private:
  Pooled<InflateState> _state;
  bool _done = false;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeGzipEncoder() {
  return std::make_unique<GzipEncoder>();
}

std::unique_ptr<ContentDecoder> MakeGzipDecoder() {
  return std::make_unique<GzipDecoder>();
}

}  // namespace sdb::network::http
