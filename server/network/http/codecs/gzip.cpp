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

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// https://www.zlib.net/manual.html#Advanced : windowBits 15 + 16 selects the
// gzip wrapper around deflate.
class GzipEncoder final : public ContentEncoder {
 public:
  GzipEncoder() {
    const int rc = deflateInit2(&_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      ThrowCodecError("gzip", zError(rc));
    }
    _initialized = true;
  }

  ~GzipEncoder() override {
    if (_initialized) {
      deflateEnd(&_stream);
    }
  }

  void Encode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    _stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    _stream.avail_in = static_cast<uInt>(in.size());
    const int flush = finish ? Z_FINISH : Z_NO_FLUSH;
    do {
      _stream.next_out = _out.data();
      _stream.avail_out = static_cast<uInt>(_out.size());
      const int rc = deflate(&_stream, flush);
      // Z_BUF_ERROR only reports "no progress possible", which is expected
      // once the input is drained; anything else is fatal.
      if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
        ThrowCodecError("gzip", zError(rc));
      }
      const size_t produced = _out.size() - _stream.avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (rc == Z_BUF_ERROR) {
        break;
      }
    } while (_stream.avail_out == 0);
    if (_stream.avail_in != 0) {
      ThrowCodecError("gzip", "input not consumed");
    }
  }

 private:
  z_stream _stream{};
  bool _initialized = false;
  std::array<uint8_t, kOutBlock> _out;
};

class GzipDecoder final : public ContentDecoder {
 public:
  GzipDecoder() {
    if (inflateInit2(&_stream, 16 + MAX_WBITS) != Z_OK) {
      ThrowCodecError("gzip", "cannot initialize the decoder");
    }
  }

  ~GzipDecoder() override { inflateEnd(&_stream); }

  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    _stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    _stream.avail_in = static_cast<uInt>(in.size());
    for (;;) {
      if (_done) {
        if (_stream.avail_in == 0) {
          break;
        }
        if (inflateReset(&_stream) != Z_OK) {
          ThrowCorrupt("gzip", "cannot start the next member");
        }
        _done = false;
      }
      _stream.next_out = _out.data();
      _stream.avail_out = static_cast<uInt>(_out.size());
      const int rc = inflate(&_stream, Z_NO_FLUSH);
      const size_t produced = _out.size() - _stream.avail_out;
      if (produced != 0) {
        sink({reinterpret_cast<const char*>(_out.data()), produced});
      }
      if (rc == Z_STREAM_END) {
        _done = true;
        continue;
      }
      if (rc == Z_BUF_ERROR) {
        break;
      }
      if (rc != Z_OK) {
        ThrowCorrupt("gzip", _stream.msg ? _stream.msg : zError(rc));
      }
      if (_stream.avail_in == 0 && _stream.avail_out != 0) {
        break;
      }
    }
    if (finish && !_done) {
      ThrowCorrupt("gzip", "truncated body");
    }
  }

 private:
  z_stream _stream{};
  bool _done = false;
  std::array<uint8_t, kOutBlock> _out;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeGzipEncoder() {
  return std::make_unique<GzipEncoder>();
}

std::unique_ptr<ContentDecoder> MakeGzipDecoder() {
  return std::make_unique<GzipDecoder>();
}

}  // namespace sdb::network::http
