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
#include <type_traits>

#include "network/http/codecs/codec.h"
#include "network/http/pooled.h"

namespace sdb::network::http {
namespace {

inline constexpr int kDefaultLevel = 6;

// https://www.zlib.net/manual.html#Advanced : windowBits 15 + 16 selects the
// gzip wrapper around deflate.
struct GzipFormat {
  static constexpr std::string_view kName = "gzip";
  static constexpr int kWindowBits = 15 + 16;
};

struct ZlibFormat {
  static constexpr std::string_view kName = "deflate";
  static constexpr int kWindowBits = 15;
};

template<typename Format>
struct DeflateState {
  DeflateState() {
    const int rc = deflateInit2(&stream, kDefaultLevel, Z_DEFLATED,
                                Format::kWindowBits, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      ThrowCodecError(Format::kName, zError(rc));
    }
  }

  ~DeflateState() { deflateEnd(&stream); }

  bool Reset() noexcept { return deflateReset(&stream) == Z_OK; }

  void SetLevel(int new_level) {
    if (new_level == level) {
      return;
    }
    const int rc = deflateParams(&stream, new_level, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      ThrowCodecError(Format::kName, zError(rc));
    }
    level = new_level;
  }

  z_stream stream{};
  int level = kDefaultLevel;
};

template<typename Format>
struct InflateState {
  InflateState() {
    if (inflateInit2(&stream, Format::kWindowBits) != Z_OK) {
      ThrowCodecError(Format::kName, "cannot initialize the decoder");
    }
  }

  ~InflateState() { inflateEnd(&stream); }

  bool Reset() noexcept {
    return inflateReset2(&stream, Format::kWindowBits) == Z_OK;
  }

  z_stream stream{};
  std::array<uint8_t, kOutBlock> out;
};

template<typename Format>
class DeflateEncoder final : public ContentEncoder {
 public:
  explicit DeflateEncoder(int level) {
    _state->SetLevel(
      ClampLevel(level, kDefaultLevel, Z_BEST_SPEED, Z_BEST_COMPRESSION));
  }

  void Encode(std::string_view in, bool finish, EncodeOutput& out) override {
    auto& stream = _state->stream;
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    const int flush = finish ? Z_FINISH : Z_NO_FLUSH;
    int rc = Z_OK;
    do {
      out.Write(kOutBlock, [&](uint8_t* dst) {
        stream.next_out = dst;
        stream.avail_out = static_cast<uInt>(kOutBlock);
        rc = deflate(&stream, flush);
        // Z_BUF_ERROR only reports "no progress possible", which is expected
        // once the input is drained; anything else is fatal.
        if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
          ThrowCodecError(Format::kName, zError(rc));
        }
        return kOutBlock - stream.avail_out;
      });
    } while (rc != Z_BUF_ERROR && stream.avail_out == 0);
    if (stream.avail_in != 0) {
      ThrowCodecError(Format::kName, "input not consumed");
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
      ThrowCodecError(Format::kName, "output exceeds the deflate bound");
    }
    out.resize(out.size() - stream.avail_out);
  }

 private:
  Pooled<DeflateState<Format>> _state;
};

bool IsZlibHeader(std::string_view in) {
  const auto cmf = static_cast<uint8_t>(in[0]);
  if ((cmf & 0x0F) != Z_DEFLATED || (cmf >> 4) > 7) {
    return false;
  }
  return in.size() < 2 || (cmf * 256 + static_cast<uint8_t>(in[1])) % 31 == 0;
}

template<typename Format>
class InflateDecoder final : public ContentDecoder {
 public:
  void Decode(std::string_view in, bool finish,
              absl::FunctionRef<void(std::string_view)> sink) override {
    auto& stream = _state->stream;
    auto& out = _state->out;
    if constexpr (std::is_same_v<Format, ZlibFormat>) {
      if (!_started && !in.empty()) {
        _started = true;
        if (!IsZlibHeader(in) && inflateReset2(&stream, -15) != Z_OK) {
          ThrowCorrupt(Format::kName, "cannot initialize the decoder");
        }
      }
    }
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    for (;;) {
      if (_done) {
        if (stream.avail_in == 0) {
          break;
        }
        if constexpr (!std::is_same_v<Format, GzipFormat>) {
          ThrowCorrupt(Format::kName, "data after the end of the stream");
        }
        if (inflateReset(&stream) != Z_OK) {
          ThrowCorrupt(Format::kName, "cannot start the next member");
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
        ThrowCorrupt(Format::kName, stream.msg ? stream.msg : zError(rc));
      }
      if (stream.avail_in == 0 && stream.avail_out != 0) {
        break;
      }
    }
    if (finish && !_done) {
      ThrowCorrupt(Format::kName, "truncated body");
    }
  }

 private:
  Pooled<InflateState<Format>> _state;
  bool _started = false;
  bool _done = false;
};

}  // namespace

std::unique_ptr<ContentEncoder> MakeGzipEncoder(int level) {
  return std::make_unique<DeflateEncoder<GzipFormat>>(level);
}

std::unique_ptr<ContentDecoder> MakeGzipDecoder() {
  return std::make_unique<InflateDecoder<GzipFormat>>();
}

std::unique_ptr<ContentEncoder> MakeDeflateEncoder(int level) {
  return std::make_unique<DeflateEncoder<ZlibFormat>>(level);
}

std::unique_ptr<ContentDecoder> MakeDeflateDecoder() {
  return std::make_unique<InflateDecoder<ZlibFormat>>();
}

}  // namespace sdb::network::http
