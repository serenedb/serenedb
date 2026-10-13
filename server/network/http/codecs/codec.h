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

#include <absl/functional/function_ref.h>
#include <lz4frame.h>
#include <zlib.h>

#include <cstddef>
#include <iresearch/utils/noncopyable.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "network/http/compression.h"
#include "server/utils/byte_pool.h"

struct BrotliEncoderStateStruct;
struct BrotliDecoderStateStruct;
struct ZSTD_CCtx_s;
struct ZSTD_DCtx_s;
struct zxc_cstream_s;
struct zxc_dstream_s;

namespace sdb::network::http {

using CodecPool = utils::BytePool<struct HttpCodecsTag>;
using CodecBytes = utils::PooledBytes<CodecPool>;

inline constexpr size_t kOutBlock = 16 * 1024;

inline constexpr LevelRange kZstdLevels{.min = -(1 << 17), .max = 8};
inline constexpr LevelRange kBrotliLevels{.min = 1, .max = 6};
inline constexpr LevelRange kDeflateLevels{.min = 1, .max = 9};
inline constexpr LevelRange kZxcLevels{.min = 1, .max = 5};
inline constexpr LevelRange kLz4Levels{.min = 0, .max = 9};

[[noreturn]] void ThrowCodecError(std::string_view coding,
                                  std::string_view detail);
[[noreturn]] void ThrowCorrupt(std::string_view coding,
                               std::string_view detail);

using DecodeSink = absl::FunctionRef<void(std::string_view)>;

class ZstdEncoder : irs::util::Noncopyable {
 public:
  explicit ZstdEncoder(int level);

  void Encode(std::string_view in, bool finish, message::Writer& out);
  void EncodeAll(std::string_view in, message::Writer& out);

 private:
  struct Deleter {
    void operator()(ZSTD_CCtx_s* cctx) const noexcept;
  };

  std::unique_ptr<ZSTD_CCtx_s, Deleter> _cctx;
};

class ZstdDecoder : irs::util::Noncopyable {
 public:
  ZstdDecoder();

  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  struct Deleter {
    void operator()(ZSTD_DCtx_s* dctx) const noexcept;
  };

  std::unique_ptr<ZSTD_DCtx_s, Deleter> _dctx;
  CodecBytes _out;
  size_t _pending = 1;
};

class BrotliEncoder : irs::util::Noncopyable {
 public:
  explicit BrotliEncoder(int level);
  ~BrotliEncoder();

  void Encode(std::string_view in, bool finish, message::Writer& out);
  void EncodeAll(std::string_view in, message::Writer& out);

 private:
  BrotliEncoderStateStruct* _state;
};

class BrotliDecoder : irs::util::Noncopyable {
 public:
  BrotliDecoder();
  ~BrotliDecoder();

  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  BrotliDecoderStateStruct* _state;
  CodecBytes _out;
  bool _done = false;
};

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
class ZlibEncoder : irs::util::Noncopyable {
 public:
  explicit ZlibEncoder(int level);
  ~ZlibEncoder();

  void Encode(std::string_view in, bool finish, message::Writer& out);
  void EncodeAll(std::string_view in, message::Writer& out);

 private:
  z_stream _stream;
};

template<typename Format>
class ZlibDecoder : irs::util::Noncopyable {
 public:
  ZlibDecoder();
  ~ZlibDecoder();

  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  z_stream _stream;
  CodecBytes _out;
  bool _started = false;
  bool _done = false;
};

using GzipEncoder = ZlibEncoder<GzipFormat>;
using GzipDecoder = ZlibDecoder<GzipFormat>;
using DeflateEncoder = ZlibEncoder<ZlibFormat>;
using DeflateDecoder = ZlibDecoder<ZlibFormat>;

class ZxcEncoder : irs::util::Noncopyable {
 public:
  explicit ZxcEncoder(int level);
  ~ZxcEncoder();

  void Encode(std::string_view in, bool finish, message::Writer& out);

 private:
  zxc_cstream_s* _stream;
};

class ZxcDecoder : irs::util::Noncopyable {
 public:
  ZxcDecoder();

  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  struct Deleter {
    void operator()(zxc_dstream_s* stream) const noexcept;
  };

  std::unique_ptr<zxc_dstream_s, Deleter> _stream;
  CodecBytes _out;
};

class Lz4Encoder : irs::util::Noncopyable {
 public:
  explicit Lz4Encoder(int level);

  void Encode(std::string_view in, bool finish, message::Writer& out);
  void EncodeAll(std::string_view in, message::Writer& out);

 private:
  struct Deleter {
    void operator()(LZ4F_cctx* cctx) const noexcept;
  };

  uint8_t* PendingData() const noexcept;
  std::string_view Pending() const noexcept;
  void Compress(std::string_view slice, bool stable, message::Writer& out);

  std::unique_ptr<LZ4F_cctx, Deleter> _cctx;
  LZ4F_preferences_t _prefs;
  size_t _out_size;
  std::optional<CodecBytes> _pending;
  size_t _pending_half = 0;
  size_t _pending_size = 0;
  bool _started = false;
};

class Lz4Decoder : irs::util::Noncopyable {
 public:
  Lz4Decoder();

  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  struct Deleter {
    void operator()(LZ4F_dctx* dctx) const noexcept;
  };

  std::unique_ptr<LZ4F_dctx, Deleter> _dctx;
  CodecBytes _out;
  size_t _pending = 1;
};

class SnappyEncoder : irs::util::Noncopyable {
 public:
  explicit SnappyEncoder(int level);

  void Encode(std::string_view in, bool finish, message::Writer& out);

 private:
  std::string _pending;
};

class SnappyDecoder : irs::util::Noncopyable {
 public:
  void Decode(std::string_view in, bool finish, DecodeSink sink);

 private:
  std::string _pending;
};

}  // namespace sdb::network::http
