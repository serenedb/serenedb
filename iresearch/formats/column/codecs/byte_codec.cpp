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

#include "iresearch/formats/column/codecs/byte_codec.hpp"

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zxc_buffer.h>

#include <limits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

static_assert(Leaf<ByteCodec::Lz4>::kMaxLevel == LZ4HC_CLEVEL_MAX);

size_t Leaf<ByteCodec::Lz4>::Bound(size_t raw_size) noexcept {
  SDB_ASSERT(raw_size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw_size)));
}

size_t Leaf<ByteCodec::Zstd>::Bound(size_t raw_size) noexcept {
  return ZSTD_compressBound(raw_size);
}

size_t Leaf<ByteCodec::Zxc>::Bound(size_t raw_size) noexcept {
  return static_cast<size_t>(zxc_compress_bound(raw_size));
}

LeafCompressor<ByteCodec::Zxc>::LeafCompressor(uint8_t level)
  : _level{EffectiveLevel<ByteCodec::Zxc>(level)},
    _ctx{zxc_create_cctx(nullptr)} {
  SDB_ENSURE(_ctx, "zxc: cannot create a compression context");
}

LeafCompressor<ByteCodec::Zxc>::~LeafCompressor() { zxc_free_cctx(_ctx); }

size_t LeafCompressor<ByteCodec::Zxc>::Compress(const char* src, size_t size,
                                                char* dst, size_t capacity) {
  zxc_compress_opts_t opts{};
  opts.level = _level;
  const auto n = zxc_compress_cctx(_ctx, src, size, dst, capacity, &opts);
  SDB_ENSURE(n > 0, "zxc compression failed: ", n);
  return static_cast<size_t>(n);
}

LeafDecompressor<ByteCodec::Zxc>::LeafDecompressor() : _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

LeafDecompressor<ByteCodec::Zxc>::~LeafDecompressor() { zxc_free_dctx(_ctx); }

bool LeafDecompressor<ByteCodec::Zxc>::Decompress(const char* src, size_t size,
                                                  char* dst,
                                                  size_t raw_size) noexcept {
  const auto n = zxc_decompress_dctx(_ctx, src, size, dst, raw_size, nullptr);
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t LeafDecompressor<ByteCodec::Zxc>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t /*want*/,
  size_t raw_size) noexcept {
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

size_t LeafCompressor<ByteCodec::Lz4>::Compress(const char* src, size_t size,
                                                char* dst, size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(capacity <= std::numeric_limits<int>::max());
  const int n = _level <= 1
                  ? LZ4_compress_default(src, dst, static_cast<int>(size),
                                         static_cast<int>(capacity))
                  : LZ4_compress_HC(src, dst, static_cast<int>(size),
                                    static_cast<int>(capacity), _level);
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

size_t LeafCompressor<ByteCodec::Zstd>::Compress(const char* src, size_t size,
                                                 char* dst, size_t capacity) {
  const auto n =
    ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level);
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

bool LeafDecompressor<ByteCodec::Lz4>::Decompress(const char* src, size_t size,
                                                  char* dst,
                                                  size_t raw_size) noexcept {
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  const int n = LZ4_decompress_safe(src, dst, static_cast<int>(size),
                                    static_cast<int>(raw_size));
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t LeafDecompressor<ByteCodec::Lz4>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t want,
  size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return 0;
  }
  const int n = LZ4_decompress_safe_partial(src, dst, static_cast<int>(size),
                                            static_cast<int>(want),
                                            static_cast<int>(raw_size));
  return n >= 0 && static_cast<size_t>(n) >= want ? static_cast<size_t>(n) : 0;
}

bool LeafDecompressor<ByteCodec::Zstd>::Decompress(const char* src, size_t size,
                                                   char* dst,
                                                   size_t raw_size) noexcept {
  const auto n = ZSTD_decompressDCtx(_ctx.get(), dst, raw_size, src, size);
  return !ZSTD_isError(n) && n == raw_size;
}

size_t LeafDecompressor<ByteCodec::Zstd>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t want,
  size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

}  // namespace irs::codecs
