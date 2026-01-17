////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "lz4compression.hpp"

#include <lz4.h>

#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// can reuse stateless instances
compression::Lz4::Lz4Compressor gLZ4BasicCompressor;
compression::Lz4::Lz4Decompressor gLZ4BasicDecompressor;

inline int Acceleration(const compression::Options::Hint hint) noexcept {
  static constexpr int kFactors[]{0, 2, 0};
  SDB_ASSERT(static_cast<size_t>(hint) < std::size(kFactors));

  return kFactors[static_cast<size_t>(hint)];
}

}  // namespace

static_assert(sizeof(char) == sizeof(byte_type));
static_assert(sizeof(LZ4_streamDecode_t) <= 32);

namespace compression {

bytes_view Lz4::Lz4Compressor::compress(byte_type* src, size_t size,
                                        bstring& out) {
  SDB_ASSERT(size <= static_cast<unsigned>(
                       std::numeric_limits<int>::max()));  // LZ4 API uses int
  const auto src_size = static_cast<int>(size);

  // Ensure we have enough space to store compressed data,
  // but preserve original size
  const uint32_t bound = LZ4_COMPRESSBOUND(src_size);
  out.resize(std::max(out.size(), size_t{bound}));

  const auto* src_data = reinterpret_cast<const char*>(src);
  auto* buf = reinterpret_cast<char*>(out.data());
  const auto buf_size = static_cast<int>(out.size());
  // TODO(mbkkt) use thread local context from serenedb
  const auto lz4_size =
    LZ4_compress_fast(src_data, buf, src_size, buf_size, _acceleration);

  if (lz4_size < 0) [[unlikely]] {
    throw IndexError{"While compressing, error: LZ4 returned negative size"};
  }

  return {reinterpret_cast<const byte_type*>(buf),
          static_cast<size_t>(lz4_size)};
}

bytes_view Lz4::Lz4Decompressor::decompress(const byte_type* src,
                                            size_t src_size, byte_type* dst,
                                            size_t dst_size) {
  SDB_ASSERT(src_size <=
             static_cast<unsigned>(
               std::numeric_limits<int>::max()));  // LZ4 API uses int

  const auto lz4_size = LZ4_decompress_safe(
    reinterpret_cast<const char*>(src), reinterpret_cast<char*>(dst),
    static_cast<int>(src_size),  // LZ4 API uses int
    static_cast<int>(std::min(
      dst_size, static_cast<size_t>(
                  std::numeric_limits<int>::max())))  // LZ4 API uses int
  );

  if (lz4_size < 0) [[unlikely]] {
    return {};  // corrupted index
  }

  return bytes_view{dst, static_cast<size_t>(lz4_size)};
}

Compressor::ptr Lz4::compressor(const Options& opts) {
  const auto acceleration = Acceleration(opts.hint);

  if (0 == acceleration) {
    return memory::to_managed<Lz4Compressor>(gLZ4BasicCompressor);
  }

  return memory::make_managed<Lz4Compressor>(acceleration);
}

Decompressor::ptr Lz4::decompressor() {
  return memory::to_managed<Lz4Decompressor>(gLZ4BasicDecompressor);
}

void Lz4::init() {
  REGISTER_COMPRESSION(Lz4, &Lz4::compressor, &Lz4::decompressor);
}

}  // namespace compression
}  // namespace irs
