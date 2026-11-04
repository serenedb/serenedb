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

#include "iresearch/formats/column/codecs/lz4.hpp"

#define LZ4_STATIC_LINKING_ONLY
#define LZ4_HC_STATIC_LINKING_ONLY

#include <lz4.h>
#include <lz4hc.h>

#include <limits>
#include <type_traits>

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
lz4 frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or
64 KiB frames without a dictionary; or 16 KiB frames compressed with the
column's trained dictionary, stored once per column.
*/

namespace irs::codecs {

static_assert(Lz4::kMaxLevel == LZ4HC_CLEVEL_MAX);

const duckdb::CompressionFunction& Lz4Function() {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::BlobLeaf<Lz4>>>(
      duckdb::CompressionType::COMPRESSION_LZ4);
  return kFunction;
}

const duckdb::CompressionFunction& DictLz4Function() {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::BlobLeaf<Lz4>>>(
      duckdb::CompressionType::COMPRESSION_DICT_LZ4);
  return kFunction;
}

size_t Lz4::Bound(size_t raw_size) noexcept {
  SDB_ASSERT(raw_size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw_size)));
}

namespace {

template<typename Stream>
std::unique_ptr<Stream, Lz4Free> NewStream() {
  std::unique_ptr<Stream, Lz4Free> stream;
  if constexpr (std::is_same_v<Stream, LZ4_stream_t>) {
    stream.reset(LZ4_createStream());
  } else {
    stream.reset(LZ4_createStreamHC());
  }
  SDB_ENSURE(stream, "lz4: cannot create a compression stream");
  return stream;
}

}  // namespace

Lz4::Compressor::Compressor(uint8_t level, std::string_view dictionary) {
  SDB_ASSERT(dictionary.size() <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  level = EffectiveLevel<Lz4>(level);
  const auto size = static_cast<int>(dictionary.size());
  if (level <= 1) {
    auto& fast = _streams.emplace<Fast>(NewStream<LZ4_stream_t>(), nullptr);
    if (!dictionary.empty()) {
      fast.dictionary = NewStream<LZ4_stream_t>();
      LZ4_loadDict(fast.dictionary.get(), dictionary.data(), size);
    }
    return;
  }
  auto& high =
    _streams.emplace<High>(level, NewStream<LZ4_streamHC_t>(), nullptr);
  if (!dictionary.empty()) {
    high.dictionary = NewStream<LZ4_streamHC_t>();
    LZ4_resetStreamHC_fast(high.dictionary.get(), level);
    LZ4_loadDictHC(high.dictionary.get(), dictionary.data(), size);
  }
}

size_t Lz4::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(capacity <= std::numeric_limits<int>::max());
  const auto src_size = static_cast<int>(size);
  const auto dst_capacity = static_cast<int>(capacity);
  int n = 0;
  if (auto* fast = std::get_if<Fast>(&_streams)) {
    if (fast->dictionary) {
      LZ4_resetStream_fast(fast->work.get());
      LZ4_attach_dictionary(fast->work.get(), fast->dictionary.get());
      n = LZ4_compress_fast_continue(fast->work.get(), src, dst, src_size,
                                     dst_capacity, 1);
    } else {
      n = LZ4_compress_fast_extState_fastReset(fast->work.get(), src, dst,
                                               src_size, dst_capacity, 1);
    }
  } else {
    auto& high = std::get<High>(_streams);
    if (high.dictionary) {
      LZ4_resetStreamHC_fast(high.work.get(), high.level);
      LZ4_attach_HC_dictionary(high.work.get(), high.dictionary.get());
      n = LZ4_compress_HC_continue(high.work.get(), src, dst, src_size,
                                   dst_capacity);
    } else {
      n = LZ4_compress_HC_extStateHC_fastReset(
        high.work.get(), src, dst, src_size, dst_capacity, high.level);
    }
  }
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

bool Lz4::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size,
                                   std::string_view dictionary) noexcept {
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  const int n = LZ4_decompress_safe_usingDict(
    src, dst, static_cast<int>(size), static_cast<int>(raw_size),
    dictionary.data(), static_cast<int>(dictionary.size()));
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t Lz4::Decompressor::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t want, size_t raw_size,
  std::string_view dictionary) noexcept {
  SDB_ASSERT(want <= raw_size);
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return 0;
  }
  const int n = LZ4_decompress_safe_partial_usingDict(
    src, dst, static_cast<int>(size), static_cast<int>(want),
    static_cast<int>(raw_size), dictionary.data(),
    static_cast<int>(dictionary.size()));
  return n >= 0 && static_cast<size_t>(n) >= want ? static_cast<size_t>(n) : 0;
}

}  // namespace irs::codecs
