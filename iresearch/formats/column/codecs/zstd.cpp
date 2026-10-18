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

#include "iresearch/formats/column/codecs/zstd.hpp"

#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include "iresearch/formats/column/codecs/compression.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

const duckdb::CompressionFunction* ZstdFunction(duckdb::PhysicalType physical) {
  static const auto kFunction =
    StringFunction(duckdb::CompressionType::COMPRESSION_COL_ZSTD,
                   duckdb::CompressionValidity::REQUIRES_VALIDITY);
  return physical == duckdb::PhysicalType::VARCHAR ? &kFunction : nullptr;
}

size_t Leaf<ByteCodec::Zstd>::Bound(size_t raw_size) noexcept {
  return ZSTD_compressBound(raw_size);
}

LeafCompressor<ByteCodec::Zstd>::~LeafCompressor() {
  ZSTD_freeCDict(_cdict);
  ZSTD_freeCDict(_trained_cdict);
}

void LeafCompressor<ByteCodec::Zstd>::LoadTrained(
  const TrainedDictionary& dictionary) {
  if (_trained != dictionary.Id() || _trained_level != _level) {
    ZSTD_freeCDict(_trained_cdict);
    _trained = 0;
    const auto bytes = dictionary.Bytes();
    _trained_cdict = ZSTD_createCDict_advanced(
      bytes.data(), bytes.size(), ZSTD_dlm_byRef, ZSTD_dct_auto,
      ZSTD_getCParams(_level, kTrainedFrameBytes, bytes.size()),
      ZSTD_defaultCMem);
    SDB_ENSURE(_trained_cdict, "zstd: cannot load a trained dictionary");
    _trained = dictionary.Id();
    _trained_level = _level;
  }
  _active = _trained_cdict;
}

void LeafCompressor<ByteCodec::Zstd>::LoadDictionary(
  std::string_view dictionary) {
  ZSTD_freeCDict(_cdict);
  _cdict = ZSTD_createCDict_advanced(
    dictionary.data(), dictionary.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
    ZSTD_getCParams(_level, kZstdDictionaryFrameBytes, dictionary.size()),
    ZSTD_defaultCMem);
  SDB_ENSURE(_cdict, "zstd: cannot create a compression dictionary");
  _active = _cdict;
}

size_t LeafCompressor<ByteCodec::Zstd>::Compress(const char* src, size_t size,
                                                 char* dst, size_t capacity) {
  const auto n =
    _active
      ? ZSTD_compress_usingCDict(_ctx.get(), dst, capacity, src, size, _active)
      : ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level);
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

LeafDecompressor<ByteCodec::Zstd>::~LeafDecompressor() {
  ZSTD_freeDDict(_ddict);
}

void LeafDecompressor<ByteCodec::Zstd>::SetDictionary(
  std::string_view dictionary) {
  _active = nullptr;
  if (dictionary.empty()) {
    return;
  }
  if (dictionary.data() != _loaded.data() ||
      dictionary.size() != _loaded.size()) {
    ZSTD_freeDDict(_ddict);
    _loaded = {};
    _ddict = ZSTD_createDDict_advanced(dictionary.data(), dictionary.size(),
                                       ZSTD_dlm_byRef, ZSTD_dct_rawContent,
                                       ZSTD_defaultCMem);
    SDB_ENSURE(_ddict, "zstd: cannot create a decompression dictionary");
    _loaded = dictionary;
  }
  _active = _ddict;
}

bool LeafDecompressor<ByteCodec::Zstd>::Decompress(const char* src, size_t size,
                                                   char* dst,
                                                   size_t raw_size) noexcept {
  const auto n =
    ZSTD_decompress_usingDDict(_ctx.get(), dst, raw_size, src, size, _active);
  return !ZSTD_isError(n) && n == raw_size;
}

size_t LeafDecompressor<ByteCodec::Zstd>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t want,
  size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

}  // namespace irs::codecs
