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
#define ZDICT_STATIC_LINKING_ONLY

#include <zdict.h>
#include <zstd.h>

#include <utility>

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
zstd frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 32 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary.
*/

namespace irs::codecs {
namespace {

constexpr ZDICT_fastCover_params_t kTrainParams{
  .k = 256,
  .d = 8,
  .f = 18,
  .nbThreads = 1,
  .splitPoint = 1.0,
  .accel = 10,
  .zParams = {.compressionLevel = 1},
};

}  // namespace

const duckdb::CompressionFunction& ZstdFunction() {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::BlobLeaf<Zstd>>>(
      duckdb::CompressionType::COMPRESSION_COL_ZSTD);
  return kFunction;
}

const duckdb::CompressionFunction& DictZstdFunction() {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::BlobLeaf<Zstd>>>(
      duckdb::CompressionType::COMPRESSION_DICT_ZSTD);
  return kFunction;
}

Zstd::Dictionary::Dictionary(uint8_t level, std::string_view bytes)
  : _dictionary{ZSTD_createCDict_advanced(
      bytes.data(), bytes.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
      ZSTD_getCParams(EffectiveLevel<Zstd>(level), kZstdDictionaryFrameBytes,
                      bytes.size()),
      ZSTD_defaultCMem)} {
  SDB_ENSURE(_dictionary, "zstd: cannot create a compression dictionary");
}

Zstd::Compressor::Compressor(uint8_t level)
  : _level{EffectiveLevel<Zstd>(level)}, _ctx{ZSTD_createCCtx()} {
  SDB_ENSURE(_ctx, "zstd: cannot create a compression context");
}

Zstd::Decompressor::Decompressor() : _ctx{ZSTD_createDCtx()} {
  SDB_ENSURE(_ctx, "zstd: cannot create a decompression context");
}

size_t Zstd::Compressor::Compress(const char* src, size_t size, char* dst,
                                  size_t capacity,
                                  const Dictionary* dictionary) {
  const auto n =
    dictionary
      ? ZSTD_compress_usingCDict(_ctx.get(), dst, capacity, src, size,
                                 dictionary->Get())
      : ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level);
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

bool Zstd::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                    size_t raw_size,
                                    std::string_view dictionary) {
  if (dictionary.empty()) {
    return Run(src, size, dst, raw_size, nullptr);
  }
  if (dictionary.data() != _raw_bytes.data() ||
      dictionary.size() != _raw_bytes.size()) {
    _raw_bytes = {};
    _raw.reset(ZSTD_createDDict_advanced(dictionary.data(), dictionary.size(),
                                         ZSTD_dlm_byRef, ZSTD_dct_rawContent,
                                         ZSTD_defaultCMem));
    SDB_ENSURE(_raw, "zstd: cannot create a decompression dictionary");
    _raw_bytes = dictionary;
  }
  return Run(src, size, dst, raw_size, _raw.get());
}

bool Zstd::Decompressor::Run(const char* src, size_t size, char* dst,
                             size_t raw_size,
                             const ZSTD_DDict* dictionary) noexcept {
  const auto n = ZSTD_decompress_usingDDict(_ctx.get(), dst, raw_size, src,
                                            size, dictionary);
  return !ZSTD_isError(n) && n == raw_size;
}

void TrainDictionary(std::string_view samples, std::span<const size_t> sizes,
                     size_t capacity, std::string& dictionary) {
  SDB_ASSERT(std::in_range<unsigned>(sizes.size()));
  dictionary.resize_and_overwrite(capacity, [&](char* buf, size_t size) {
    const auto n = ZDICT_trainFromBuffer_fastCover(
      buf, size, samples.data(), sizes.data(),
      static_cast<unsigned>(sizes.size()), kTrainParams);
    return ZDICT_isError(n) ? 0 : n;
  });
  if (dictionary.empty()) {
    return;
  }
  const auto header =
    ZDICT_getDictHeaderSize(dictionary.data(), dictionary.size());
  if (ZDICT_isError(header) || header >= dictionary.size()) {
    dictionary.clear();
    return;
  }
  dictionary.erase(0, header);
}

}  // namespace irs::codecs
