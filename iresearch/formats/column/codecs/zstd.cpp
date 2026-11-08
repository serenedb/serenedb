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

#include <algorithm>
#include <utility>

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
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

Zstd::Compressor::Compressor(uint8_t level)
  : _level{EffectiveLevel<Zstd>(level)}, _ctx{ZSTD_createCCtx()} {
  SDB_ENSURE(_ctx, "zstd: cannot create a compression context");
}

bool Zstd::Compressor::Train(std::string_view frames,
                             std::span<const size_t> sizes) {
  TrainDictionary(frames, sizes, kDictionaryCap, _dictionary);
  return Pack();
}

bool Zstd::Compressor::Pack() {
  _stored.clear();
  if (_dictionary.empty()) {
    return false;
  }
  PackDictionary(_dictionary, Lz4::Bound(_dictionary.size()), _stored,
                 [](const char* src, size_t size, char* dst, size_t capacity) {
                   return static_cast<size_t>(std::max(
                     0, LZ4_compress_default(src, dst, static_cast<int>(size),
                                             static_cast<int>(capacity))));
                 });
  Attach();
  return true;
}

void Zstd::Compressor::Use(std::string_view raw) {
  _dictionary.assign(raw);
  _stored.clear();
  Attach();
}

void Zstd::Compressor::Attach() {
  _attached.reset(ZSTD_createCDict_advanced(
    _dictionary.data(), _dictionary.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
    ZSTD_getCParams(_level, kZstdDictionaryFrameBytes, _dictionary.size()),
    ZSTD_defaultCMem));
  SDB_ENSURE(_attached, "zstd: cannot create a compression dictionary");
}

size_t Zstd::Compressor::Compress(const char* src, size_t size, char* dst,
                                  size_t capacity) {
  if (_dictionary.empty()) {
    return Plain(src, size, dst, capacity);
  }
  const auto n = ZSTD_compress_usingCDict(_ctx.get(), dst, capacity, src, size,
                                          _attached.get());
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

size_t Zstd::Compressor::Plain(const char* src, size_t size, char* dst,
                               size_t capacity) {
  const auto n =
    ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level);
  SDB_ENSURE(!ZSTD_isError(n),
             "zstd compression failed: ", ZSTD_getErrorName(n));
  return n;
}

Zstd::Decompressor::Decompressor() : _ctx{ZSTD_createDCtx()} {
  SDB_ENSURE(_ctx, "zstd: cannot create a decompression context");
}

bool Zstd::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                    size_t raw_size) {
  return Run(src, size, dst, raw_size, nullptr);
}

bool Zstd::Decompressor::Load(std::string_view stored, size_t size) {
  return FillDictionary(_window, _dictionary, size, [&](char* buf) {
    return UnpackDictionary(stored, buf, size, Lz4::Decompressor::Decompress);
  });
}

bool Zstd::Decompressor::LoadFrame(const char* src, size_t size,
                                   size_t raw_size) {
  return FillDictionary(_window, _dictionary, raw_size, [&](char* buf) {
    return Decompress(src, size, buf, raw_size);
  });
}

std::string_view Zstd::Decompressor::Inflate(const char* src, size_t size,
                                             char* dst, size_t,
                                             size_t raw_size) {
  if (_dictionary == 0) {
    return Decompress(src, size, dst, raw_size)
             ? std::string_view{dst, raw_size}
             : std::string_view{};
  }
  auto* out = FrameAfterDictionary(_window, _dictionary, raw_size);
  return Run(src, size, out, raw_size, Attached())
           ? std::string_view{out, raw_size}
           : std::string_view{};
}

const ZSTD_DDict* Zstd::Decompressor::Attached() {
  const auto dictionary = Dictionary();
  if (dictionary.data() != _attached_bytes.data() ||
      dictionary.size() != _attached_bytes.size()) {
    _attached_bytes = {};
    _attached.reset(ZSTD_createDDict_advanced(
      dictionary.data(), dictionary.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
      ZSTD_defaultCMem));
    SDB_ENSURE(_attached, "zstd: cannot create a decompression dictionary");
    _attached_bytes = dictionary;
  }
  return _attached.get();
}

bool Zstd::Decompressor::Run(const char* src, size_t size, char* dst,
                             size_t raw_size,
                             const ZSTD_DDict* dictionary) noexcept {
  const auto n = ZSTD_decompress_usingDDict(_ctx.get(), dst, raw_size, src,
                                            size, dictionary);
  return !ZSTD_isError(n) && n == raw_size;
}

}  // namespace irs::codecs
