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

#include <memory>
#include <utility>

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
lz4 frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary.
*/

namespace irs::codecs {

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

Lz4::Compressor::Compressor(uint8_t)
  : _work{std::make_unique_for_overwrite<LZ4_stream_t>()} {
  SDB_ENSURE(LZ4_initStream(_work.get(), sizeof(LZ4_stream_t)),
             "lz4: cannot initialize a compression stream");
}

bool Lz4::Compressor::Train(std::string_view frames,
                            std::span<const size_t> sizes) {
  TrainDictionary(frames, sizes, kDictionaryCap, _dictionary);
  return Pack();
}

bool Lz4::Compressor::Pack() {
  _stored.clear();
  if (_dictionary.empty()) {
    return false;
  }
  PackDictionary(_dictionary, Bound(_dictionary.size()), _stored,
                 [&](const char* src, size_t size, char* dst, size_t capacity) {
                   return Plain(src, size, dst, capacity);
                 });
  Attach();
  return true;
}

void Lz4::Compressor::Use(std::string_view raw) {
  _dictionary.assign(raw);
  _stored.clear();
  Attach();
}

void Lz4::Compressor::Attach() {
  SDB_ASSERT(_dictionary.size() <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  if (!_attached) {
    _attached = std::make_unique_for_overwrite<LZ4_stream_t>();
  }
  LZ4_loadDict(_attached.get(), _dictionary.data(),
               static_cast<int>(_dictionary.size()));
}

size_t Lz4::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  if (_dictionary.empty()) {
    return Plain(src, size, dst, capacity);
  }
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  LZ4_resetStream_fast(_work.get());
  LZ4_attach_dictionary(_work.get(), _attached.get());
  const int n =
    LZ4_compress_fast_continue(_work.get(), src, dst, static_cast<int>(size),
                               static_cast<int>(capacity), 1);
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

size_t Lz4::Compressor::Plain(const char* src, size_t size, char* dst,
                              size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  const int n = LZ4_compress_fast_extState_fastReset(
    _work.get(), src, dst, static_cast<int>(size), static_cast<int>(capacity),
    1);
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

Lz4Hc::Compressor::Compressor(uint8_t level)
  : _level{EffectiveLevel<Lz4Hc>(level)}, _work{LZ4_createStreamHC()} {
  SDB_ENSURE(_work, "lz4: cannot create a compression stream");
}

bool Lz4Hc::Compressor::Train(std::string_view frames,
                              std::span<const size_t> sizes) {
  TrainDictionary(frames, sizes, kDictionaryCap, _dictionary);
  return Pack();
}

bool Lz4Hc::Compressor::Pack() {
  _stored.clear();
  if (_dictionary.empty()) {
    return false;
  }
  PackDictionary(_dictionary, Bound(_dictionary.size()), _stored,
                 [&](const char* src, size_t size, char* dst, size_t capacity) {
                   return Plain(src, size, dst, capacity);
                 });
  Attach();
  return true;
}

void Lz4Hc::Compressor::Use(std::string_view raw) {
  _dictionary.assign(raw);
  _stored.clear();
  Attach();
}

void Lz4Hc::Compressor::Attach() {
  SDB_ASSERT(_dictionary.size() <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  if (!_attached) {
    _attached = std::make_unique_for_overwrite<LZ4_streamHC_t>();
  }
  LZ4_setCompressionLevel(_attached.get(), _level);
  LZ4_favorDecompressionSpeed(_attached.get(), 0);
  _attached->internal_donotuse.hasWorkspace = 0;
  LZ4_loadDictHC(_attached.get(), _dictionary.data(),
                 static_cast<int>(_dictionary.size()));
}

size_t Lz4Hc::Compressor::Compress(const char* src, size_t size, char* dst,
                                   size_t capacity) {
  if (_dictionary.empty()) {
    return Plain(src, size, dst, capacity);
  }
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  LZ4_resetStreamHC_fast(_work.get(), _level);
  LZ4_attach_HC_dictionary(_work.get(), _attached.get());
  const int n = LZ4_compress_HC_continue(
    _work.get(), src, dst, static_cast<int>(size), static_cast<int>(capacity));
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

size_t Lz4Hc::Compressor::Plain(const char* src, size_t size, char* dst,
                                size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(std::in_range<int>(capacity));
  const int n = LZ4_compress_HC_extStateHC_fastReset(
    _work.get(), src, dst, static_cast<int>(size), static_cast<int>(capacity),
    _level);
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

bool Lz4::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return false;
  }
  const int n = LZ4_decompress_safe(src, dst, static_cast<int>(size),
                                    static_cast<int>(raw_size));
  return n == static_cast<int>(raw_size);
}

size_t Lz4::Decompressor::DecompressPrefix(const char* src, size_t size,
                                           char* dst, size_t want,
                                           size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return 0;
  }
  const int n = LZ4_decompress_safe_partial(src, dst, static_cast<int>(size),
                                            static_cast<int>(want),
                                            static_cast<int>(raw_size));
  return n >= static_cast<int>(want) ? static_cast<size_t>(n) : 0;
}

bool Lz4::Decompressor::Load(std::string_view stored, size_t size) {
  return FillDictionary(_window, _dictionary, size, [&](char* buf) {
    return UnpackDictionary(
      stored, buf, size,
      [&](const char* src, size_t n, char* dst, size_t raw_size) {
        return Decompress(src, n, dst, raw_size);
      });
  });
}

bool Lz4::Decompressor::LoadFrame(const char* src, size_t size,
                                  size_t raw_size) {
  return FillDictionary(_window, _dictionary, raw_size, [&](char* buf) {
    return Decompress(src, size, buf, raw_size);
  });
}

std::string_view Lz4::Decompressor::Inflate(const char* src, size_t size,
                                            char* dst, size_t want,
                                            size_t raw_size) {
  SDB_ASSERT(want <= raw_size);
  if (_dictionary == 0) {
    if (want == raw_size) {
      return Decompress(src, size, dst, raw_size)
               ? std::string_view{dst, raw_size}
               : std::string_view{};
    }
    return {dst, DecompressPrefix(src, size, dst, want, raw_size)};
  }
  if (!std::in_range<int>(size) || !std::in_range<int>(raw_size)) {
    return {};
  }
  auto* out = FrameAfterDictionary(_window, _dictionary, raw_size);
  const auto dictionary = Dictionary();
  const int n =
    want == raw_size
      ? LZ4_decompress_safe_usingDict(
          src, out, static_cast<int>(size), static_cast<int>(raw_size),
          dictionary.data(), static_cast<int>(dictionary.size()))
      : LZ4_decompress_safe_partial_usingDict(
          src, out, static_cast<int>(size), static_cast<int>(want),
          static_cast<int>(raw_size), dictionary.data(),
          static_cast<int>(dictionary.size()));
  return n >= static_cast<int>(want)
           ? std::string_view{out, static_cast<size_t>(n)}
           : std::string_view{};
}

}  // namespace irs::codecs
