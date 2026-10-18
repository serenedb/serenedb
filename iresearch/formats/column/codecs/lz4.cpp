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

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/compression.hpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

static_assert(Lz4::kMaxLevel == LZ4HC_CLEVEL_MAX);

const duckdb::CompressionFunction* Lz4Function(duckdb::PhysicalType physical) {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::BlobLeaf<Lz4>>>(
      duckdb::CompressionType::COMPRESSION_LZ4);
  return physical == duckdb::PhysicalType::VARCHAR ? &kFunction : nullptr;
}

size_t Lz4::Bound(size_t raw_size) noexcept {
  SDB_ASSERT(raw_size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw_size)));
}

Lz4::Compressor::~Compressor() {
  LZ4_freeStream(_dict);
  LZ4_freeStream(_work);
  LZ4_freeStreamHC(_hc_dict);
  LZ4_freeStreamHC(_hc_work);
}

void Lz4::Compressor::LoadDictionary(std::string_view dictionary) {
  SDB_ASSERT(dictionary.size() <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  _trained = 0;
  const auto size = static_cast<int>(dictionary.size());
  if (_level <= 1) {
    if (!_dict) {
      _dict = LZ4_createStream();
      SDB_ENSURE(_dict, "lz4: cannot create a dictionary stream");
    }
    LZ4_loadDict(_dict, dictionary.data(), size);
  } else {
    if (!_hc_dict) {
      _hc_dict = LZ4_createStreamHC();
      SDB_ENSURE(_hc_dict, "lz4: cannot create a dictionary stream");
    }
    LZ4_resetStreamHC_fast(_hc_dict, _level);
    LZ4_loadDictHC(_hc_dict, dictionary.data(), size);
  }
  _dictionary = true;
}

void Lz4::Compressor::LoadTrained(const TrainedDictionary& dictionary) {
  if (_trained == dictionary.Id() && _trained_level == _level) {
    _dictionary = true;
    return;
  }
  LoadDictionary(dictionary.Bytes());
  _trained = dictionary.Id();
  _trained_level = _level;
}

size_t Lz4::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  SDB_ASSERT(size <= static_cast<size_t>(LZ4_MAX_INPUT_SIZE));
  SDB_ASSERT(capacity <= std::numeric_limits<int>::max());
  const auto src_size = static_cast<int>(size);
  const auto dst_capacity = static_cast<int>(capacity);
  int n = 0;
  if (_level <= 1) {
    if (!_work) {
      _work = LZ4_createStream();
      SDB_ENSURE(_work, "lz4: cannot create a compression stream");
    }
    if (_dictionary) {
      LZ4_resetStream_fast(_work);
      LZ4_attach_dictionary(_work, _dict);
      n =
        LZ4_compress_fast_continue(_work, src, dst, src_size, dst_capacity, 1);
    } else {
      n = LZ4_compress_fast_extState_fastReset(_work, src, dst, src_size,
                                               dst_capacity, 1);
    }
  } else {
    if (!_hc_work) {
      _hc_work = LZ4_createStreamHC();
      SDB_ENSURE(_hc_work, "lz4: cannot create a compression stream");
    }
    if (_dictionary) {
      LZ4_resetStreamHC_fast(_hc_work, _level);
      LZ4_attach_HC_dictionary(_hc_work, _hc_dict);
      n = LZ4_compress_HC_continue(_hc_work, src, dst, src_size, dst_capacity);
    } else {
      n = LZ4_compress_HC_extStateHC_fastReset(_hc_work, src, dst, src_size,
                                               dst_capacity, _level);
    }
  }
  SDB_ENSURE(n > 0, "lz4 compression failed");
  return static_cast<size_t>(n);
}

bool Lz4::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  const int n = LZ4_decompress_safe_usingDict(
    src, dst, static_cast<int>(size), static_cast<int>(raw_size),
    _dictionary.data(), static_cast<int>(_dictionary.size()));
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t Lz4::Decompressor::DecompressPrefix(const char* src, size_t size,
                                           char* dst, size_t want,
                                           size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      raw_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return 0;
  }
  const int n = LZ4_decompress_safe_partial_usingDict(
    src, dst, static_cast<int>(size), static_cast<int>(want),
    static_cast<int>(raw_size), _dictionary.data(),
    static_cast<int>(_dictionary.size()));
  return n >= 0 && static_cast<size_t>(n) >= want ? static_cast<size_t>(n) : 0;
}

}  // namespace irs::codecs
