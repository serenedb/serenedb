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

#define LZ4_STATIC_LINKING_ONLY
#define LZ4_HC_STATIC_LINKING_ONLY
#define ZSTD_STATIC_LINKING_ONLY

#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zxc_buffer.h>
#include <zxc_constants.h>

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

namespace {

std::string_view ZxcDictionary(std::string_view dictionary) noexcept {
  return dictionary.size() > ZXC_DICT_SIZE_MAX
           ? dictionary.substr(dictionary.size() - ZXC_DICT_SIZE_MAX)
           : dictionary;
}

}  // namespace

LeafCompressor<ByteCodec::Zxc>::~LeafCompressor() {
  zxc_free_cctx(_ctx);
  zxc_free_cctx(_block_ctx);
}

void LeafCompressor<ByteCodec::Zxc>::LoadDictionary(std::string_view dictionary,
                                                    size_t /*frame_bytes*/) {
  _dictionary = ZxcDictionary(dictionary);
}

size_t LeafCompressor<ByteCodec::Zxc>::Compress(const char* src, size_t size,
                                                char* dst, size_t capacity) {
  zxc_compress_opts_t opts{};
  opts.level = _level;
  if (_dictionary.empty() || size == 0 || size > ZXC_BLOCK_SIZE_MAX) {
    const auto n = zxc_compress_cctx(_ctx, src, size, dst, capacity, &opts);
    SDB_ENSURE(n > 0, "zxc compression failed: ", n);
    return static_cast<size_t>(n);
  }
  if (!_block_ctx) {
    _block_ctx = zxc_create_cctx(nullptr);
    SDB_ENSURE(_block_ctx, "zxc: cannot create a compression context");
  }
  opts.dict = _dictionary.data();
  opts.dict_size = _dictionary.size();
  const auto n =
    zxc_compress_block(_block_ctx, src, size, dst, capacity, &opts);
  SDB_ENSURE(n > 0, "zxc compression failed: ", n);
  return static_cast<size_t>(n);
}

LeafDecompressor<ByteCodec::Zxc>::LeafDecompressor() : _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

LeafDecompressor<ByteCodec::Zxc>::~LeafDecompressor() {
  zxc_free_dctx(_ctx);
  zxc_free_dctx(_block_ctx);
}

void LeafDecompressor<ByteCodec::Zxc>::SetDictionary(
  std::string_view dictionary) noexcept {
  _dictionary = ZxcDictionary(dictionary);
}

bool LeafDecompressor<ByteCodec::Zxc>::Decompress(const char* src, size_t size,
                                                  char* dst,
                                                  size_t raw_size) noexcept {
  if (_dictionary.empty() || raw_size == 0 || raw_size > ZXC_BLOCK_SIZE_MAX) {
    const auto n = zxc_decompress_dctx(_ctx, src, size, dst, raw_size, nullptr);
    return n >= 0 && static_cast<size_t>(n) == raw_size;
  }
  if (!_block_ctx) {
    _block_ctx = zxc_create_dctx();
    if (!_block_ctx) {
      return false;
    }
  }
  zxc_decompress_opts_t opts{};
  opts.dict = _dictionary.data();
  opts.dict_size = _dictionary.size();
  const auto n =
    zxc_decompress_block_safe(_block_ctx, src, size, dst, raw_size, &opts);
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t LeafDecompressor<ByteCodec::Zxc>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t /*want*/,
  size_t raw_size) noexcept {
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

LeafCompressor<ByteCodec::Lz4>::~LeafCompressor() {
  LZ4_freeStream(_dict);
  LZ4_freeStream(_work);
  LZ4_freeStreamHC(_hc_dict);
  LZ4_freeStreamHC(_hc_work);
}

void LeafCompressor<ByteCodec::Lz4>::LoadDictionary(std::string_view dictionary,
                                                    size_t /*frame_bytes*/) {
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

void LeafCompressor<ByteCodec::Lz4>::LoadTrained(
  const TrainedDictionary& dictionary) {
  if (_trained == dictionary.Id() && _trained_level == _level) {
    _dictionary = true;
    return;
  }
  LoadDictionary(dictionary.Bytes(), 0);
  _trained = dictionary.Id();
  _trained_level = _level;
}

size_t LeafCompressor<ByteCodec::Lz4>::Compress(const char* src, size_t size,
                                                char* dst, size_t capacity) {
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

LeafCompressor<ByteCodec::Zstd>::~LeafCompressor() {
  ClearDictionary();
  ZSTD_freeCDict(_trained_cdict);
}

void LeafCompressor<ByteCodec::Zstd>::ClearDictionary() noexcept {
  ZSTD_freeCDict(_cdict);
  _cdict = nullptr;
  _use_trained = false;
}

void LeafCompressor<ByteCodec::Zstd>::LoadTrained(
  const TrainedDictionary& dictionary) {
  ClearDictionary();
  if (_trained != dictionary.Id() || _trained_level != _level) {
    ZSTD_freeCDict(_trained_cdict);
    _trained_cdict = nullptr;
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
  _use_trained = true;
}

void LeafCompressor<ByteCodec::Zstd>::LoadDictionary(
  std::string_view dictionary, size_t frame_bytes) {
  ClearDictionary();
  _cdict = ZSTD_createCDict_advanced(
    dictionary.data(), dictionary.size(), ZSTD_dlm_byRef, ZSTD_dct_rawContent,
    ZSTD_getCParams(_level, frame_bytes, dictionary.size()), ZSTD_defaultCMem);
  SDB_ENSURE(_cdict, "zstd: cannot create a compression dictionary");
}

size_t LeafCompressor<ByteCodec::Zstd>::Compress(const char* src, size_t size,
                                                 char* dst, size_t capacity) {
  const auto* cdict = _use_trained ? _trained_cdict : _cdict;
  const auto n =
    cdict
      ? ZSTD_compress_usingCDict(_ctx.get(), dst, capacity, src, size, cdict)
      : ZSTD_compressCCtx(_ctx.get(), dst, capacity, src, size, _level);
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
  const int n =
    _dictionary.empty()
      ? LZ4_decompress_safe(src, dst, static_cast<int>(size),
                            static_cast<int>(raw_size))
      : LZ4_decompress_safe_usingDict(
          src, dst, static_cast<int>(size), static_cast<int>(raw_size),
          _dictionary.data(), static_cast<int>(_dictionary.size()));
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
  const int n = _dictionary.empty()
                  ? LZ4_decompress_safe_partial(
                      src, dst, static_cast<int>(size), static_cast<int>(want),
                      static_cast<int>(raw_size))
                  : LZ4_decompress_safe_partial_usingDict(
                      src, dst, static_cast<int>(size), static_cast<int>(want),
                      static_cast<int>(raw_size), _dictionary.data(),
                      static_cast<int>(_dictionary.size()));
  return n >= 0 && static_cast<size_t>(n) >= want ? static_cast<size_t>(n) : 0;
}

LeafDecompressor<ByteCodec::Zstd>::~LeafDecompressor() {
  ZSTD_freeDDict(_ddict);
}

void LeafDecompressor<ByteCodec::Zstd>::SetDictionary(
  std::string_view dictionary) {
  _trained = nullptr;
  _use = !dictionary.empty();
  if (!_use || (dictionary.data() == _loaded.data() &&
                dictionary.size() == _loaded.size())) {
    return;
  }
  ZSTD_freeDDict(_ddict);
  _ddict = nullptr;
  _loaded = {};
  _ddict = ZSTD_createDDict_advanced(dictionary.data(), dictionary.size(),
                                     ZSTD_dlm_byRef, ZSTD_dct_rawContent,
                                     ZSTD_defaultCMem);
  SDB_ENSURE(_ddict, "zstd: cannot create a decompression dictionary");
  _loaded = dictionary;
}

bool LeafDecompressor<ByteCodec::Zstd>::Decompress(const char* src, size_t size,
                                                   char* dst,
                                                   size_t raw_size) noexcept {
  const auto* ddict = _trained ? _trained : _use ? _ddict : nullptr;
  const auto n =
    ddict
      ? ZSTD_decompress_usingDDict(_ctx.get(), dst, raw_size, src, size, ddict)
      : ZSTD_decompressDCtx(_ctx.get(), dst, raw_size, src, size);
  return !ZSTD_isError(n) && n == raw_size;
}

size_t LeafDecompressor<ByteCodec::Zstd>::DecompressPrefix(
  const char* src, size_t size, char* dst, size_t want,
  size_t raw_size) noexcept {
  SDB_ASSERT(want <= raw_size);
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

}  // namespace irs::codecs
