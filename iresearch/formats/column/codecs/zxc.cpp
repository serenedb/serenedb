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

#include "iresearch/formats/column/codecs/zxc.hpp"

#include <zxc_buffer.h>
#include <zxc_constants.h>
#include <zxc_error.h>

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
zxc frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or
64 KiB frames without a dictionary. zxc takes no trained dictionary.
*/

namespace irs::codecs {
namespace {

std::string_view ZxcDictionary(std::string_view dictionary) noexcept {
  return dictionary.size() > ZXC_DICT_SIZE_MAX
           ? dictionary.substr(dictionary.size() - ZXC_DICT_SIZE_MAX)
           : dictionary;
}

}  // namespace

const duckdb::CompressionFunction& ZxcFunction() {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::BlobLeaf<Zxc>>>(
      duckdb::CompressionType::COMPRESSION_ZXC);
  return kFunction;
}

const duckdb::CompressionFunction& DictZxcFunction() {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::BlobLeaf<Zxc>>>(
      duckdb::CompressionType::COMPRESSION_DICT_ZXC);
  return kFunction;
}

size_t Zxc::Bound(size_t raw_size) noexcept {
  return static_cast<size_t>(zxc_compress_bound(raw_size));
}

Zxc::Compressor::Compressor() : _ctx{zxc_create_cctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a compression context");
}

void Zxc::Compressor::LoadDictionary(std::string_view dictionary) {
  dictionary = ZxcDictionary(dictionary);
  _dictionary = !dictionary.empty();
  if (!_dictionary) {
    return;
  }
  if (!_block_ctx) {
    _block_ctx.reset(zxc_create_cctx());
    SDB_ENSURE(_block_ctx, "zxc: cannot create a compression context");
  }
  const auto rc = zxc_cctx_set_dict(_block_ctx.get(), dictionary.data(),
                                    dictionary.size(), nullptr);
  SDB_ENSURE(rc == ZXC_OK, "zxc: cannot load a dictionary: ", rc);
}

size_t Zxc::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  const bool block = _dictionary && size != 0 && size <= ZXC_BLOCK_SIZE_MAX;
  auto* ctx = block ? _block_ctx.get() : _ctx.get();
  const auto rc = zxc_cctx_set_param(ctx, ZXC_C_LEVEL, _level);
  SDB_ENSURE(rc == ZXC_OK, "zxc: cannot set level ", _level, ": ", rc);
  const auto n = block ? zxc_compress_block(ctx, src, size, dst, capacity)
                       : zxc_compress_cctx(ctx, src, size, dst, capacity);
  SDB_ENSURE(n > 0, "zxc compression failed: ", n);
  return static_cast<size_t>(n);
}

Zxc::Decompressor::Decompressor() : _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

void Zxc::Decompressor::SetDictionary(std::string_view dictionary) noexcept {
  _dictionary = ZxcDictionary(dictionary);
}

bool Zxc::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  if (_dictionary.empty() || raw_size == 0 || raw_size > ZXC_BLOCK_SIZE_MAX) {
    const auto n = zxc_decompress_dctx(_ctx.get(), src, size, dst, raw_size);
    return n >= 0 && static_cast<size_t>(n) == raw_size;
  }
  if (!_block_ctx) {
    _block_ctx.reset(zxc_create_dctx());
    if (!_block_ctx) {
      return false;
    }
  }
  if (zxc_dctx_ref_dict(_block_ctx.get(), _dictionary.data(),
                        _dictionary.size()) != ZXC_OK) {
    return false;
  }
  const auto n =
    zxc_decompress_block_safe(_block_ctx.get(), src, size, dst, raw_size);
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

size_t Zxc::Decompressor::DecompressPrefix(const char* src, size_t size,
                                           char* dst, size_t,
                                           size_t raw_size) noexcept {
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

}  // namespace irs::codecs
