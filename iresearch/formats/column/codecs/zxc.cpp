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
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
zxc frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary.
*/

namespace irs::codecs {
namespace {

std::string_view ZxcDictionary(std::string_view dictionary) noexcept {
  return dictionary.size() > ZXC_DICT_SIZE_MAX
           ? dictionary.substr(dictionary.size() - ZXC_DICT_SIZE_MAX)
           : dictionary;
}

std::unique_ptr<zxc_cctx, ZxcFree> NewContext(uint8_t level) {
  level = EffectiveLevel<Zxc>(level);
  std::unique_ptr<zxc_cctx, ZxcFree> ctx{zxc_create_cctx()};
  SDB_ENSURE(ctx, "zxc: cannot create a compression context");
  const auto rc = zxc_cctx_set_param(ctx.get(), ZXC_C_LEVEL, level);
  SDB_ENSURE(rc == ZXC_OK, "zxc: cannot set level ", level, ": ", rc);
  return ctx;
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

Zxc::Compressor::Compressor(uint8_t level)
  : _level{level}, _ctx{NewContext(level)} {}

bool Zxc::Compressor::Train(std::string_view frames,
                            std::span<const size_t> sizes) {
  TrainDictionary(frames, sizes, kDictionaryCap, _dictionary);
  return Pack();
}

bool Zxc::Compressor::Pack() {
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

void Zxc::Compressor::Use(std::string_view raw) {
  _dictionary.assign(raw);
  _stored.clear();
  Attach();
}

void Zxc::Compressor::Attach() {
  const auto bytes = ZxcDictionary(_dictionary);
  _attached = NewContext(_level);
  const auto rc =
    zxc_cctx_set_dict(_attached.get(), bytes.data(), bytes.size(), nullptr);
  SDB_ENSURE(rc == ZXC_OK, "zxc: cannot load a dictionary: ", rc);
}

size_t Zxc::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  if (_dictionary.empty() || size == 0 || size > ZXC_BLOCK_SIZE_MAX) {
    return Plain(src, size, dst, capacity);
  }
  const auto n = zxc_compress_block(_attached.get(), src, size, dst, capacity);
  SDB_ENSURE(n > 0, "zxc compression failed: ", n);
  return static_cast<size_t>(n);
}

size_t Zxc::Compressor::Plain(const char* src, size_t size, char* dst,
                              size_t capacity) {
  const auto n = zxc_compress_cctx(_ctx.get(), src, size, dst, capacity);
  SDB_ENSURE(n > 0, "zxc compression failed: ", n);
  return static_cast<size_t>(n);
}

Zxc::Decompressor::Decompressor() : _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

bool Zxc::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  const auto n = zxc_decompress_dctx(_ctx.get(), src, size, dst, raw_size);
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

bool Zxc::Decompressor::Load(std::string_view stored, size_t size) {
  return FillDictionary(_window, _dictionary, size, [&](char* buf) {
    return UnpackDictionary(
      stored, buf, size,
      [&](const char* src, size_t n, char* dst, size_t raw_size) {
        return Decompress(src, n, dst, raw_size);
      });
  });
}

bool Zxc::Decompressor::LoadFrame(const char* src, size_t size,
                                  size_t raw_size) {
  return FillDictionary(_window, _dictionary, raw_size, [&](char* buf) {
    return Decompress(src, size, buf, raw_size);
  });
}

std::string_view Zxc::Decompressor::Inflate(const char* src, size_t size,
                                            char* dst, size_t,
                                            size_t raw_size) {
  if (_dictionary == 0 || raw_size == 0 || raw_size > ZXC_BLOCK_SIZE_MAX) {
    return Decompress(src, size, dst, raw_size)
             ? std::string_view{dst, raw_size}
             : std::string_view{};
  }
  if (!_blocks) {
    _blocks.reset(zxc_create_dctx());
    if (!_blocks) {
      return {};
    }
  }
  const auto capacity = zxc_decompress_block_bound(raw_size);
  auto* out = FrameAfterDictionary(_window, _dictionary, capacity);
  const auto dictionary = ZxcDictionary(Dictionary());
  if (zxc_dctx_ref_dict(_blocks.get(), dictionary.data(), dictionary.size()) !=
      ZXC_OK) {
    return {};
  }
  const auto n = zxc_decompress_block(_blocks.get(), src, size, out, capacity);
  return n >= 0 && static_cast<size_t>(n) == raw_size
           ? std::string_view{out, raw_size}
           : std::string_view{};
}

}  // namespace irs::codecs
