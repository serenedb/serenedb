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

#include "iresearch/formats/column/codecs/bytes/zxc.hpp"

#include <zxc_buffer.h>
#include <zxc_constants.h>
#include <zxc_error.h>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

std::string_view ZxcDictionary(std::string_view dictionary) noexcept {
  return dictionary.size() > ZXC_DICT_SIZE_MAX
           ? dictionary.substr(dictionary.size() - ZXC_DICT_SIZE_MAX)
           : dictionary;
}

size_t DecoderBytes() noexcept {
  return zxc_static_dctx_workspace_size(ZXC_BLOCK_SIZE_DEFAULT);
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

size_t Zxc::ContextBytes(uint8_t level) noexcept {
  return zxc_estimate_cctx_size(ZXC_BLOCK_SIZE_DEFAULT,
                                EffectiveLevel<Zxc>(level));
}

Zxc::Compressor::Compressor(LocalBudget& budget, uint8_t level)
  : DictionaryCompressor{budget, kDictionaryCap},
    _level{level},
    _ctx_charge{budget, ContextBytes(level)},
    _ctx{NewContext(level)},
    _attached_charge{budget, 0} {}

void Zxc::Compressor::Attach() {
  const auto bytes = ZxcDictionary(Dictionary());
  _attached_charge.Resize(ContextBytes(_level));
  _attached = NewContext(_level);
  const auto rc =
    zxc_cctx_set_dict(_attached.get(), bytes.data(), bytes.size(), nullptr);
  SDB_ENSURE(rc == ZXC_OK, "zxc: cannot load a dictionary: ", rc);
}

size_t Zxc::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
  if (Dictionary().empty() || size == 0 || size > ZXC_BLOCK_SIZE_MAX) {
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

Zxc::Decompressor::Decompressor()
  : _ctx_charge{DecoderBytes()}, _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

bool Zxc::Decompressor::Decompress(const char* src, size_t size, char* dst,
                                   size_t raw_size) noexcept {
  const auto n = zxc_decompress_dctx(_ctx.get(), src, size, dst, raw_size);
  return n >= 0 && static_cast<size_t>(n) == raw_size;
}

std::string_view Zxc::Decompressor::Inflate(const char* src, size_t size,
                                            char* dst, size_t,
                                            size_t raw_size) {
  if (!HasDictionary() || raw_size == 0 || raw_size > ZXC_BLOCK_SIZE_MAX) {
    return Decompress(src, size, dst, raw_size)
             ? std::string_view{dst, raw_size}
             : std::string_view{};
  }
  if (!_blocks) {
    _blocks_charge.Resize(DecoderBytes());
    _blocks.reset(zxc_create_dctx());
    SDB_ENSURE(_blocks, "zxc: cannot create a decompression context");
  }
  const auto capacity = zxc_decompress_block_bound(raw_size);
  auto* out = FrameAfterDictionary(capacity);
  const auto dictionary = ZxcDictionary(Dictionary());
  const auto referenced =
    zxc_dctx_ref_dict(_blocks.get(), dictionary.data(), dictionary.size());
  SDB_ASSERT(referenced == ZXC_OK);
  const auto n = zxc_decompress_block(_blocks.get(), src, size, out, capacity);
  return n >= 0 && static_cast<size_t>(n) == raw_size
           ? std::string_view{out, raw_size}
           : std::string_view{};
}

}  // namespace irs::codecs
