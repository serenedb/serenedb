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

#include "iresearch/formats/column/codecs/blob_leaf.tpp"
#include "iresearch/formats/column/codecs/compression.hpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
Data layout per PlainScan segment, one block of one column; a text column's
row group is cut into segments of about kDefaultColSegmentTarget encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags, trained dictionary id      |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked length[entry_count]              |   |
|   +----------------------------------------------+   |
|   one entry per row, empty for a NULL row            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   zxc frames of the concatenated entries     |   |
|   +----------------------------------------------+   |
|   frame 0 holds up to 32 KiB and is the              |
|   dictionary of the later frames of up to 16 KiB;    |
|   or 64 KiB frames without a dictionary              |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

/*
Data layout per DictionaryScan segment, one block of one column; a text
column's row group is cut into segments of about kDefaultColSegmentTarget
encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags, trained dictionary id      |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked length[entry_count]              |   |
|   +----------------------------------------------+   |
|   entry 0 is NULL, then each distinct value in       |
|   the order the rows first use it                    |
+------------------------------------------------------+
|                      Row codes                       |
|   +----------------------------------------------+   |
|   |   bitpacked code[row_count]                  |   |
|   |   or bitpacked run value[run_count]          |   |
|   |   or a numeric codec segment                 |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                       Run ends                       |
|   +----------------------------------------------+   |
|   |   bitpacked run_end[run_count]               |   |
|   +----------------------------------------------+   |
|   run-length row codes only                          |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   zxc frames of the concatenated entries     |   |
|   +----------------------------------------------+   |
|   frame 0 holds up to 32 KiB and is the              |
|   dictionary of the later frames of up to 16 KiB;    |
|   or 64 KiB frames without a dictionary              |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

namespace irs::codecs {
namespace {

std::string_view ZxcDictionary(std::string_view dictionary) noexcept {
  return dictionary.size() > ZXC_DICT_SIZE_MAX
           ? dictionary.substr(dictionary.size() - ZXC_DICT_SIZE_MAX)
           : dictionary;
}

}  // namespace

const duckdb::CompressionFunction* ZxcFunction(duckdb::PhysicalType physical) {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::BlobLeaf<Zxc>>>(
      duckdb::CompressionType::COMPRESSION_ZXC);
  return physical == duckdb::PhysicalType::VARCHAR ? &kFunction : nullptr;
}

const duckdb::CompressionFunction* DictZxcFunction(
  duckdb::PhysicalType physical) {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::BlobLeaf<Zxc>>>(
      duckdb::CompressionType::COMPRESSION_DICT_ZXC);
  return physical == duckdb::PhysicalType::VARCHAR ? &kFunction : nullptr;
}

size_t Zxc::Bound(size_t raw_size) noexcept {
  return static_cast<size_t>(zxc_compress_bound(raw_size));
}

Zxc::Compressor::Compressor() : _ctx{zxc_create_cctx(nullptr)} {
  SDB_ENSURE(_ctx, "zxc: cannot create a compression context");
}

Zxc::Compressor::~Compressor() {
  zxc_free_cctx(_ctx);
  zxc_free_cctx(_block_ctx);
}

void Zxc::Compressor::LoadDictionary(std::string_view dictionary) {
  _dictionary = ZxcDictionary(dictionary);
}

size_t Zxc::Compressor::Compress(const char* src, size_t size, char* dst,
                                 size_t capacity) {
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

Zxc::Decompressor::Decompressor() : _ctx{zxc_create_dctx()} {
  SDB_ENSURE(_ctx, "zxc: cannot create a decompression context");
}

Zxc::Decompressor::~Decompressor() {
  zxc_free_dctx(_ctx);
  zxc_free_dctx(_block_ctx);
}

void Zxc::Decompressor::SetDictionary(std::string_view dictionary) noexcept {
  _dictionary = ZxcDictionary(dictionary);
}

bool Zxc::Decompressor::Decompress(const char* src, size_t size, char* dst,
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

size_t Zxc::Decompressor::DecompressPrefix(const char* src, size_t size,
                                           char* dst, size_t,
                                           size_t raw_size) noexcept {
  return Decompress(src, size, dst, raw_size) ? raw_size : 0;
}

}  // namespace irs::codecs
