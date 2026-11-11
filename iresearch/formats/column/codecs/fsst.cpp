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

#include "iresearch/formats/column/codecs/fsst.hpp"

#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/fsst_leaf.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
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
|   section offsets, flags                             |
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
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each row's FSST-encoded suffix,          |
|   0 for a NULL row                                   |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table, at most 2066 bytes      |   |
|   +----------------------------------------------+   |
|   trained per row group and again when its data      |
|   drifts; each FSST segment stores a copy            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries          |
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
|   section offsets, flags                             |
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
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each entry's FSST-encoded suffix;        |
|   entry 0 is NULL, then the distinct values in       |
|   sorted order                                       |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                      Row codes                       |
|   +----------------------------------------------+   |
|   |   numeric codec segment of code[row_count]   |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table, at most 2066 bytes      |   |
|   +----------------------------------------------+   |
|   trained per row group and again when its data      |
|   drifts; each FSST segment stores a copy            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries          |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

namespace irs::codecs {

constinit const CompressionFunction kFsstFunction =
  text::StringFunction<text::PlainScan<text::FsstLeaf>>(
    duckdb::CompressionType::COMPRESSION_COL_FSST);

constinit const CompressionFunction kDictFsstFunction =
  text::StringFunction<text::DictionaryScan<text::FsstLeaf>>(
    duckdb::CompressionType::COMPRESSION_COL_DICT_FSST);

FsstEncoded FsstEncoder::Encode(std::span<const duckdb::string_t> strings,
                                std::span<const uint32_t> skip, char* out,
                                size_t capacity, std::span<uint32_t> lengths) {
  SDB_ASSERT(skip.size() == strings.size());
  SDB_ASSERT(lengths.size() >= strings.size());
  const size_t n = strings.size();
  if (n == 0) {
    return {};
  }
  _in_lengths.resize(n);
  _in_ptrs.resize(n);
  for (size_t i = 0; i < n; ++i) {
    _in_lengths[i] = strings[i].GetSize() - skip[i];
    _in_ptrs[i] = reinterpret_cast<unsigned char*>(
      const_cast<char*>(strings[i].GetData() + skip[i]));
  }
  if (!_encoder) {
    _encoder.reset(
      duckdb_fsst_create(n, _in_lengths.data(), _in_ptrs.data(), 0));
    SDB_ENSURE(_encoder, "fsst: cannot build a symbol table");
    _table_size = duckdb_fsst_export(_encoder.get(), _table);
  }
  _out_lengths.resize(n);
  _out_ptrs.resize(n);
  const size_t done =
    duckdb_fsst_compress(_encoder.get(), n, _in_lengths.data(), _in_ptrs.data(),
                         capacity, reinterpret_cast<unsigned char*>(out),
                         _out_lengths.data(), _out_ptrs.data());
  size_t used = 0;
  for (size_t i = 0; i < done; ++i) {
    lengths[i] = static_cast<uint32_t>(_out_lengths[i]);
    used += _out_lengths[i];
  }
  return {.strings = done, .bytes = used};
}

bool FsstDecoder::Import(std::string_view table) noexcept {
  return duckdb_fsst_import(
           &_decoder, reinterpret_cast<const unsigned char*>(table.data()),
           table.size()) == table.size();
}

}  // namespace irs::codecs
