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

#include "iresearch/formats/column/codecs/compression.hpp"
#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/fsst_leaf.tpp"

/*
Data layout per segment, one block of one column; a text column's row group
is cut into segments of about 256 KiB:
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
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table, at most 2066 bytes      |   |
|   +----------------------------------------------+   |
|   trained once per row group; each of the row        |
|   group's FSST segments stores a copy                |
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

const duckdb::CompressionFunction* DictFsstFunction(
  duckdb::PhysicalType physical) {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::FsstLeaf>>(
      duckdb::CompressionType::COMPRESSION_COL_DICT_FSST);
  return physical == duckdb::PhysicalType::VARCHAR ? &kFunction : nullptr;
}

}  // namespace irs::codecs
