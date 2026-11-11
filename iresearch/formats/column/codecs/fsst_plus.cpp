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

#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/fsst_plus_leaf.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"

/*
FSST+ segments (fsst_plus, dict_fsst_plus) keep the layout of the fsst
segments (fsst.cpp), with entries grouped instead of front-coded: a group of
neighbouring entries stores the FSST codes they share once, and each entry
stores the rest of its codes.
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each entry's codes after its group's     |
|   shared codes                                       |
+------------------------------------------------------+
|                    Entry groups                      |
|   +----------------------------------------------+   |
|   |   bitpacked group[entry_count]               |   |
|   +----------------------------------------------+   |
|   1-based group of each entry, 0 for none            |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table, dictionary_size bytes   |   |
|   |   uint32 group_count                         |   |
|   |   uint32 shared_end[group_count]             |   |
|   |   shared codes of every group                |   |
|   +----------------------------------------------+   |
|   group g's shared codes end at shared_end[g - 1]    |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   each entry's codes after its group's       |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries          |
+------------------------------------------------------+
*/

namespace irs::codecs {

constinit const CompressionFunction kFsstPlusFunction =
  text::StringFunction<text::PlainScan<text::FsstPlusLeaf>>(
    duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS);

constinit const CompressionFunction kDictFsstPlusFunction =
  text::StringFunction<text::DictionaryScan<text::FsstPlusLeaf>>(
    duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS);

}  // namespace irs::codecs
