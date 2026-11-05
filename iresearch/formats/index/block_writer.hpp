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

#pragma once

#include <fsst.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/fsst.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

using FsstEncoder = std::unique_ptr<duckdb_fsst_encoder_t, codecs::FsstFree>;

struct VarColumns {
  std::vector<uint32_t> lcps;
  std::vector<uint32_t> lens;
  bstring suffixes;
  std::vector<uint16_t> restarts;
  uint32_t max = 0;
};

struct BlockBuilder {
  bstring keys;
  std::vector<uint32_t> ends;
  bstring records;
  std::vector<uint16_t> record_starts;
  VarColumns raw;
  VarColumns coded;
  uint64_t size = 0;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  uint32_t width = 0;
  uint32_t prefix = 0;
  uint32_t head_skip = 0;
  BlockKind kind = BlockKind::Var;
  bool all_coded = true;
};

class BlockWriter final {
 public:
  BlockWriter() noexcept = default;
  explicit BlockWriter(uint64_t inline_base) noexcept;

  BlockWriter(const BlockWriter&) = delete;
  BlockWriter& operator=(const BlockWriter&) = delete;

  void Open(IndexOutput& out) noexcept;
  void SetFsst(duckdb_fsst_encoder_t* fsst);
  void Recode();
  void BeginField(uint32_t width, bool encode, IndexFeatures features) noexcept;
  void SetKeyPrefix(bytes_view prefix) { _key_prefix.assign(prefix); }
  void Add(bytes_view term, const PostingMeta& meta);
  void Cut();
  void Splice(BlockWriter& part, bytes_view first);
  void Finish(TermsMeta& terms);
  void Reset() noexcept;

  uint32_t Blocks() const noexcept { return _blocks; }
  uint32_t OpenEntries() const noexcept {
    return static_cast<uint32_t>(_block.ends.size());
  }
  uint32_t LastRecordStart() const noexcept {
    return static_cast<uint32_t>(_block.records.size() - _record.View().size());
  }
  bytes_view LastKey() const noexcept;
  uint64_t InlineEnd() const noexcept { return _inline_base + _inline.size(); }
  uint64_t Position() const noexcept { return _position; }

 private:
  bytes_view EncodeSuffix(bytes_view suffix);
  bool Append(bytes_view term, const PostingMeta& meta);
  bool AppendFixedKey(bytes_view key, uint64_t records);
  bool AppendVarKey(bytes_view key, uint64_t records);
  void AppendVarColumns(uint32_t index, bytes_view key);
  void Seal();
  void WriteSeparator(bytes_view first);
  void EncodeSeparator(bytes_view separator);
  void AppendSeparators(std::string_view separators);
  void PadToPage();
  void WriteBlock();

  RecordOutput _record;
  RecordEncoder _encoder;
  BlockBuilder _block;
  FsstEncoder _fsst;
  bstring _coded;
  bstring _key_prefix;
  bstring _inline;
  std::string _inline_starts;
  std::vector<SpanningBlock> _spans;
  std::string _separators;
  bstring _separator;
  bstring _last;
  bstring _pages;
  BytesOutput _pages_out{_pages};
  DataOutput* _out = nullptr;
  uint64_t _position = 0;
  uint64_t _first_page = 0;
  uint64_t _inline_base = 0;
  uint64_t _inline_start = 0;
  uint64_t _inline_last = 0;
  uint32_t _blocks = 0;
  uint32_t _width = 0;
  bool _encode = true;
  bool _group_inline = false;
};

}  // namespace irs::term_dict
