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

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/field_sink.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict_block_writer.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_index.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_ranges.hpp"
#include "iresearch/formats/index/term_dict_records.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs::term_dict {

struct BlockRecords {
  RecordRun run;
  bstring bytes;
  uint64_t inline_bytes = 0;
};

struct BlockShare {
  uint64_t bytes = 0;
  uint64_t inline_bytes = 0;
};

class FieldWriter final : public FieldSink {
 public:
  explicit FieldWriter(bool compaction) noexcept : _compaction{compaction} {}

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void prepare(const FlushState& state, uint64_t keyed_dicts) final;
  void write(DictRef dict, const BasicTermReader& reader) final;
  void WriteSlot(field_id id, DictRef dict,
                 std::span<const doc_id_t> docs) final;
  void WriteSlot(field_id id, DictRef dict, TermPostings& docs) final;
  void end() final;

  PostingsInputs TakePostings() noexcept { return _pw.TakeInputs(); }

 private:
  void Begin(field_id id, DictRef dict) noexcept;
  void BeginSlot(field_id id, DictRef dict);
  void EndSlot(field_id id, const PostingMeta& meta);
  bool OwnsBlock() const noexcept;
  void Add(bytes_view term, const PostingMeta& meta);
  void Commit(bytes_view term, const PostingMeta& meta);
  void FeedIndex();
  bool Append(bytes_view term, const PostingMeta& meta);
  void CloseRun();
  void Cut();
  void WriteBlock(uint64_t size);
  void EndField(field_id id, const FieldProperties& props, uint64_t total_freq,
                uint64_t docs_sum);
  void EndKeyed(FieldHead& head);
  void WriteLevels(FieldHead& head);
  void ResetField() noexcept;
  void Reset() noexcept;
  void StartBlocks() noexcept;

  PostingsWriter _pw;
  BlockEncoder _encoder;
  BlockRecords _records;
  BlockShare _share;
  RangeWriter _ranges;
  std::vector<SpanningBlock> _spans;
  std::vector<FieldHead> _heads;
  DictIndexBuilder _index;
  bstring _key_prefix;
  bstring _min;
  PostingMeta _single;
  std::array<byte_type, PostingMeta::kInlineBytes> _single_inline{};
  IdxWriter* _idx = nullptr;
  IndexOutput* _out = nullptr;
  uint64_t _docs = 0;
  uint64_t _terms = 0;
  uint64_t _first_page = 0;
  uint32_t _blocks = 0;
  uint32_t _width = 0;
  uint32_t _ordinal = 0;
  uint32_t _ordinal_bytes = 1;
  field_id _last_id = field_limits::invalid();
  DictRef _dict;
  uint32_t _first_block = 0;
  uint32_t _first_entry = 0;
  uint32_t _first_record = 0;
  const bool _compaction;
};

}  // namespace irs::term_dict
