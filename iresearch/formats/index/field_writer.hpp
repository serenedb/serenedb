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
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/block_writer.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_ranges.hpp"
#include "iresearch/formats/index/term_parts.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs::term_dict {

struct PartsField {
  field_id id = field_limits::invalid();
  FieldProperties props;
  std::span<const TermPart> parts;
  std::vector<uint64_t> inline_bases;
  bstring key_prefix;
  duckdb_fsst_encoder_t* fsst = nullptr;
  uint64_t total_freq = 0;
  uint64_t first_inline = 0;
  uint64_t docs = 0;
  uint32_t width = 0;
  bool encode = true;
};

struct PartBlocks {
  PartBlocks(const PartsField& field, size_t part);

  BlockWriter blocks;
  std::vector<uint32_t> prefixes;
  bstring min;
  uint64_t terms = 0;
};

class FieldWriter final {
 public:
  explicit FieldWriter(bool compaction) noexcept : _compaction{compaction} {}

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void prepare(const FlushState& state, uint64_t keyed_dicts,
               PostingsOutputs postings = {});
  void write(DictRef dict, const BasicTermReader& reader);
  PartsField BeginParts(DictRef dict, const BasicTermReader& reader,
                        std::span<const TermPart> parts, uint64_t docs);
  void AppendPart(PartBlocks& built);
  void EndParts(const PartsField& field);
  void WriteSlot(field_id id, DictRef dict, std::span<const doc_id_t> docs);
  void WriteSlot(field_id id, DictRef dict, TermPostings& docs);
  void end();

 private:
  void Begin(field_id id, DictRef dict) noexcept;
  void BeginTerms(const BasicTermReader& reader, const FieldProperties& props);
  void BeginSlot(field_id id, DictRef dict);
  void EndSlot(field_id id, const PostingMeta& meta);
  void SampleFsst(const BasicTermReader& reader);
  void TrainFsst();
  void Add(bytes_view term, const PostingMeta& meta);
  void AssignOrdinal();
  void Commit(bytes_view term, const PostingMeta& meta);
  void EndField(field_id id, const FieldProperties& props, uint64_t total_freq,
                uint64_t first_inline, const PostingsWriter::FieldStats& stats);
  void ResetField() noexcept;
  void Reset() noexcept;

  PostingsWriter _pw;
  BlockWriter _blocks;
  RangeWriter _ranges;
  RecordOutput _record;
  FsstEncoder _fsst;
  std::string _fsst_table;
  bstring _samples;
  std::vector<size_t> _sample_lengths;
  bstring _range_regions;
  std::vector<FieldHead> _heads;
  std::vector<uint32_t> _prefixes;
  bstring _key_prefix;
  bstring _min;
  PostingMeta _single;
  std::array<byte_type, PostingMeta::kInlineBytes> _single_inline{};
  IdxWriter* _idx = nullptr;
  uint64_t _docs = 0;
  uint64_t _terms = 0;
  uint32_t _width = 0;
  uint32_t _ordinal = 0;
  uint32_t _ordinal_bytes = 1;
  field_id _last_id = field_limits::invalid();
  DictRef _dict;
  uint32_t _first_block = 0;
  uint32_t _first_entry = 0;
  uint32_t _first_record = 0;
  bool _encode = true;
  const bool _compaction;
};

}  // namespace irs::term_dict
