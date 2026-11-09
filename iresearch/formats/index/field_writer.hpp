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

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_ranges.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs::term_dict {

struct FsstEncoderDelete {
  void operator()(duckdb_fsst_encoder_t* encoder) const noexcept;
};

using FsstEncoder = std::unique_ptr<duckdb_fsst_encoder_t, FsstEncoderDelete>;

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
  std::vector<uint32_t> lcps;
  VarColumns columns;
  uint64_t size = 0;
  uint64_t deferred = 0;
  uint32_t deferred_max = 0;
  uint32_t coded = 0;
  uint32_t width = 0;
  uint32_t prefix = 0;
  uint32_t head_skip = 0;
  BlockKind kind = BlockKind::Var;
};

class FieldWriter final {
 public:
  explicit FieldWriter(bool compaction) noexcept : _compaction{compaction} {}

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void prepare(const FlushState& state, uint64_t keyed_dicts);
  void write(DictRef dict, const BasicTermReader& reader);
  void WriteSlot(field_id id, DictRef dict, std::span<const doc_id_t> docs);
  void WriteSlot(field_id id, DictRef dict, TermPostings& docs);
  void end();

 private:
  void Begin(field_id id, DictRef dict) noexcept;
  void BeginSlot(field_id id, DictRef dict);
  void EndSlot(field_id id, const PostingMeta& meta);
  duckdb_fsst_encoder_t* Encoder();
  void TrainFsst();
  bytes_view EncodeSuffix(bytes_view suffix);
  void EncodeEntries(VarColumns& columns, uint32_t from, uint32_t to,
                     bytes_view last, size_t last_lcp);
  void EncodeDeferred();
  bool OwnsBlock() const noexcept;
  void Add(bytes_view term, const PostingMeta& meta);
  void Commit(bytes_view term, const PostingMeta& meta);
  bool Append(bytes_view term, const PostingMeta& meta);
  bool AppendFixedKey(bytes_view key, uint64_t records);
  bool AppendVarKey(bytes_view key, uint64_t records);
  bool EncodeBlock(bytes_view key, size_t lcp, uint64_t records);
  bool AppendCodedKey(bytes_view key, size_t lcp, uint64_t records);
  void Seal();
  void Cut();
  bool Leads() const noexcept;
  void Lead(bytes_view upper);
  void WriteBlock();
  void EndField(field_id id, const FieldProperties& props, uint64_t total_freq,
                uint64_t docs_sum, uint64_t first_inline);
  void ResetField() noexcept;
  void Reset() noexcept;

  PostingsWriter _pw;
  RecordOutput _record;
  BlockBuilder _block;
  RangeWriter _ranges;
  bstring _inline;
  FsstEncoder _fsst;
  std::string _fsst_table;
  const BasicTermReader* _reader = nullptr;
  bstring _samples;
  std::vector<size_t> _sample_lengths;
  bstring _coded;
  VarColumns _encoded;
  std::vector<size_t> _batch_lengths;
  std::vector<unsigned char*> _batch_starts;
  std::vector<size_t> _coded_lengths;
  std::vector<unsigned char*> _coded_starts;
  std::vector<SpanningBlock> _spans;
  std::string _separators;
  bstring _separator;
  bstring _last;
  std::string _inline_starts;
  bstring _range_regions;
  std::vector<FieldHead> _heads;
  std::vector<uint32_t> _prefixes;
  std::string _leads;
  bstring _lead_bound;
  bstring _key_prefix;
  bstring _min;
  PostingMeta _single;
  std::array<byte_type, PostingMeta::kInlineBytes> _single_inline{};
  IdxWriter* _idx = nullptr;
  IndexOutput* _out = nullptr;
  uint64_t _docs = 0;
  uint64_t _terms = 0;
  uint64_t _first_page = 0;
  uint64_t _inline_start = 0;
  uint64_t _inline_last = 0;
  uint32_t _blocks = 0;
  uint32_t _width = 0;
  uint32_t _ordinal = 0;
  uint32_t _ordinal_bytes = 1;
  field_id _last_id = field_limits::invalid();
  DictRef _dict;
  uint32_t _first_block = 0;
  uint32_t _first_entry = 0;
  uint32_t _first_record = 0;
  bool _trained = false;
  bool _fsst_used = false;
  bool _group_inline = false;
  const bool _compaction;
};

}  // namespace irs::term_dict
