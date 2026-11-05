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

#include "iresearch/formats/index/field_writer.hpp"

#include <absl/algorithm/container.h>

#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <iterator>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_parts.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr size_t kFsstMinSamples = 64;
constexpr uint64_t kFsstMinMeanSuffix = 2;
constexpr uint64_t kFsstMaxMeanSuffix = 16;
constexpr uint64_t kFsstMinRatioPct = 115;

std::vector<unsigned char*> SampleStarts(bstring& arena,
                                         std::span<const size_t> lengths) {
  std::vector<unsigned char*> starts(lengths.size());
  auto* start = arena.data();
  for (size_t i = 0; i != lengths.size(); ++i) {
    starts[i] = start;
    start += lengths[i];
  }
  return starts;
}

bool Compresses(duckdb_fsst_encoder_t* encoder, bstring& arena,
                std::vector<size_t>& lengths, uint64_t table_size) {
  auto starts = SampleStarts(arena, lengths);
  const uint64_t raw = arena.size();
  bstring out(7 + 2 * raw, 0);
  std::vector<size_t> out_lengths(lengths.size());
  std::vector<unsigned char*> out_starts(lengths.size());
  const size_t done = duckdb_fsst_compress(
    encoder, lengths.size(), lengths.data(), starts.data(), out.size(),
    out.data(), out_lengths.data(), out_starts.data());
  const uint64_t coded = absl::c_accumulate(out_lengths, uint64_t{0});
  return done == lengths.size() &&
         raw * 100 >= coded * kFsstMinRatioPct + table_size * 100;
}

void AddPrefix(std::vector<uint32_t>& prefixes, uint32_t prefix) {
  if (prefixes.empty() || prefixes.back() != prefix) {
    prefixes.push_back(prefix);
  }
}

template<typename Visitor>
void VisitPart(const TermPart& part, IndexFeatures features,
               Visitor&& visitor) {
  TermPartReader terms{part.terms, features, part.bases};
  while (terms.Next()) {
    visitor(terms.Term(), terms.Meta());
  }
}

}  // namespace

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          kMaxRecordBytes, " bytes"));
}

PartBlocks::PartBlocks(const PartsField& field, size_t part)
  : blocks{field.inline_bases[part]} {
  blocks.SetFsst(field.fsst);
  blocks.BeginField(field.width, field.encode, field.props.index_features);
  blocks.SetKeyPrefix(field.key_prefix);
  VisitPart(field.parts[part], field.props.index_features,
            [&](bytes_view term, const PostingMeta& meta) {
              if (field.width == 0) {
                AddPrefix(prefixes, TermPrefix(term));
              }
              if (terms++ == 0) {
                min.assign(term);
              }
              blocks.Add(term, meta);
            });
}

void FieldWriter::SampleFsst(const BasicTermReader& reader) {
  reader.SampleTerms(kFsstSampleBytes, [&](std::span<const bytes_view> run) {
    for (size_t i = 1; i < run.size(); ++i) {
      const auto suffix = run[i].substr(CommonPrefixLength(run[i - 1], run[i]));
      if (!suffix.empty()) {
        _samples.append(suffix);
        _sample_lengths.push_back(suffix.size());
      }
    }
  });
  const bool enough = _sample_lengths.size() >= kFsstMinSamples;
  if (_fsst) {
    _encode = !enough || Compresses(_fsst.get(), _samples, _sample_lengths, 0);
    _samples.clear();
    _sample_lengths.clear();
    return;
  }
  if (!enough) {
    return;
  }
  TrainFsst();
  _samples.clear();
  _sample_lengths.clear();
  if (_fsst) {
    _blocks.SetFsst(_fsst.get());
    _blocks.Recode();
  }
}

void FieldWriter::TrainFsst() {
  auto& arena = _samples;
  auto& lengths = _sample_lengths;
  const uint64_t raw = arena.size();
  if (raw < kFsstMinMeanSuffix * lengths.size() ||
      raw > kFsstMaxMeanSuffix * lengths.size()) {
    return;
  }
  auto starts = SampleStarts(arena, lengths);
  FsstEncoder encoder{
    duckdb_fsst_create(lengths.size(), lengths.data(), starts.data(), 0)};
  if (!encoder) {
    return;
  }
  unsigned char table[FSST_MAXHEADER];
  const auto table_size = duckdb_fsst_export(encoder.get(), table);
  if (!Compresses(encoder.get(), arena, lengths, table_size)) {
    return;
  }
  _fsst = std::move(encoder);
  _fsst_table.assign(reinterpret_cast<const char*>(table), table_size);
}

void FieldWriter::prepare(const FlushState& state, uint64_t keyed_dicts,
                          PostingsOutputs postings) {
  SDB_ASSERT(_idx);
  if (keyed_dicts > OrdinalLimit(kMaxOrdinalBytes)) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("term dictionary: ", keyed_dicts,
                            " keyed dictionaries exceed the limit of ",
                            OrdinalLimit(kMaxOrdinalBytes)));
  }
  Reset();
  _docs = state.doc_count;
  _ordinal_bytes = OrdinalBytes(keyed_dicts);
  _blocks.Open(_idx->BlocksOut());
  _pw.Prepare(state, std::move(postings));
}

void FieldWriter::Begin(field_id id, DictRef dict) noexcept {
  SDB_ASSERT(Contains(dict));
  SDB_ASSERT(!field_limits::valid(_last_id) || _last_id < id ||
             (_last_id == id && _dict.layout == dict.layout &&
              _dict.member < dict.member));
  _last_id = id;
  _dict = dict;
}

void FieldWriter::BeginTerms(const BasicTermReader& reader,
                             const FieldProperties& props) {
  _width = reader.key_width <= kMaxStride ? reader.key_width : 0;
  if (_width == 0) {
    SampleFsst(reader);
  }
  _blocks.BeginField(_width, _encode, props.index_features);
  if (IsSubsetOf(IndexFeatures::Ranges, props.index_features) && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw, _docs, props.index_features);
  }
}

void FieldWriter::WriteSlot(field_id id, DictRef dict,
                            std::span<const doc_id_t> docs) {
  BeginSlot(id, dict);
  PostingMeta meta;
  _pw.WritePostings(
    PostingRows{.span = {.docs = docs.data(), .count = docs.size()}}, meta);
  EndSlot(id, meta);
}

void FieldWriter::WriteSlot(field_id id, DictRef dict, TermPostings& docs) {
  BeginSlot(id, dict);
  PostingMeta meta;
  _pw.Write(docs, meta);
  EndSlot(id, meta);
}

void FieldWriter::BeginSlot(field_id id, DictRef dict) {
  SDB_ASSERT(!IsTermMember(dict.member));
  Begin(id, dict);
  _pw.BeginField({});
}

void FieldWriter::EndSlot(field_id id, const PostingMeta& meta) {
  std::string record_inline{ViewCast<char>(meta.Inline())};
  _pw.EndField();
  if (meta.docs_count == 0) {
    return;
  }
  _record.Clear();
  RecordEncoder{}.Encode(_record, meta);
  _heads.push_back({
    .id = id,
    .layout = _dict.layout,
    .member = _dict.member,
    .term_count = 1,
    .doc_count = meta.docs_count,
    .record = std::string{ViewCast<char>(_record.View())},
    .record_inline = std::move(record_inline),
  });
}

void FieldWriter::write(DictRef dict, const BasicTermReader& reader) {
  const auto id = reader.id();
  SDB_ASSERT(IsTermMember(dict.member));
  Begin(id, dict);
  const auto props = reader.properties();
  _pw.BeginField(props, reader.PayloadWriter());
  BeginTerms(reader, props);

  const uint64_t first_inline = _blocks.InlineEnd();
  uint64_t total_freq = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](bytes_view term, const PostingMeta& meta) {
    if (freq_exists) {
      total_freq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    if (!freq_exists) {
      total_freq += meta.docs_count;
    }
    Add(term, meta);
    if (_ranges.Active()) {
      _ranges.Add(term, meta);
    }
  };

  auto terms = reader.iterator();
  SDB_ASSERT(terms);

  const auto tee = _ranges.Tee();
  if (_compaction) {
    while (terms->next()) {
      PostingMeta meta;
      auto postings = terms->postings(props.index_features);
      _pw.Write(*postings, meta, tee);
      consume(terms->value(), meta);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta meta;
        _pw.WritePostings(posting_batch[i], meta, tee);
        consume(term_batch[i], meta);
      }
    }
  }

  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  EndField(id, props, total_freq, first_inline, _pw.EndField());
  ResetField();
}

PartsField FieldWriter::BeginParts(DictRef dict, const BasicTermReader& reader,
                                   std::span<const TermPart> parts,
                                   uint64_t docs) {
  const auto id = reader.id();
  SDB_ASSERT(IsTermMember(dict.member));
  Begin(id, dict);
  PartsField field{
    .id = id,
    .props = reader.properties(),
    .first_inline = _blocks.InlineEnd(),
    .docs = docs,
  };
  const auto features = field.props.index_features;
  _pw.BeginField(field.props);
  BeginTerms(reader, field.props);

  const bool freq_exists =
    IndexFeatures::None != (features & IndexFeatures::Freq);
  uint64_t terms = 0;
  for (const auto& part : parts) {
    SDB_ASSERT((part.ranges != nullptr) == _ranges.Active());
    if (part.ranges) {
      _ranges.Append(*part.ranges, part.bases.doc);
    }
    field.total_freq +=
      freq_exists ? part.total_term_freq : part.total_doc_freq;
    terms += part.term_count;
  }
  if (terms < 2) {
    for (const auto& part : parts) {
      VisitPart(part, features, [&](bytes_view term, const PostingMeta& meta) {
        Add(term, meta);
      });
    }
    return field;
  }

  AssignOrdinal();
  field.parts = parts;
  field.inline_bases.reserve(parts.size());
  uint64_t base = _blocks.InlineEnd();
  for (const auto& part : parts) {
    field.inline_bases.push_back(base);
    base += part.inline_bytes;
  }
  field.key_prefix = _key_prefix;
  field.fsst = _encode && _width == 0 ? _fsst.get() : nullptr;
  field.width = _width;
  field.encode = _encode;
  return field;
}

void FieldWriter::AppendPart(PartBlocks& built) {
  if (built.terms == 0) {
    return;
  }
  _blocks.Cut();
  if (_terms == 0) {
    _min.assign(built.min);
    _first_block = _blocks.Blocks();
    _first_entry = 0;
    _first_record = 0;
  }
  _terms += built.terms;
  for (const auto prefix : built.prefixes) {
    AddPrefix(_prefixes, prefix);
  }
  bstring first{_key_prefix};
  first.append(built.min);
  _blocks.Splice(built.blocks, first);
}

void FieldWriter::EndParts(const PartsField& field) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  auto stats = _pw.EndField();
  stats.docs_count = static_cast<doc_id_t>(field.docs);
  EndField(field.id, field.props, field.total_freq, field.first_inline, stats);
  ResetField();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_width == 0) {
    AddPrefix(_prefixes, TermPrefix(term));
  }
  if (_terms++ == 0) {
    _min.assign(term);
    _single = meta;
    if (meta.inline_size != 0) {
      std::memcpy(_single_inline.data(), meta.inline_data, meta.inline_size);
      _single.inline_data = _single_inline.data();
    }
    return;
  }
  if (_terms == 2) {
    AssignOrdinal();
    Commit(_min, _single);
  }
  Commit(term, meta);
}

void FieldWriter::AssignOrdinal() {
  SDB_ENSURE(_ordinal < OrdinalLimit(_ordinal_bytes),
             "term dictionary: more keyed dictionaries than ",
             OrdinalLimit(_ordinal_bytes));
  _key_prefix.clear();
  AppendOrdinal(_key_prefix, _ordinal++, _ordinal_bytes);
  _blocks.SetKeyPrefix(_key_prefix);
}

void FieldWriter::Commit(bytes_view term, const PostingMeta& meta) {
  _blocks.Add(term, meta);
  if (_first_block == kNoBlock) {
    _first_block = _blocks.Blocks();
    _first_entry = _blocks.OpenEntries() - 1;
    _first_record = _blocks.LastRecordStart();
  }
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_freq, uint64_t first_inline,
                           const PostingsWriter::FieldStats& stats) {
  if (_terms == 0) {
    return;
  }

  auto& head = _heads.emplace_back();
  head.id = id;
  head.layout = _dict.layout;
  head.member = _dict.member;
  head.features = props.index_features;
  head.term_count = _terms;
  head.doc_count = stats.docs_count;
  const bool text = _dict.member == DictMember::Strings;
  if (text) {
    head.text.total_freq = total_freq;
    SDB_IF_FAILURE("irs::term_dict::TotalTermFreqPast32Bits") {
      head.text.total_freq += uint64_t{1} << 32;
    }
    head.text.norm = props.norm;
    head.text.has_score_bounds = stats.has_score_bounds;
  }
  head.width = _width;
  head.min = ViewCast<char>(bytes_view{_min});
  if (_terms == 1) {
    _record.Clear();
    RecordEncoder{props.index_features}.Encode(_record, _single);
    head.record = ViewCast<char>(_record.View());
    head.record_inline = ViewCast<char>(_single.Inline());
    return;
  }
  const auto max = _blocks.LastKey().substr(_key_prefix.size());
  head.max = ViewCast<char>(max);
  head.first_block = _first_block;
  head.first_entry = _first_entry;
  head.first_record = _first_record;
  head.last_block = _blocks.Blocks();
  head.last_entry = _blocks.OpenEntries() - 1;
  if (_first_block == _blocks.Blocks()) {
    head.inline_base = first_inline;
    head.inline_size = _blocks.InlineEnd() - first_inline;
  } else if (text && _width == 0) {
    auto out = std::back_inserter(head.text.prefixes);
    uint32_t last = 0;
    for (const auto prefix : _prefixes) {
      WriteVarint(prefix - last, out);
      last = prefix;
    }
  }
  if (_ranges.Active()) {
    const auto prefix = CommonPrefixLength(bytes_view{_min}, max);
    const auto offset = _range_regions.size();
    auto& ranges = head.ranges;
    _ranges.Write(_range_regions, prefix, ranges.counts);
    if (!ranges.counts.empty()) {
      SDB_ASSERT(_dict.member == DictMember::Numbers);
      ranges.offset = offset;
      ranges.size = _range_regions.size() - offset;
      ranges.prefix = static_cast<uint32_t>(prefix);
    }
    SDB_ASSERT(!ranges.counts.empty() || _range_regions.size() == offset);
  }
}

void FieldWriter::ResetField() noexcept {
  _ranges.Clear();
  _prefixes.clear();
  _min.clear();
  _terms = 0;
  _first_block = kNoBlock;
  _encode = true;
}

void FieldWriter::Reset() noexcept {
  ResetField();
  _blocks.Reset();
  _fsst.reset();
  _fsst_table.clear();
  _samples.clear();
  _sample_lengths.clear();
  _range_regions.clear();
  _heads.clear();
  _ordinal = 0;
  _ordinal_bytes = 1;
  _last_id = field_limits::invalid();
  _dict = {};
}

void FieldWriter::end() {
  _blocks.Cut();
  _pw.End();
  if (!_heads.empty()) {
    auto& out = _idx->BlocksOut();
    SDB_ASSERT(out.Position() == _blocks.Position());
    TermsMeta terms{
      .fsst_table = std::move(_fsst_table),
      .ranges_size = _range_regions.size(),
      .fields = std::move(_heads),
      .ordinal_bytes = _ordinal_bytes,
    };
    _blocks.Finish(terms);
    out.WriteData(_range_regions.data(), _range_regions.size());
    _idx->SetTerms(std::move(terms));
  }
  Reset();
  _idx = nullptr;
}

}  // namespace irs::term_dict
