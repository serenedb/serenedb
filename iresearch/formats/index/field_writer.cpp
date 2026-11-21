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

#include <algorithm>
#include <cstring>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iterator>
#include <span>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr byte_type kZeros[kPage]{};
constexpr uint64_t kMaxBlockPages = 16;

uint32_t PagesFor(const BlockShare& share) noexcept {
  const uint64_t keyed = share.bytes - share.inline_bytes;
  if (keyed == 0) {
    return 1;
  }
  return static_cast<uint32_t>(
    std::clamp<uint64_t>((share.bytes + keyed / 2) / keyed, 1, kMaxBlockPages));
}

}  // namespace

bool FieldWriter::OwnsBlock() const noexcept {
  return _first_block != kNoBlock &&
         (_first_block != _blocks || _first_entry == 0);
}

void FieldWriter::prepare(const FlushState& state, uint64_t keyed_dicts) {
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
  _out = &_idx->BlocksOut();
  _pw.Prepare(state);
}

void FieldWriter::Begin(field_id id, DictRef dict) noexcept {
  SDB_ASSERT(Contains(dict));
  SDB_ASSERT(!field_limits::valid(_last_id) || _last_id < id ||
             (_last_id == id && _dict.layout == dict.layout &&
              _dict.member < dict.member));
  _last_id = id;
  _dict = dict;
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
  _pw.SetTermPayloadWriter(nullptr);
  _pw.BeginField({});
}

void FieldWriter::EndSlot(field_id id, const PostingMeta& meta) {
  std::string record_inline{ViewCast<char>(meta.Inline())};
  _pw.EndField();
  if (meta.docs_count == 0) {
    return;
  }
  RecordOutput record;
  _pw.Encode(record, meta);
  _heads.push_back({
    .id = id,
    .layout = _dict.layout,
    .member = _dict.member,
    .term_count = 1,
    .doc_count = meta.docs_count,
    .record = std::string{ViewCast<char>(record.View())},
    .record_inline = std::move(record_inline),
  });
}

void FieldWriter::write(DictRef dict, const BasicTermReader& reader) {
  const auto id = reader.id();
  SDB_ASSERT(IsTermMember(dict.member));
  Begin(id, dict);
  const auto props = reader.properties();
  _pw.SetTermPayloadWriter(reader.PayloadWriter());
  _pw.BeginField(props);
  _records.run.Start(props.index_features);
  _width = reader.key_width <= kMaxStride ? reader.key_width : 0;
  _encoder.StartField(reader, _width);
  StartBlocks();
  _index.Reset(GatesTerms(_dict.member, _width));
  if (IsSubsetOf(IndexFeatures::Ranges, props.index_features) && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw, _docs);
  }

  uint64_t total_freq = 0;
  uint64_t docs_sum = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](bytes_view term, const PostingMeta& meta) {
    if (freq_exists) {
      total_freq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    docs_sum += meta.docs_count;
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

  EndField(id, props, total_freq, docs_sum);
  if (_encoder.Coded()) {
    Cut();
  }
  CloseRun();
  if (_ranges.Active() && _ranges.Height() != 0) {
    WriteLevels(_heads.back());
  }
  ResetField();
}

void FieldWriter::WriteLevels(FieldHead& head) {
  SDB_ASSERT(head.term_count > 1 && head.levels.empty());
  head.levels.reserve(_ranges.Height());
  for (size_t level = 0; level != _ranges.Height(); ++level) {
    _min.clear();
    _terms = 0;
    _first_block = kNoBlock;
    _index.Reset(GatesTerms(_dict.member, _width));
    _records.run.Start(head.features);
    StartBlocks();
    const uint32_t count = _ranges.Count(level);
    for (uint32_t i = 0; i != count; ++i) {
      const auto entry = _ranges.At(level, i);
      Add(entry.key, entry.meta);
    }
    SDB_ASSERT(_terms == count && count > 1);
    auto& dict = head.levels.emplace_back();
    dict.id = head.id;
    dict.layout = head.layout;
    dict.member = head.member;
    dict.features = head.features;
    dict.term_count = count;
    dict.doc_count = head.doc_count;
    dict.width = head.width;
    dict.min = ViewCast<char>(bytes_view{_min});
    EndKeyed(dict);
    CloseRun();
  }
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_terms++ == 0) {
    _min.assign(term);
    _single = meta;
    return;
  }
  if (_terms == 2) {
    SDB_ENSURE(_ordinal < OrdinalLimit(_ordinal_bytes),
               "term dictionary: more keyed dictionaries than ",
               OrdinalLimit(_ordinal_bytes));
    _key_prefix.clear();
    AppendOrdinal(_key_prefix, _ordinal++, _ordinal_bytes);
    Commit(_min, _single);
  }
  Commit(term, meta);
}

void FieldWriter::Commit(bytes_view term, const PostingMeta& meta) {
  if (!Append(term, meta)) {
    if (_first_block != kNoBlock) {
      FeedIndex();
    }
    Cut();
    Append(term, meta);
  }
  if (_first_block == kNoBlock) {
    _first_block = _blocks;
    _first_entry = _encoder.Count() - 1;
    _first_record = static_cast<uint32_t>(_records.bytes.size());
  }
}

void FieldWriter::CloseRun() {
  auto& run = _records.run;
  auto& bytes = _records.bytes;
  if (run.Empty()) {
    return;
  }
  const auto at = bytes.size();
  const uint64_t size = run.Bytes();
  bytes.resize(at + size);
  duckdb::MemoryStream out{bytes.data() + at, size};
  run.Write(out);
  SDB_ASSERT(out.GetPosition() == size);
  _records.inline_bytes += run.InlineBytes();
  run.Clear();
}

bool FieldWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& run = _records.run;
  const auto staged = run.Stage(meta);
  const uint64_t records = _records.bytes.size() + run.SizeOf(staged);
  if (!_encoder.Append(_key_prefix, term, records, OwnsBlock())) {
    return false;
  }
  run.Commit(meta, staged);
  return true;
}

void FieldWriter::FeedIndex() {
  const auto prefix = _key_prefix.size();
  const auto first = _first_block == _blocks ? _first_entry : 0;
  _index.Add(_encoder.Count() - first, [&](uint32_t i) {
    return _encoder.Key(first + i).substr(prefix);
  });
}

void FieldWriter::Cut() {
  if (_encoder.Empty()) {
    return;
  }
  const auto first = _encoder.Key(0);
  if (_blocks != 0) {
    const auto separator =
      first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1);
    const auto shared = CommonPrefixLength(bytes_view{_separator}, separator);
    const auto suffix = separator.substr(shared);
    auto out = std::back_inserter(_separators);
    WriteVarint(static_cast<uint32_t>(shared), out);
    WriteVarint(static_cast<uint32_t>(suffix.size()), out);
    _separators.append(ViewCast<char>(suffix));
    _separator.assign(separator);
  }
  _last.assign(_encoder.Key(_encoder.Count() - 1));
  CloseRun();
  const uint64_t size = _encoder.Seal(_records.bytes.size());
  WriteBlock(size);
  _share.bytes += size;
  _share.inline_bytes += _records.inline_bytes;
  _encoder.Reset();
  _encoder.SetPages(PagesFor(_share));
  _records.bytes.clear();
  _records.inline_bytes = 0;
}

void FieldWriter::StartBlocks() noexcept {
  _share = {};
  if (_encoder.Empty()) {
    _encoder.SetPages(1);
  }
}

void FieldWriter::WriteBlock(uint64_t size) {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  if (_blocks == 0) {
    _first_page = (position + pad) / kPage;
  }
  const auto pages = static_cast<uint32_t>((size + kPage - 1) / kPage);
  if (pages != 1) {
    _spans.push_back({.block = _blocks, .extra = pages - 1});
  }
  _encoder.Serialize(*_out, _records.bytes);
  SDB_ASSERT(_out->Position() == position + pad + size);
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_freq, uint64_t docs_sum) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  const auto stats = _pw.EndField();
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
  head.single_valued = docs_sum == stats.docs_count;
  const bool text = _dict.member == DictMember::Strings;
  if (text) {
    head.text.total_freq = total_freq;
    head.text.norm = props.norm;
    head.text.has_score_bounds = stats.has_score_bounds;
  }
  head.width = _width;
  head.min = ViewCast<char>(bytes_view{_min});
  if (_terms == 1) {
    RecordOutput record;
    _pw.Encode(record, _single);
    head.record = ViewCast<char>(record.View());
    head.record_inline = ViewCast<char>(_single.Inline());
    return;
  }
  head.fsst_table = _encoder.TakeFsstTable();
  EndKeyed(head);
}

void FieldWriter::EndKeyed(FieldHead& head) {
  const uint32_t last = _encoder.Count() - 1;
  head.max = ViewCast<char>(_encoder.Key(last).substr(_key_prefix.size()));
  head.first_block = _first_block;
  head.first_entry = _first_entry;
  head.first_record = _first_record;
  head.last_block = _blocks;
  head.last_entry = last;
  if (_first_block != _blocks) {
    FeedIndex();
    _index.Finish(head);
  }
}

void FieldWriter::ResetField() noexcept {
  _ranges.Clear();
  _min.clear();
  _terms = 0;
  _first_block = kNoBlock;
  _encoder.EndField();
}

void FieldWriter::Reset() noexcept {
  ResetField();
  _encoder.Reset();
  _encoder.SetPages(1);
  _share = {};
  _records.run.Clear();
  _records.bytes.clear();
  _records.inline_bytes = 0;
  _spans.clear();
  _heads.clear();
  _last.clear();
  _separator.clear();
  _separators.clear();
  _first_page = 0;
  _blocks = 0;
  _ordinal = 0;
  _ordinal_bytes = 1;
  _last_id = field_limits::invalid();
  _dict = {};
}

void FieldWriter::end() {
  Cut();
  _pw.End();
  if (!_heads.empty()) {
    auto& out = *_out;
    TermsMeta terms{
      .blocks = _blocks,
      .first_page = _first_page,
      .spans = std::move(_spans),
      .blocks_end = out.Position(),
      .fields = std::move(_heads),
      .separators = std::move(_separators),
      .ordinal_bytes = _ordinal_bytes,
    };
    _idx->SetTerms(std::move(terms));
  }
  Reset();
  _idx = nullptr;
  _out = nullptr;
}

}  // namespace irs::term_dict
