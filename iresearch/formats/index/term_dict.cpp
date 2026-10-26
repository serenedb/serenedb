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

#include "iresearch/formats/index/term_dict.hpp"

#include <absl/algorithm/container.h>

#include <cstring>
#include <limits>
#include <string_view>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/index/typed_terms.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr uint8_t kFieldBlocks = 1;
constexpr uint8_t kFieldFixed = 2;
constexpr uint8_t kFieldDictless = 4;
constexpr uint8_t kFieldFsst = 8;
constexpr uint8_t kFieldRanges = 16;
constexpr uint8_t kFieldInline = 32;
constexpr std::string_view kDictlessKeys[]{kNullTerm, kFalseTerm, kTrueTerm};
constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr size_t kFsstMinSamples = 64;
constexpr uint64_t kFsstMinMeanSuffix = 2;
constexpr uint64_t kFsstMaxMeanSuffix = 16;
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint64_t headers,
                  uint64_t suffixes, uint64_t records) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  return 1 + VSize(count) + VSize(restarts) + VSize(headers) + VSize(suffixes) +
         VSize(records) + VSize(head_skip) +
         (kVarRestart + kHeadBytes) * restarts +
         kRecordOffset * RecordGroups(count) + headers + suffixes + records +
         kSlack;
}

uint64_t FixedBytes(uint64_t count, uint64_t records, uint64_t width,
                    uint64_t prefix) noexcept {
  const uint64_t stride = width - prefix;
  const uint64_t groups = RecordGroups(count);
  return 1 + VSize(count) + VSize(groups) + VSize(records) + VSize(stride) +
         VSize(prefix) + prefix + (kRecordOffset + kHeadBytes) * groups +
         count * stride + records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

void Clear(VarColumns& columns) noexcept {
  columns.headers.clear();
  columns.suffixes.clear();
  columns.restarts.clear();
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  block.records.clear();
  block.record_starts.clear();
  Clear(block.raw);
  Clear(block.coded);
  block.size = 0;
  block.coded_size = std::numeric_limits<uint64_t>::max();
  block.width = 0;
  block.prefix = 0;
  block.head_skip = 0;
  block.kind = BlockKind::Var;
}

uint64_t HeaderBytes(size_t lcp, size_t len) noexcept {
  return VSize(uint64_t{lcp} << 5 | std::min<uint64_t>(len, 31)) +
         (len >= 31 ? VSize(len - 31) : 0);
}

void AppendEntry(VarColumns& columns, bool restart, size_t lcp,
                 bytes_view suffix) {
  if (restart) {
    columns.restarts.push_back({
      static_cast<uint16_t>(columns.headers.size()),
      static_cast<uint16_t>(columns.suffixes.size()),
    });
  }
  BytesOutput out{columns.headers};
  out.WriteV64(uint64_t{lcp} << 5 | std::min<uint64_t>(suffix.size(), 31));
  if (suffix.size() >= 31) {
    out.WriteV64(suffix.size() - 31);
  }
  columns.suffixes.append(suffix);
}

void WriteHeads(BytesOutput& out, const BlockBuilder& block, size_t step,
                size_t skip) {
  for (size_t i = 0; i < block.ends.size(); i += step) {
    out.WriteU32(static_cast<uint32_t>(HeadAt(KeyAt(block, i), skip) >> 32));
  }
}

void Serialize(const BlockBuilder& block, bstring& page) {
  page.clear();
  BytesOutput out{page};
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ASSERT(block.record_starts.size() == RecordGroups(count));
  out.WriteByte(static_cast<byte_type>(block.kind));
  out.WriteV32(count);
  const bool coded = block.kind == BlockKind::VarFsst;
  if (block.kind != BlockKind::FixedStride) {
    const auto& columns = coded ? block.coded : block.raw;
    out.WriteV32(static_cast<uint32_t>(columns.restarts.size()));
    out.WriteV64(columns.headers.size());
    out.WriteV64(columns.suffixes.size());
    out.WriteV64(block.records.size());
    out.WriteV32(block.head_skip);
    for (const auto& restart : columns.restarts) {
      out.WriteU16(restart[0]);
      out.WriteU16(restart[1]);
    }
    WriteHeads(out, block, kRestartInterval, block.head_skip);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    out.WriteData(columns.headers.data(), columns.headers.size());
    out.WriteData(columns.suffixes.data(), columns.suffixes.size());
  } else {
    const uint32_t stride = block.width - block.prefix;
    out.WriteV32(static_cast<uint32_t>(block.record_starts.size()));
    out.WriteV64(block.records.size());
    out.WriteV32(stride);
    out.WriteV32(block.prefix);
    out.WriteData(block.keys.data(), block.prefix);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    WriteHeads(out, block, kRecordInterval, block.prefix);
    for (size_t i = 0; i != count; ++i) {
      out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
    }
  }
  out.WriteData(block.records.data(), block.records.size());
  out.WriteData(kZeros, kSlack);
  SDB_ASSERT(page.size() == (coded ? block.coded_size : block.size));
}

uint8_t DictlessMask(const BlockBuilder& block) noexcept {
  uint8_t mask = 0;
  for (size_t i = 0; i != block.ends.size(); ++i) {
    const auto key = ViewCast<char>(KeyAt(block, i));
    const auto* it = absl::c_find(kDictlessKeys, key);
    if (it == std::end(kDictlessKeys)) {
      return 0;
    }
    mask |= 1 << (it - std::begin(kDictlessKeys));
  }
  return mask;
}

void BuildDictless(uint8_t mask, bytes_view records, bstring& page,
                   bstring& min, bstring& max) {
  BlockBuilder block;
  for (size_t i = 0; i != std::size(kDictlessKeys); ++i) {
    if ((mask >> i & 1) == 0) {
      continue;
    }
    const auto key = ViewCast<byte_type>(kDictlessKeys[i]);
    if (block.ends.empty()) {
      min.assign(key);
    }
    max.assign(key);
    AppendEntry(block.raw, block.ends.empty(), 0, key);
    block.keys.append(key);
    block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  }
  SDB_ENSURE(!block.ends.empty(), "term dictionary: empty dictless field");
  block.records.assign(records);
  block.record_starts.push_back(0);
  block.head_skip = static_cast<uint32_t>(KeyAt(block, 0).size());
  block.size =
    VarBytes(block.ends.size(), block.head_skip, block.raw.headers.size(),
             block.raw.suffixes.size(), block.records.size());
  Serialize(block, page);
}

uint32_t Find(const BlockView& view, Probe key,
              const duckdb_fsst_decoder_t* fsst) noexcept {
  if (view.kind == BlockKind::Var) {
    const auto bound = VarLowerBound(view, key, fsst);
    return bound.equal ? bound.entry : kNoEntry;
  }
  if (key.size != size_t{view.prefix} + view.stride ||
      PaddedMismatch(Prefix(view).data(), key.data, view.prefix) !=
        view.prefix) {
    return kNoEntry;
  }
  const auto entry = FixedLowerBound(view, key);
  if (entry == view.count ||
      PaddedMismatch(FixedKey(view, entry).data(), key.data + view.prefix,
                     view.stride) != view.stride) {
    return kNoEntry;
  }
  return entry;
}

const byte_type* MapAt(IndexInput& in, uint64_t offset, uint64_t size,
                       bstring& copy) {
  if (const auto* p = in.ReadStable(offset, size)) {
    return p;
  }
  copy.resize(size);
  in.ReadData(offset, copy.data(), size);
  return copy.data();
}

const byte_type* Map(IndexInput& in, uint64_t size, bstring& copy) {
  const uint64_t offset = in.Position();
  const auto* p = MapAt(in, offset, size, copy);
  in.Seek(offset + size);
  return p;
}

IndexInput& Reopened(BlockBuffer& buffer, const IndexInput& in) {
  if (!buffer.in) {
    buffer.in = in.Reopen();
  }
  return *buffer.in;
}

PostingMeta DecodeRecord(const TermReaderImpl& field, const BlockView& view,
                         uint32_t entry, BlockBuffer& buffer) {
  auto cursor = GroupStart(view, entry / kRecordInterval);
  PostingMeta meta;
  DecodeRecords(cursor, entry % kRecordInterval + 1,
                field.meta().index_features, meta);
  if (meta.inline_size != 0) {
    field.ReadInline(meta, buffer);
  }
  return meta;
}

class TermIteratorImpl : public SeekTermIterator {
 public:
  explicit TermIteratorImpl(const TermReaderImpl& field) noexcept
    : _cursor{field} {}

  void ReadaheadUntil(uint32_t end) noexcept {
    _cursor.ReadaheadUntil(end, true);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        if (_cursor.Field().size() == 0) {
          return End();
        }
        _cursor.Load(0);
        _cursor.Next();
        break;
      case Position::At:
        if (!_cursor.Advance()) {
          return End();
        }
        break;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return true;
  }

  SeekResult seek_ge(bytes_view term) final {
    const auto result = _cursor.SeekGe(term);
    if (result == SeekResult::End) {
      End();
      return result;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return result;
  }

  bool seek(bytes_view term) final {
    return seek_ge(term) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    const auto& field = _cursor.Field();
    return field.Owner().Postings().Postings(
      field.meta().index_features, features, _cursor.Meta(),
      field.HasScoreBounds(), std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  mutable Cursor _cursor;
  TermAttr _term;
  Position _position = Position::Before;
};

}  // namespace

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          RecordBytes{}.bytes.size(), " bytes"));
}

void FsstEncoderDelete::operator()(
  duckdb_fsst_encoder_t* encoder) const noexcept {
  duckdb_fsst_destroy(encoder);
}

void FieldWriter::TrainFsst(const BasicTermReader& reader) {
  bstring arena;
  std::vector<size_t> lengths;
  reader.SampleTerms(kFsstSampleBytes, [&](std::span<const bytes_view> run) {
    for (size_t i = 1; i < run.size(); ++i) {
      const auto suffix = run[i].substr(CommonPrefixLength(run[i - 1], run[i]));
      if (!suffix.empty()) {
        arena.append(suffix);
        lengths.push_back(suffix.size());
      }
    }
  });
  const uint64_t raw = arena.size();
  if (lengths.size() < kFsstMinSamples ||
      raw < kFsstMinMeanSuffix * lengths.size() ||
      raw > kFsstMaxMeanSuffix * lengths.size()) {
    return;
  }
  std::vector<unsigned char*> starts(lengths.size());
  auto* start = arena.data();
  for (size_t i = 0; i != lengths.size(); ++i) {
    starts[i] = start;
    start += lengths[i];
  }
  FsstEncoder encoder{
    duckdb_fsst_create(lengths.size(), lengths.data(), starts.data(), 0)};
  if (!encoder) {
    return;
  }
  unsigned char table[FSST_MAXHEADER];
  const auto table_size = duckdb_fsst_export(encoder.get(), table);
  bstring out(7 + 2 * raw, 0);
  std::vector<size_t> out_lengths(lengths.size());
  std::vector<unsigned char*> out_starts(lengths.size());
  const size_t done = duckdb_fsst_compress(
    encoder.get(), lengths.size(), lengths.data(), starts.data(), out.size(),
    out.data(), out_lengths.data(), out_starts.data());
  const uint64_t coded = absl::c_accumulate(out_lengths, uint64_t{0});
  if (done != lengths.size() ||
      raw * 100 < coded * kFsstMinRatioPct + uint64_t{table_size} * 100) {
    return;
  }
  _fsst = std::move(encoder);
  _fsst_table.assign(table, table_size);
}

bytes_view FieldWriter::EncodeSuffix(bytes_view suffix) {
  if (suffix.empty()) {
    return {};
  }
  size_t length = suffix.size();
  auto* input = const_cast<unsigned char*>(suffix.data());
  utils::StrResizeAmortized(_coded, 7 + 2 * length);
  size_t out_length = 0;
  unsigned char* out = nullptr;
  const size_t done =
    duckdb_fsst_compress(_fsst.get(), 1, &length, &input, _coded.size(),
                         _coded.data(), &out_length, &out);
  SDB_ASSERT(done == 1);
  return {out, out_length};
}

void FieldWriter::prepare(const FlushState& state) {
  SDB_ASSERT(_idx);
  _out = &_idx->BlocksOut();
  _pw.Prepare(state);
}

void FieldWriter::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  _pw.SetTermPayloadWriter(reader.PayloadWriter());
  _pw.BeginField(props);
  const auto width = reader.KeyWidth();
  _width = width <= kMaxStride ? width : 0;
  if (_width == 0) {
    TrainFsst(reader);
  }
  if (props.ranges && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw);
  }

  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](auto&& term_of, const PostingMeta& meta) {
    if (freq_exists) {
      sum_tfreq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    sum_dfreq += meta.docs_count;
    const bytes_view term = term_of();
    Add(term, meta);
    if (_ranges.Active()) {
      _ranges.Add(term, meta);
    }
  };

  auto terms = reader.iterator();
  SDB_ASSERT(terms);

  if (_compaction) {
    while (terms->next()) {
      PostingMeta meta;
      auto postings = terms->postings(props.index_features);
      _pw.Write(*postings, meta);
      consume([&] { return terms->value(); }, meta);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta meta;
        _pw.WritePostings(posting_batch[i], meta);
        consume([&] { return term_batch[i]; }, meta);
      }
    }
  }

  EndField(reader.id(), props, sum_dfreq, sum_tfreq);
  Reset();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_terms++ == 0) {
    _min.assign(term);
  }
  if (!Append(term, meta)) {
    Cut();
    Append(term, meta);
  }
  _inline.append(meta.Inline());
  if (std::min(_block.size, _block.coded_size) > kPage) {
    Cut();
  }
}

bool FieldWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& block = _block;
  const bool record_start = block.ends.size() % kRecordInterval == 0;
  if (record_start) {
    _pw.BeginBlock();
  }
  _record.Clear();
  _pw.EncodeDetached(_record, meta, _inline.size());
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  if (!(_width != 0 ? AppendFixedKey(term, records)
                    : AppendVarKey(term, records))) {
    return false;
  }
  if (record_start) {
    block.record_starts.push_back(static_cast<uint16_t>(block.records.size()));
  }
  block.records.append(record);
  block.keys.append(term);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  return true;
}

bool FieldWriter::AppendFixedKey(bytes_view term, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ENSURE(term.size() == _width, "term dictionary: a key of ", term.size(),
             " bytes in a field of ", _width, "-byte keys");
  const auto prefix =
    count == 0
      ? _width
      : static_cast<uint32_t>(CommonPrefixLength(KeyAt(block, 0), term));
  const uint64_t size = FixedBytes(count + 1, records, _width, prefix);
  if (count != 0 && size > kPage) {
    return false;
  }
  block.size = size;
  block.width = _width;
  block.prefix = prefix;
  block.kind = BlockKind::FixedStride;
  return true;
}

bool FieldWriter::AppendVarKey(bytes_view term, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const bool restart = count % kRestartInterval == 0;
  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, count - 1), term);
  const auto suffix = term.substr(lcp);
  uint32_t head_skip = block.head_skip;
  if (restart) {
    head_skip = static_cast<uint32_t>(
      count == 0 ? term.size() : CommonPrefixLength(KeyAt(block, 0), term));
  }
  const auto size_with = [&](const VarColumns& columns, size_t suffix_size) {
    return VarBytes(count + 1, head_skip,
                    columns.headers.size() + HeaderBytes(lcp, suffix_size),
                    columns.suffixes.size() + suffix_size, records);
  };
  const uint64_t size = size_with(block.raw, suffix.size());
  bytes_view coded;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  if (_fsst) {
    coded = restart ? suffix : EncodeSuffix(suffix);
    coded_size = size_with(block.coded, coded.size());
  }
  if (count != 0 && std::min(size, coded_size) > kPage) {
    return false;
  }
  AppendEntry(block.raw, restart, lcp, suffix);
  if (_fsst) {
    AppendEntry(block.coded, restart, lcp, coded);
    block.coded_size = coded_size;
  }
  block.size = size;
  block.head_skip = head_skip;
  return true;
}

void FieldWriter::Seal() {
  if (_block.kind == BlockKind::Var && _block.coded_size < _block.size) {
    _block.kind = BlockKind::VarFsst;
    _fsst_used = true;
  }
  Serialize(_block, _page);
}

void FieldWriter::Cut() {
  if (_block.ends.empty()) {
    return;
  }
  Seal();
  const auto first = KeyAt(_block, 0);
  if (_blocks != 0) {
    const auto separator =
      first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1);
    const auto shared = CommonPrefixLength(bytes_view{_separator}, separator);
    BytesOutput out{_separators};
    out.WriteV32(static_cast<uint32_t>(shared));
    WriteStr(out, separator.substr(shared));
    _separator.assign(separator);
  }
  WriteBlock(_page);
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void FieldWriter::WriteBlock(bytes_view block) {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  BytesOutput directory{_directory};
  if (_blocks == 0) {
    directory.WriteV64((position + pad) / kPage);
  }
  directory.WriteV32(static_cast<uint32_t>((block.size() + kPage - 1) / kPage));
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  const bool small = _blocks == 0;
  const uint8_t dictless = small ? DictlessMask(_block) : 0;
  if (dictless == 0) {
    if (small) {
      _last.assign(KeyAt(_block, _block.ends.size() - 1));
      Seal();
    } else {
      Cut();
    }
  }
  auto& out = *_out;
  const uint64_t body_offset = out.Position();
  const uint8_t inlined = _inline.empty() ? 0 : kFieldInline;
  if (dictless != 0) {
    out.WriteByte(kFieldDictless | inlined);
    out.WriteByte(dictless);
    out.WriteV64(_block.records.size());
    out.WriteData(_block.records.data(), _block.records.size());
  } else {
    out.WriteByte(static_cast<byte_type>(
      (small ? 0 : kFieldBlocks) | (_width != 0 ? kFieldFixed : 0) |
      (_fsst_used ? kFieldFsst : 0) | (props.ranges ? kFieldRanges : 0) |
      inlined));
    out.WriteV32(kRestartInterval);
    if (_width != 0) {
      out.WriteV32(_width);
    }
    WriteStr(out, _min);
    WriteStr(out, _last);
    if (_fsst_used) {
      WriteStr(out, _fsst_table);
    }
    if (small) {
      out.WriteV64(_page.size());
      out.WriteData(_page.data(), _page.size());
    } else {
      out.WriteV32(_blocks);
      out.WriteData(_directory.data(), _directory.size());
      out.WriteV64(_separators.size());
      out.WriteData(_separators.data(), _separators.size());
    }
    if (props.ranges) {
      _ranges.Write(_region,
                    CommonPrefixLength(bytes_view{_min}, bytes_view{_last}));
      out.WriteV64(_region.size());
      out.WriteData(_region.data(), _region.size());
    }
  }
  if (inlined != 0) {
    out.WriteV64(_inline.size());
    out.WriteData(_inline.data(), _inline.size());
  }

  _idx->AddTermDictEntry(id, TermDictMeta{
                               .features = props.index_features,
                               .term_count = _terms,
                               .doc_count = stats.docs_count,
                               .total_doc_freq = total_doc_freq,
                               .total_term_freq = total_term_freq,
                               .has_score_bounds = stats.has_score_bounds,
                               .body_offset = body_offset,
                               .norm = props.norm,
                             });
}

void FieldWriter::Reset() noexcept {
  Clear(_block);
  _ranges.Clear();
  _fsst.reset();
  _fsst_table.clear();
  _fsst_used = false;
  _inline.clear();
  _directory.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _min.clear();
  _terms = 0;
  _blocks = 0;
}

void FieldWriter::end() {
  _pw.End();
  _idx = nullptr;
  _out = nullptr;
}

TermReaderImpl::TermReaderImpl(const FieldReader& owner, field_id id,
                               const TermDictMeta& meta, IndexInput& in)
  : _owner{&owner},
    _meta{id, meta.features},
    _terms{meta.term_count},
    _docs{meta.doc_count},
    _has_score_bounds{meta.has_score_bounds} {
  _meta.norm = meta.norm;
  const auto total =
    IndexFeatures::None != (meta.features & IndexFeatures::Freq)
      ? meta.total_term_freq
      : meta.total_doc_freq;
  SDB_ENSURE(total <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", total,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(total);

  in.Seek(meta.body_offset);
  const auto flags = in.ReadByte();
  uint64_t bytes = 0;
  if ((flags & kFieldDictless) != 0) {
    const auto mask = in.ReadByte();
    bstring records(in.ReadV64(), 0);
    in.ReadData(records.data(), records.size());
    BuildDictless(mask, records, _small_copy, _min, _max);
    _small = _small_copy.data();
    _blocks = 1;
  } else {
    bytes = ReadDictionary(in, flags);
  }
  if ((flags & kFieldInline) != 0) {
    bytes += MapInline(in);
  }
  if (bytes != 0) {
    owner.ResourceManager().Increase(bytes);
    _root_bytes = bytes;
  }
}

uint64_t TermReaderImpl::ReadDictionary(IndexInput& in, uint8_t flags) {
  if (const auto interval = in.ReadV32(); interval != kRestartInterval) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("term dictionary: restart interval ", interval,
                            " of field ", _meta.id));
  }
  if ((flags & kFieldFixed) != 0) {
    _width = in.ReadV32();
  }
  _min = ReadString<bstring>(in);
  _max = ReadString<bstring>(in);
  uint64_t bytes = 0;
  if ((flags & kFieldFsst) != 0) {
    const auto table = ReadString<bstring>(in);
    auto decoder = std::make_unique<duckdb_fsst_decoder_t>();
    const auto consumed =
      duckdb_fsst_import(decoder.get(), table.data(), table.size());
    if (consumed == DUCKDB_FSST_IMPORT_VERSION_MISMATCH ||
        consumed == DUCKDB_FSST_IMPORT_OUT_OF_BOUNDS) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INDEX_CORRUPTED),
        ERR_MSG("term dictionary: bad FSST table of field ", _meta.id));
    }
    _fsst = std::move(decoder);
    bytes += sizeof(duckdb_fsst_decoder_t);
  }
  if ((flags & kFieldBlocks) == 0) {
    const auto size = in.ReadV64();
    _small = Map(in, size, _small_copy);
    _blocks = 1;
  } else {
    _blocks = in.ReadV32();
    _pages.reserve(_blocks + 1);
    _pages.push_back(static_cast<uint32_t>(in.ReadV64()));
    for (uint32_t i = 0; i != _blocks; ++i) {
      _pages.push_back(_pages.back() + in.ReadV32());
    }
    const auto size = in.ReadV64();
    bstring copy;
    const auto* separators = Map(in, size, copy);
    _root = Root{DecodeSeparators(bytes_view{separators, size}, _blocks - 1),
                 _width != 0};
    bytes += _root.MemoryBytes() + _pages.capacity() * sizeof(uint32_t);
  }
  if ((flags & kFieldRanges) != 0) {
    _meta.ranges = true;
    bytes += ReadRanges(in);
  }
  return bytes;
}

uint64_t TermReaderImpl::ReadRanges(IndexInput& in) {
  const auto size = in.ReadV64();
  const auto* p = Map(in, size, _ranges_copy);
  const auto* const end = p + size;
  const auto corrupted = [&] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("term dictionary: bad range levels of field ", _meta.id));
  };
  const auto height = vread<uint32_t>(p);
  if (height == 0) {
    return _ranges_copy.size();
  }
  _range_prefix = vread<uint32_t>(p);
  if (height > kMaxRangeLevels || _width == 0 || _range_prefix > _width ||
      bytes_view{_min}.substr(0, _range_prefix) !=
        bytes_view{_max}.substr(0, _range_prefix)) {
    corrupted();
  }
  const uint64_t stride = _width - _range_prefix;
  _levels.reserve(height);
  for (uint32_t k = 0; k != height; ++k) {
    RangeLevel level{};
    level.count = static_cast<uint32_t>(vread<uint64_t>(p));
    const uint64_t firsts = k == 0 ? 0 : level.count * sizeof(uint32_t);
    const uint64_t offsets = (uint64_t{level.count} + 1) * sizeof(uint32_t);
    if (level.count < 2 || static_cast<uint64_t>(end - p) <
                             level.count * stride + firsts + offsets) {
      corrupted();
    }
    level.starts = p;
    p += level.count * stride;
    level.firsts = k == 0 ? nullptr : p;
    p += firsts;
    level.offsets = p;
    p += offsets;
    level.records = p;
    const uint32_t records =
      absl::little_endian::Load32(level.offsets + offsets - sizeof(uint32_t));
    if (static_cast<uint64_t>(end - p) < records) {
      corrupted();
    }
    p += records;
    _levels.push_back(level);
  }
  if (static_cast<uint64_t>(end - p) != kSlack) {
    corrupted();
  }
  return _ranges_copy.size() + _levels.capacity() * sizeof(RangeLevel);
}

uint64_t TermReaderImpl::MapInline(IndexInput& in) {
  _inline_size = in.ReadV64();
  _inline_offset = in.Position();
  if (_inline_size > in.Length() - _inline_offset) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("term dictionary: bad inline postings of field ", _meta.id));
  }
  if (!_small) {
    return 0;
  }
  _inline = Map(in, _inline_size, _inline_copy);
  return _inline_copy.size();
}

void TermReaderImpl::ReadInline(PostingMeta& meta, BlockBuffer& buffer) const {
  if (meta.inline_end > _inline_size) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("term dictionary: bad inline postings of field ", _meta.id));
  }
  const uint64_t at = meta.inline_end - meta.inline_size;
  if (_inline) {
    std::memcpy(meta.inline_data.data(), _inline + at, meta.inline_size);
    return;
  }
  const uint64_t offset = _inline_offset + at;
  if (const auto* base = _owner->Base()) {
    std::memcpy(meta.inline_data.data(), base + offset, meta.inline_size);
    return;
  }
  Reopened(buffer, _owner->Input())
    .ReadData(offset, meta.inline_data.data(), meta.inline_size);
}

void TermReaderImpl::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  constexpr size_t kRun = 16;
  constexpr uint64_t kRunBytes = 256;
  if (_terms == 0) {
    return;
  }
  const uint64_t runs = std::max<uint64_t>(1, bytes / kRunBytes);
  const uint32_t step =
    std::max<uint32_t>(1, static_cast<uint32_t>(_blocks / runs));
  Cursor cursor{*this};
  std::array<bstring, kRun> keys;
  std::array<bytes_view, kRun> run;
  uint64_t taken = 0;
  for (uint32_t block = 0; block < _blocks && taken < bytes; block += step) {
    cursor.Load(block);
    size_t n = 0;
    do {
      cursor.Next();
      keys[n].assign(cursor.Key());
      run[n] = keys[n];
      taken += keys[n].size();
      ++n;
    } while (n != kRun && !cursor.AtBlockEnd());
    visit({run.data(), n});
  }
}

TermReaderImpl::~TermReaderImpl() {
  _owner->ResourceManager().DecreaseChecked(_root_bytes);
}

const byte_type* TermReaderImpl::Block(size_t block,
                                       BlockBuffer& buffer) const {
  if (_small) {
    return _small;
  }
  const uint64_t offset = BlockOffset(block);
  if (const auto* base = _owner->Base()) {
    return base + offset;
  }
  auto& in = Reopened(buffer, _owner->Input());
  return MapAt(in, offset,
               std::min(BlockOffset(block + 1), in.Length()) - offset,
               buffer.copy);
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::IteratorUntil(bytes_view upper) const {
  auto it = memory::make_managed<TermIteratorImpl>(*this);
  if (!upper.empty() && !_small) {
    it->ReadaheadUntil(
      upper > _max ? _blocks : static_cast<uint32_t>(_root.Find(upper)) + 1);
  }
  return it;
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (_terms == 0 || term < _min || _max < term ||
      (_width != 0 && term.size() != _width)) {
    return {};
  }
  const size_t block = _small ? 0 : GetRoot().Find(term);
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer));
  SDB_ASSERT((view.kind == BlockKind::FixedStride) == (_width != 0));
  const PaddedKey padded{term};
  const auto entry = Find(view, padded.View(), _fsst.get());
  if (entry == kNoEntry) {
    return {};
  }
  return DecodeRecord(*this, view, entry, buffer);
}

void TermReaderImpl::ReadDocs(bytes_view term, Acceptor acceptor) const {
  const auto meta = Lookup(term);
  if (meta.docs_count == 0) {
    return;
  }
  if (meta.docs_count == 1) {
    acceptor(doc_limits::min() + meta.doc_delta);
    return;
  }
  auto docs_it = _owner->Postings().Postings(
    _meta.index_features, IndexFeatures::None, meta, _has_score_bounds);
  doc_id_t docs[doc_limits::kBlockSize];
  while (const auto n = docs_it->NextDocs(docs, nullptr)) {
    for (uint32_t i = 0; i != n; ++i) {
      SDB_ASSERT(doc_limits::valid(docs[i]));
      if (!acceptor(docs[i])) {
        return;
      }
    }
  }
}

std::unique_ptr<IndexInput> TermReaderImpl::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles TermReaderImpl::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* TermReaderImpl::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() ? &_freq : nullptr;
}

uint64_t FieldReader::CountMappedMemory() const {
  uint64_t bytes = _pr.CountMappedMemory();
  if (_in) {
    bytes += _in->CountMappedMemory();
  }
  return bytes;
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  const auto entries = state.idx->TermDicts();
  _in = state.idx->ReopenIn();
  if (!_in) {
    SDB_ENSURE(entries.empty(), "term dictionary: ", entries.size(),
               " fields but no `.idx` stream");
    return;
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& [id, meta] : entries) {
    features |= meta.features;
  }
  _pr.prepare(state, features);

  _base = _in->ReadStable(0, _in->Length());
  _ids.reserve(entries.size());
  _by_id.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    const auto& field = _fields.emplace_back(*this, id, meta, *_in);
    const auto [it, ok] = _by_id.emplace(id, &field);
    SDB_ENSURE(ok, "term dictionary: duplicate field ", id);
    _ids.push_back(id);
  }
  SDB_ENSURE(absl::c_is_sorted(_ids),
             "term dictionary: fields are not sorted by id");
  _in->Advise(IOAdvice::RANDOM);
}

const TermReader* FieldReader::field(field_id id) const {
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs::term_dict
