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
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr uint8_t kFieldFixed = 1;
constexpr uint8_t kFieldRanges = 2;
constexpr uint8_t kFieldSingle = 4;
constexpr uint32_t kShortPrefix = 0xF0;
constexpr uint32_t kMediumPrefix = kShortPrefix + (0xFF - kShortPrefix) * 256;
constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr size_t kFsstMinSamples = 64;
constexpr uint64_t kFsstMinMeanSuffix = 2;
constexpr uint64_t kFsstMaxMeanSuffix = 16;
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr byte_type kZeros[kPage]{};
constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

void AppendFieldPrefix(bstring& out, uint32_t ordinal) {
  if (ordinal < kShortPrefix) {
    out.push_back(static_cast<byte_type>(ordinal));
    return;
  }
  if (ordinal < kMediumPrefix) {
    const uint32_t rest = ordinal - kShortPrefix;
    out.push_back(static_cast<byte_type>(kShortPrefix + rest / 256));
    out.push_back(static_cast<byte_type>(rest % 256));
    return;
  }
  out.push_back(0xFF);
  byte_type rest[sizeof(uint32_t)];
  absl::big_endian::Store32(rest, ordinal - kMediumPrefix);
  out.append(rest, sizeof(rest));
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint32_t max,
                  uint64_t suffixes, uint64_t records) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  return 2 + VSize(count) + VSize(restarts) + VSize(suffixes) +
         VSize(records) + VSize(head_skip) +
         (kVarRestart + kHeadBytes) * restarts +
         kRecordOffset * RecordGroups(count) +
         count * (2 * uint64_t{ColumnWidth(max)} + 2) + suffixes + records +
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
  columns.lcps.clear();
  columns.lens.clear();
  columns.suffixes.clear();
  columns.restarts.clear();
  columns.max = 0;
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
  block.all_coded = true;
}

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

uint32_t ColumnMax(const VarColumns& columns, size_t lcp,
                   size_t len) noexcept {
  return std::max({columns.max, static_cast<uint32_t>(lcp),
                   static_cast<uint32_t>(len)});
}

void AppendEntry(VarColumns& columns, bool restart, size_t lcp,
                 bytes_view suffix) {
  if (restart) {
    columns.restarts.push_back(static_cast<uint16_t>(columns.suffixes.size()));
  }
  columns.max = ColumnMax(columns, lcp, suffix.size());
  columns.lcps.push_back(static_cast<uint32_t>(lcp));
  columns.lens.push_back(static_cast<uint32_t>(suffix.size()));
  columns.suffixes.append(suffix);
}

void WriteColumn(BytesOutput& out, std::span<const uint32_t> values,
                 uint32_t width) {
  for (const auto value : values) {
    switch (width) {
      case 1:
        out.WriteByte(static_cast<byte_type>(value));
        break;
      case 2:
        out.WriteU16(static_cast<uint16_t>(value));
        break;
      default:
        out.WriteU32(value);
        break;
    }
  }
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
    const uint32_t width = ColumnWidth(columns.max);
    out.WriteV32(static_cast<uint32_t>(columns.restarts.size()));
    out.WriteByte(static_cast<byte_type>(width));
    out.WriteV64(columns.suffixes.size());
    out.WriteV64(block.records.size());
    out.WriteV32(block.head_skip);
    for (const auto restart : columns.restarts) {
      out.WriteU16(restart);
    }
    WriteHeads(out, block, kRestartInterval, block.head_skip);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    WriteColumn(out, columns.lcps, width);
    WriteColumn(out, columns.lens, width);
    for (uint32_t i = 0; i != count; ++i) {
      const auto key = KeyAt(block, i);
      out.WriteByte(key.empty() ? 0 : key.back());
    }
    for (uint32_t i = 0; i != count; ++i) {
      const auto lcp = columns.lcps[i];
      out.WriteByte(lcp == 0 ? 0 : KeyAt(block, i)[lcp - 1]);
    }
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
                         uint32_t block, uint32_t entry) {
  uint32_t count = 0;
  auto cursor = FieldRecords(field, view, block, entry, count);
  PostingMeta meta;
  DecodeRecords(cursor, count, field.meta().index_features, meta);
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
        _cursor.First();
        break;
      case Position::At:
        if (!_cursor.Advance()) {
          return End();
        }
        break;
    }
    _position = Position::At;
    _term.value = _cursor.Term();
    return true;
  }

  SeekResult seek_ge(bytes_view term) final {
    const auto result = _cursor.SeekGe(term);
    if (result == SeekResult::End) {
      End();
      return result;
    }
    _position = Position::At;
    _term.value = _cursor.Term();
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
  auto& block = _block;
  if (!_fsst || block.kind != BlockKind::Var || block.ends.empty()) {
    return;
  }
  block.all_coded = true;
  Clear(block.raw);
  Clear(block.coded);
  const auto count = static_cast<uint32_t>(block.ends.size());
  for (uint32_t i = 0; i != count; ++i) {
    AppendVarColumns(i, KeyAt(block, i));
  }
  block.coded_size =
    VarBytes(count, block.head_skip, block.coded.max,
             block.coded.suffixes.size(), block.records.size());
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
  Reset();
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
    SampleFsst(reader);
  }
  if (props.ranges && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw);
  }

  const uint64_t first_inline = _inline.size();
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

  EndField(reader.id(), props, sum_dfreq, sum_tfreq, first_inline);
  ResetField();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_terms++ == 0) {
    _min.assign(term);
    _single = meta;
    _key.clear();
    AppendFieldPrefix(_key, _ordinal++);
    _prefix_size = static_cast<uint32_t>(_key.size());
    return;
  }
  if (_terms == 2) {
    Commit(_min, _single);
  }
  Commit(term, meta);
}

void FieldWriter::Commit(bytes_view term, const PostingMeta& meta) {
  _key.resize(_prefix_size);
  _key.append(term);
  const bytes_view key{_key};
  if (!Append(key, meta)) {
    Cut();
    Append(key, meta);
  }
  _inline.append(meta.Inline());
  if (_first_block == kNoBlock) {
    _first_block = _blocks;
    _first_entry = static_cast<uint32_t>(_block.ends.size() - 1);
    _first_record =
      static_cast<uint32_t>(_block.records.size() - _record.View().size());
  }
}

bool FieldWriter::Append(bytes_view key, const PostingMeta& meta) {
  auto& block = _block;
  const bool record_start = block.ends.size() % kRecordInterval == 0;
  if (record_start) {
    _pw.BeginBlock();
  }
  _record.Clear();
  _pw.EncodeDetached(_record, meta, _inline.size());
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  const bool fixed =
    _width != 0 &&
    (block.ends.empty() ||
     (block.kind == BlockKind::FixedStride && block.width == key.size()));
  if (!(fixed ? AppendFixedKey(key, records) : AppendVarKey(key, records))) {
    return false;
  }
  if (record_start) {
    block.record_starts.push_back(static_cast<uint16_t>(block.records.size()));
  }
  block.records.append(record);
  block.keys.append(key);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  return true;
}

bool FieldWriter::AppendFixedKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const auto width = static_cast<uint32_t>(key.size());
  const auto prefix =
    count == 0
      ? width
      : static_cast<uint32_t>(CommonPrefixLength(KeyAt(block, 0), key));
  const uint64_t size = FixedBytes(count + 1, records, width, prefix);
  if (count != 0 && size > kPage) {
    return false;
  }
  block.size = size;
  block.width = width;
  block.prefix = prefix;
  block.kind = BlockKind::FixedStride;
  return true;
}

void FieldWriter::AppendVarColumns(uint32_t index, bytes_view key) {
  auto& block = _block;
  const bool restart = index % kRestartInterval == 0;
  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, index - 1), key);
  const auto suffix = key.substr(lcp);
  if (restart) {
    block.head_skip = static_cast<uint32_t>(
      index == 0 ? key.size() : CommonPrefixLength(KeyAt(block, 0), key));
  }
  AppendEntry(block.raw, restart, lcp, suffix);
  if (_fsst && _encode && block.all_coded) {
    AppendEntry(block.coded, restart, lcp,
                restart ? suffix : EncodeSuffix(suffix));
  }
}

bool FieldWriter::AppendVarKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  if (block.kind == BlockKind::FixedStride) {
    const auto fixed_size = block.size;
    for (uint32_t i = 0; i != count; ++i) {
      AppendVarColumns(i, KeyAt(block, i));
    }
    block.kind = BlockKind::Var;
    if (!AppendVarKey(key, records)) {
      Clear(block.raw);
      Clear(block.coded);
      block.coded_size = std::numeric_limits<uint64_t>::max();
      block.head_skip = 0;
      block.size = fixed_size;
      block.kind = BlockKind::FixedStride;
      return false;
    }
    return true;
  }
  const bool restart = count % kRestartInterval == 0;
  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, count - 1), key);
  const auto suffix = key.substr(lcp);
  uint32_t head_skip = block.head_skip;
  if (restart) {
    head_skip = static_cast<uint32_t>(
      count == 0 ? key.size() : CommonPrefixLength(KeyAt(block, 0), key));
  }
  const auto size_with = [&](const VarColumns& columns, size_t suffix_size) {
    return VarBytes(count + 1, head_skip, ColumnMax(columns, lcp, suffix_size),
                    columns.suffixes.size() + suffix_size, records);
  };
  const uint64_t size = size_with(block.raw, suffix.size());
  const bool encode = _fsst && _encode && block.all_coded;
  bytes_view coded;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  if (encode) {
    coded = restart ? suffix : EncodeSuffix(suffix);
    coded_size = size_with(block.coded, coded.size());
  }
  if (count != 0 && std::min(size, coded_size) > kPage) {
    return false;
  }
  AppendEntry(block.raw, restart, lcp, suffix);
  if (encode) {
    AppendEntry(block.coded, restart, lcp, coded);
    block.coded_size = coded_size;
  } else if (block.all_coded) {
    block.all_coded = false;
    Clear(block.coded);
    block.coded_size = std::numeric_limits<uint64_t>::max();
  }
  block.size = size;
  block.head_skip = head_skip;
  return true;
}

void FieldWriter::Seal() {
  if (_block.kind == BlockKind::Var && _block.coded_size < _block.size) {
    _block.kind = BlockKind::VarFsst;
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
  if (_blocks == 0) {
    _first_page = (position + pad) / kPage;
  }
  const auto pages = static_cast<uint32_t>((block.size() + kPage - 1) / kPage);
  if (pages != 1) {
    BytesOutput directory{_directory};
    directory.WriteV32(_blocks - _last_span);
    directory.WriteV32(pages - 1);
    _last_span = _blocks;
    ++_spans;
  }
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq,
                           uint64_t first_inline) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  const uint64_t offset = _fields.size();
  BytesOutput out{_fields};
  const bool single = _terms == 1;
  out.WriteByte(static_cast<byte_type>((_width != 0 ? kFieldFixed : 0) |
                                       (props.ranges ? kFieldRanges : 0) |
                                       (single ? kFieldSingle : 0)));
  out.WriteV32(_ordinal - 1);
  if (_width != 0) {
    out.WriteV32(_width);
  }
  WriteStr(out, _min);
  if (single) {
    _record.Clear();
    _pw.EncodeDetached(_record, _single, 0);
    WriteStr(out, _record.View());
    WriteStr(out, _single.Inline());
  } else {
    const bytes_view max = bytes_view{_key}.substr(_prefix_size);
    WriteStr(out, max);
    out.WriteV32(_first_block);
    out.WriteV32(_first_entry);
    out.WriteV32(_first_record);
    out.WriteV32(_blocks);
    out.WriteV32(static_cast<uint32_t>(_block.ends.size() - 1));
    if (_first_block == _blocks) {
      out.WriteV64(first_inline);
      out.WriteV64(_inline.size() - first_inline);
    }
    if (props.ranges) {
      bstring region;
      _ranges.Write(region, CommonPrefixLength(bytes_view{_min}, max));
      out.WriteV64(_range_regions.size());
      out.WriteV64(region.size());
      _range_regions.append(region);
    }
  }

  _entries.push_back({
    .id = id,
    .meta =
      {
        .features = props.index_features,
        .term_count = _terms,
        .doc_count = stats.docs_count,
        .total_doc_freq = total_doc_freq,
        .total_term_freq = total_term_freq,
        .has_score_bounds = stats.has_score_bounds,
        .body_offset = offset,
        .norm = props.norm,
      },
  });
}

void FieldWriter::ResetField() noexcept {
  _ranges.Clear();
  _min.clear();
  _terms = 0;
  _first_block = kNoBlock;
  _encode = true;
}

void FieldWriter::Reset() noexcept {
  ResetField();
  Clear(_block);
  _fsst.reset();
  _fsst_table.clear();
  _samples.clear();
  _sample_lengths.clear();
  _inline.clear();
  _directory.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _fields.clear();
  _range_regions.clear();
  _entries.clear();
  _first_page = 0;
  _blocks = 0;
  _spans = 0;
  _last_span = 0;
  _ordinal = 0;
}

void FieldWriter::end() {
  Cut();
  _pw.End();
  if (!_entries.empty()) {
    auto& out = *_out;
    const uint64_t body = out.Position();
    out.WriteV32(kRestartInterval);
    out.WriteV32(_blocks);
    if (_blocks != 0) {
      out.WriteV64(_first_page);
      out.WriteV32(_spans);
      out.WriteData(_directory.data(), _directory.size());
    }
    out.WriteV64(_separators.size());
    out.WriteData(_separators.data(), _separators.size());
    WriteStr(out, bytes_view{_fsst_table});
    out.WriteV64(_inline.size());
    out.WriteV64(_range_regions.size());
    const uint64_t fields = out.Position();
    out.WriteData(_fields.data(), _fields.size());
    const uint64_t head = out.Position() - body;
    out.WriteData(_inline.data(), _inline.size());
    out.WriteData(_range_regions.data(), _range_regions.size());
    for (auto& entry : _entries) {
      entry.meta.body_offset += fields;
      _idx->AddTermDictEntry(entry.id, entry.meta);
    }
    _idx->SetTermsBody(body, head);
  }
  Reset();
  _idx = nullptr;
  _out = nullptr;
}

TermReaderImpl::TermReaderImpl(FieldReader& owner, field_id id,
                               const TermDictMeta& meta, IndexInput& in,
                               const Separators& separators)
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

  const auto corrupted = [&] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("term dictionary: bad record of field ", id));
  };
  in.Seek(meta.body_offset);
  const auto flags = in.ReadByte();
  bstring prefix;
  AppendFieldPrefix(prefix, in.ReadV32());
  _prefix_size = static_cast<uint32_t>(prefix.size());
  uint64_t bytes = 0;
  if ((flags & kFieldFixed) != 0) {
    _width = in.ReadV32();
  }
  _meta.ranges = (flags & kFieldRanges) != 0;
  _min_key.assign(prefix);
  _min_key.append(ReadString<bstring>(in));
  if ((flags & kFieldSingle) != 0) {
    _max_key = _min_key;
    RecordBytes record{};
    const auto size = in.ReadV32();
    if (size > record.bytes.size()) {
      corrupted();
    }
    in.ReadData(record.bytes.data(), size);
    if (PostingsReader::DecodeDetached(record.bytes.data(), meta.features,
                                       _single) != size ||
        in.ReadV32() != _single.inline_size) {
      corrupted();
    }
    in.ReadData(_single.inline_data.data(), _single.inline_size);
  } else {
    _max_key.assign(prefix);
    _max_key.append(ReadString<bstring>(in));
    _first_block = in.ReadV32();
    _first_entry = in.ReadV32();
    _first_record = in.ReadV32();
    const auto last_block = in.ReadV32();
    _last_entry = in.ReadV32();
    if (last_block < _first_block || last_block >= owner.BlockCount()) {
      corrupted();
    }
    _blocks = last_block - _first_block + 1;
    uint64_t inline_size = 0;
    if (_blocks > 1) {
      std::vector<bytes_view> slice;
      slice.reserve(_blocks - 1);
      for (uint32_t block = _first_block; block != last_block; ++block) {
        slice.push_back(SeparatorAt(separators, block));
      }
      _root = Root{MakeSeparators(slice), _width != 0};
      bytes += _root.MemoryBytes();
    } else {
      _inline_base = in.ReadV64();
      inline_size = in.ReadV64();
      if (_inline_base > owner.InlineEnd() - owner.InlineBegin() ||
          inline_size >
            owner.InlineEnd() - owner.InlineBegin() - _inline_base) {
        corrupted();
      }
    }
    if (_meta.ranges) {
      bytes += ReadRanges(owner, in);
    }
    if (_blocks == 1) {
      _small = owner.CopyBlock(_first_block, in.Length());
      if (inline_size != 0) {
        _small_inline =
          owner.Copy(owner.InlineBegin() + _inline_base, inline_size);
      }
    }
  }
  if (bytes != 0) {
    owner.ResourceManager().Increase(bytes);
    _root_bytes = bytes;
  }
}

uint64_t TermReaderImpl::ReadRanges(FieldReader& owner, IndexInput& in) {
  const auto corrupted = [&] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("term dictionary: bad range levels of field ", _meta.id));
  };
  const auto offset = in.ReadV64();
  const auto size = in.ReadV64();
  if (size == 0 || offset > owner.RangesSize() ||
      size > owner.RangesSize() - offset) {
    corrupted();
  }
  const auto* p = owner.Load(in, owner.RangesBegin() + offset, size).data();
  const auto* const end = p + size;
  const auto height = vread<uint32_t>(p);
  if (height == 0) {
    return 0;
  }
  _range_prefix = vread<uint32_t>(p);
  if (height > kMaxRangeLevels || _width == 0 || _range_prefix > _width ||
      min().substr(0, _range_prefix) != max().substr(0, _range_prefix)) {
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
  return _levels.capacity() * sizeof(RangeLevel);
}

void TermReaderImpl::ReadInline(PostingMeta& meta, BlockBuffer& buffer) const {
  const uint64_t begin = Small() ? _inline_base : 0;
  const uint64_t end =
    Small() ? _inline_base + _small_inline.size() : InlineEnd() - InlineBegin();
  if (meta.inline_end < begin + meta.inline_size || meta.inline_end > end) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("term dictionary: bad inline postings of field ", _meta.id));
  }
  const uint64_t at = meta.inline_end - meta.inline_size;
  if (Small()) {
    std::memcpy(meta.inline_data.data(), _small_inline.data() + (at - begin),
                meta.inline_size);
    return;
  }
  const uint64_t offset = InlineBegin() + at;
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
  if (Single()) {
    const auto term = min();
    visit({&term, 1});
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
    if (block == 0) {
      cursor.First();
    } else {
      cursor.Load(block);
      cursor.Next();
    }
    size_t n = 0;
    for (;;) {
      keys[n].assign(cursor.Term());
      run[n] = keys[n];
      taken += keys[n].size();
      if (++n == kRun || cursor.AtBlockEnd()) {
        break;
      }
      cursor.Next();
    }
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
  if (Single()) {
    return memory::make_managed<SingleIterator<false>>(*this);
  }
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::IteratorUntil(bytes_view upper) const {
  if (Single()) {
    return iterator();
  }
  auto it = memory::make_managed<TermIteratorImpl>(*this);
  if (!upper.empty() && !Small()) {
    PaddedKey key;
    key.Assign(KeyPrefix(), upper);
    it->ReadaheadUntil(upper > max()
                         ? _blocks
                         : static_cast<uint32_t>(_root.Find(key.Key())) + 1);
  }
  return it;
}

bool TermReaderImpl::Excludes(bytes_view term) const noexcept {
  return _terms == 0 || term < min() || max() < term ||
         (_width != 0 && term.size() != _width);
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (Excludes(term)) {
    return {};
  }
  if (Single()) {
    return _single;
  }
  PaddedKey padded;
  const auto probe = padded.Assign(KeyPrefix(), term);
  const size_t block = Small() ? 0 : _root.Find(padded.Key());
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer));
  const auto entry = Find(view, probe, Fsst());
  if (entry == kNoEntry) {
    return {};
  }
  auto meta = DecodeRecord(*this, view, static_cast<uint32_t>(block), entry);
  if (meta.inline_size != 0) {
    ReadInline(meta, buffer);
  }
  return meta;
}

void TermReaderImpl::LookupBatch(std::span<const bytes_view> terms,
                                 std::span<PostingMeta> out) const {
  SDB_ASSERT(terms.size() == out.size());
  if (Single()) {
    for (size_t i = 0; i != terms.size(); ++i) {
      out[i] = Excludes(terms[i]) ? PostingMeta{} : _single;
    }
    return;
  }
  std::vector<uint32_t> blocks(terms.size(), kNoBlock);
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  PaddedKey padded;
  uint32_t last = kNoBlock;
  bytes_view last_term;
  for (size_t i = 0; i != terms.size(); ++i) {
    const auto term = terms[i];
    if (Excludes(term)) {
      continue;
    }
    if (Small()) {
      blocks[i] = 0;
      continue;
    }
    padded.Assign(KeyPrefix(), term);
    const auto key = padded.Key();
    auto block = last;
    if (block == kNoBlock || term < last_term) {
      block = static_cast<uint32_t>(_root.Find(key));
    } else if (block + 1 < _blocks && _root.NotAfter(block + 1, key)) {
      block = static_cast<uint32_t>(_root.FindFrom(block + 1, key));
    }
    blocks[i] = last = block;
    last_term = term;
    ranges.emplace_back(BlockOffset(block), BlockOffset(block + 1));
  }
  if (ranges.size() > 1) {
    PrefetchRanges(_owner->Input(), ranges);
  }
  ranges.clear();

  BlockBuffer buffer;
  BlockView view{};
  uint32_t parsed = kNoBlock;
  for (size_t i = 0; i != terms.size(); ++i) {
    out[i] = {};
    if (blocks[i] == kNoBlock) {
      continue;
    }
    if (blocks[i] != parsed) {
      parsed = blocks[i];
      view = Parse(Block(parsed, buffer));
    }
    const auto entry = Find(view, padded.Assign(KeyPrefix(), terms[i]), Fsst());
    if (entry == kNoEntry) {
      continue;
    }
    const auto& meta = out[i] = DecodeRecord(*this, view, parsed, entry);
    if (meta.inline_size != 0 && !Small()) {
      const uint64_t end = InlineBegin() + meta.inline_end;
      ranges.emplace_back(end - meta.inline_size, end);
    }
  }
  if (ranges.size() > 1) {
    PrefetchRanges(_owner->Input(), ranges);
  }

  for (auto& meta : out) {
    if (meta.inline_size != 0) {
      ReadInline(meta, buffer);
    }
  }
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

FieldReader::~FieldReader() { _rm->DecreaseChecked(_bytes); }

uint64_t FieldReader::CountMappedMemory() const {
  uint64_t bytes = _pr.CountMappedMemory();
  if (_in) {
    bytes += _in->CountMappedMemory();
  }
  return bytes;
}

bytes_view FieldReader::Load(IndexInput& in, uint64_t offset, uint64_t size) {
  if (_base) {
    return {_base + offset, size};
  }
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    _rm->Increase(size);
    _bytes += size;
    copy.resize(size);
    in.ReadData(offset, copy.data(), size);
  }
  SDB_ASSERT(copy.size() == size);
  return copy;
}

bytes_view FieldReader::Copy(uint64_t offset, uint64_t size) {
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    _rm->Increase(size);
    _bytes += size;
    copy.resize(size);
    _pending.push_back({.offset = offset, .size = size, .data = copy.data()});
  }
  SDB_ASSERT(copy.size() == size);
  return copy;
}

const byte_type* FieldReader::CopyBlock(uint32_t block, uint64_t length) {
  const uint64_t offset = PageOffset(block);
  return Copy(offset, std::min(PageOffset(block + 1), length) - offset).data();
}

void FieldReader::FillCopies() {
  absl::c_sort(_pending, [](const PendingCopy& lhs, const PendingCopy& rhs) {
    return lhs.offset < rhs.offset;
  });
  for (size_t i = 0; i != _pending.size();) {
    const uint64_t begin = _pending[i].offset;
    uint64_t end = begin + _pending[i].size;
    while (++i != _pending.size() && _pending[i].offset <= end) {
      end = std::max(end, _pending[i].offset + _pending[i].size);
    }
    _in->Prefetch(begin, end - begin);
  }
  for (const auto& copy : _pending) {
    _in->ReadData(copy.offset, copy.data, copy.size);
  }
  _pending.clear();
  _pending.shrink_to_fit();
}

Separators FieldReader::ReadBody(IndexInput& in, uint64_t offset,
                                 uint64_t head) {
  const auto corrupted = [](std::string_view what) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("term dictionary: bad ", what));
  };
  if (head == 0 || offset > in.Length() || head > in.Length() - offset) {
    corrupted("head extent");
  }
  in.Seek(offset);
  if (in.ReadV32() != kRestartInterval) {
    corrupted("restart interval");
  }
  const auto blocks = in.ReadV32();
  _block_count = blocks;
  if (blocks != 0) {
    _first_page = in.ReadV64();
    const auto spans = in.ReadV32();
    if (spans > blocks) {
      corrupted("block directory");
    }
    _spans.reserve(spans);
    uint64_t block = 0;
    uint64_t extra = 0;
    for (uint32_t i = 0; i != spans; ++i) {
      const auto delta = in.ReadV32();
      if (i != 0 && delta == 0) {
        corrupted("block directory");
      }
      block += delta;
      extra += in.ReadV32();
      if (block >= blocks || extra > std::numeric_limits<uint32_t>::max()) {
        corrupted("block directory");
      }
      _spans.push_back({
        .block = static_cast<uint32_t>(block),
        .extra = static_cast<uint32_t>(extra),
      });
    }
    if (_first_page > offset / kPage ||
        PageOffset(blocks - 1) >= offset) {
      corrupted("block directory");
    }
  }
  const auto size = in.ReadV64();
  bstring copy;
  const auto* encoded = Map(in, size, copy);
  auto separators =
    blocks == 0 ? Separators{} : DecodeSeparators({encoded, size}, blocks - 1);
  if (const auto table = ReadString<bstring>(in); !table.empty()) {
    auto decoder = std::make_unique<duckdb_fsst_decoder_t>();
    const auto consumed =
      duckdb_fsst_import(decoder.get(), table.data(), table.size());
    if (consumed == DUCKDB_FSST_IMPORT_VERSION_MISMATCH ||
        consumed == DUCKDB_FSST_IMPORT_OUT_OF_BOUNDS) {
      corrupted("FSST table");
    }
    _fsst = std::move(decoder);
  }
  _inline_size = in.ReadV64();
  _ranges_size = in.ReadV64();
  _inline_offset = offset + head;
  if (in.Position() > _inline_offset ||
      _inline_size > in.Length() - _inline_offset ||
      _ranges_size > in.Length() - _inline_offset - _inline_size) {
    corrupted("inline postings");
  }
  _bytes = _spans.capacity() * sizeof(SpanningBlock) +
           (_fsst ? sizeof(duckdb_fsst_decoder_t) : 0);
  if (_bytes != 0) {
    _rm->Increase(_bytes);
  }
  return separators;
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
  const uint64_t body = state.idx->TermsBody();
  const uint64_t head = state.idx->TermsHead();
  Separators separators;
  if (!entries.empty()) {
    _in->Prefetch(body, head);
    separators = ReadBody(*_in, body, head);
  }
  _ids.reserve(entries.size());
  _by_id.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    const auto& field = _fields.emplace_back(*this, id, meta, *_in, separators);
    const auto [it, ok] = _by_id.emplace(id, &field);
    SDB_ENSURE(ok, "term dictionary: duplicate field ", id);
    _ids.push_back(id);
  }
  SDB_ENSURE(absl::c_is_sorted(_ids),
             "term dictionary: fields are not sorted by id");
  FillCopies();
  if (!entries.empty()) {
    _in->Evict(body, head);
  }
}

const TermReader* FieldReader::field(field_id id) const {
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs::term_dict
