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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr size_t kFsstMinSamples = 64;
constexpr uint64_t kFsstMinMeanSuffix = 2;
constexpr uint64_t kFsstMaxMeanSuffix = 16;
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint32_t max,
                  uint64_t suffixes, uint64_t records) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  return 2 + VSize(count) + VSize(restarts) + VSize(suffixes) +
         VSize(head_skip) + (kVarRestart + kHeadBytes) * restarts +
         kRecordOffset * RecordGroups(count) +
         count * (2 * uint64_t{ColumnWidth(max)} + 2) + suffixes + records +
         kSlack;
}

uint64_t FixedBytes(uint64_t count, uint64_t records, uint64_t width,
                    uint64_t prefix) noexcept {
  const uint64_t stride = width - prefix;
  const uint64_t groups = RecordGroups(count);
  return 1 + VSize(count) + VSize(groups) + VSize(stride) + VSize(prefix) +
         prefix + (kRecordOffset + kHeadBytes) * groups + count * stride +
         records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

struct VarEntry {
  bytes_view suffix;
  size_t lcp;
  uint32_t head_skip;
  bool restart;
};

VarEntry VarEntryAt(const BlockBuilder& block, uint32_t index,
                    bytes_view key) noexcept {
  const bool restart = index % kRestartInterval == 0;
  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, index - 1), key);
  uint32_t head_skip = block.head_skip;
  if (restart) {
    head_skip = static_cast<uint32_t>(
      index == 0 ? key.size() : CommonPrefixLength(KeyAt(block, 0), key));
  }
  return {
    .suffix = key.substr(lcp),
    .lcp = lcp,
    .head_skip = head_skip,
    .restart = restart,
  };
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

uint32_t ColumnMax(const VarColumns& columns, size_t lcp, size_t len) noexcept {
  return std::max(
    {columns.max, static_cast<uint32_t>(lcp), static_cast<uint32_t>(len)});
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

}  // namespace

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          kMaxRecordBytes, " bytes"));
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
  _width = props.key_width <= kMaxStride ? props.key_width : 0;
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

  const auto consume = [&](bytes_view term, const PostingMeta& meta) {
    if (freq_exists) {
      sum_tfreq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    sum_dfreq += meta.docs_count;
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
        _pw.WritePostings(posting_batch[i], meta);
        consume(term_batch[i], meta);
      }
    }
  }

  EndField(reader.key(), props, sum_dfreq, sum_tfreq, first_inline);
  ResetField();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_width == 0) {
    const auto prefix = TermPrefix(term);
    if (_prefixes.empty() || _prefixes.back() != prefix) {
      _prefixes.push_back(prefix);
    }
  }
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
  const auto entry = VarEntryAt(block, index, key);
  block.head_skip = entry.head_skip;
  AppendEntry(block.raw, entry.restart, entry.lcp, entry.suffix);
  if (_fsst && _encode && block.all_coded) {
    AppendEntry(block.coded, entry.restart, entry.lcp,
                entry.restart ? entry.suffix : EncodeSuffix(entry.suffix));
  }
}

bool FieldWriter::AppendVarKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const bool converts = block.kind == BlockKind::FixedStride;
  if (converts) {
    for (uint32_t i = 0; i != count; ++i) {
      AppendVarColumns(i, KeyAt(block, i));
    }
  }
  const auto entry = VarEntryAt(block, count, key);
  const auto size_with = [&](const VarColumns& columns, size_t suffix_size) {
    return VarBytes(count + 1, entry.head_skip,
                    ColumnMax(columns, entry.lcp, suffix_size),
                    columns.suffixes.size() + suffix_size, records);
  };
  const uint64_t size = size_with(block.raw, entry.suffix.size());
  const bool encode = _fsst && _encode && block.all_coded;
  bytes_view coded;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  if (encode) {
    coded = entry.restart ? entry.suffix : EncodeSuffix(entry.suffix);
    coded_size = size_with(block.coded, coded.size());
  }
  if (count != 0 && std::min(size, coded_size) > kPage) {
    if (converts) {
      Clear(block.raw);
      Clear(block.coded);
      block.head_skip = 0;
    }
    return false;
  }
  block.kind = BlockKind::Var;
  AppendEntry(block.raw, entry.restart, entry.lcp, entry.suffix);
  if (encode) {
    AppendEntry(block.coded, entry.restart, entry.lcp, coded);
    block.coded_size = coded_size;
  } else if (block.all_coded) {
    block.all_coded = false;
    Clear(block.coded);
    block.coded_size = std::numeric_limits<uint64_t>::max();
  }
  block.size = size;
  block.head_skip = entry.head_skip;
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
  BytesOutput{_inline_starts}.WriteV64(_inline_start - _inline_last);
  _inline_last = _inline_start;
  _inline_start = _inline.size();
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
    _spans.push_back({.block = _blocks, .extra = pages - 1});
  }
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(FieldKey key, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq,
                           uint64_t first_inline) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  auto& head = _heads.emplace_back();
  head.key = key;
  head.features = props.index_features;
  head.term_count = _terms;
  head.doc_count = stats.docs_count;
  head.total_doc_freq = total_doc_freq;
  head.total_term_freq = total_term_freq;
  head.norm = props.norm;
  head.has_score_bounds = stats.has_score_bounds;
  head.ranges = props.ranges;
  head.ordinal = _ordinal - 1;
  head.width = _width;
  head.min = _min;
  if (_terms == 1) {
    _record.Clear();
    _pw.EncodeDetached(_record, _single, 0);
    head.record = _record.View();
    head.record_inline = _single.Inline();
    return;
  }
  head.max = bytes_view{_key}.substr(_prefix_size);
  head.first_block = _first_block;
  head.first_entry = _first_entry;
  head.first_record = _first_record;
  head.last_block = _blocks;
  head.last_entry = static_cast<uint32_t>(_block.ends.size() - 1);
  if (_first_block == _blocks) {
    head.inline_base = first_inline;
    head.inline_size = _inline.size() - first_inline;
  } else {
    auto out = std::back_inserter(head.prefixes);
    uint32_t last = 0;
    for (const auto prefix : _prefixes) {
      WriteVarint(prefix - last, out);
      last = prefix;
    }
  }
  if (props.ranges) {
    bstring region;
    const auto prefix =
      CommonPrefixLength(bytes_view{_min}, bytes_view{head.max});
    _ranges.Write(region, prefix, head.range_counts);
    if (!head.range_counts.empty()) {
      head.range_offset = _range_regions.size();
      head.range_size = region.size();
      head.range_prefix = static_cast<uint32_t>(prefix);
      _range_regions.append(region);
    }
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
  Clear(_block);
  _fsst.reset();
  _fsst_table.clear();
  _samples.clear();
  _sample_lengths.clear();
  _inline.clear();
  _spans.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _inline_starts.clear();
  _range_regions.clear();
  _heads.clear();
  _first_page = 0;
  _inline_start = 0;
  _inline_last = 0;
  _blocks = 0;
  _ordinal = 0;
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
      .separators = std::move(_separators),
      .fsst_table = std::move(_fsst_table),
      .inline_offset = out.Position(),
      .inline_size = _inline.size(),
      .ranges_size = _range_regions.size(),
      .fields = std::move(_heads),
      .inline_starts = _inline.empty() ? bstring{} : std::move(_inline_starts),
    };
    out.WriteData(_inline.data(), _inline.size());
    out.WriteData(_range_regions.data(), _range_regions.size());
    _idx->SetTerms(std::move(terms));
  }
  Reset();
  _idx = nullptr;
  _out = nullptr;
}

}  // namespace irs::term_dict
