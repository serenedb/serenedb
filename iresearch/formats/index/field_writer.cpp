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
#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <iterator>
#include <limits>
#include <ranges>
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
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

struct Packing {
  uint32_t base;
  uint32_t escaped;
};

struct Shape {
  uint32_t lcp_width;
  uint32_t len_width;
  Packing packing;
};

uint64_t PackedBytes(uint64_t count, const Shape& shape) noexcept {
  return VSize(shape.packing.base) + VSize(shape.packing.escaped) + count +
         shape.packing.escaped *
           (kEscapedEntry + uint64_t{shape.lcp_width} + shape.len_width);
}

uint64_t ColumnBytes(uint64_t count, const Shape& shape) noexcept {
  return count * (uint64_t{shape.lcp_width} + shape.len_width);
}

bool Packed(uint64_t count, const Shape& shape) noexcept {
  return PackedBytes(count, shape) <= ColumnBytes(count, shape);
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint64_t suffixes,
                  uint64_t records, const Shape& shape) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  return 4 + VSize(count) + VSize(restarts) + VSize(suffixes) +
         VSize(head_skip) + (kVarRestart + kHeadBytes) * restarts +
         kRecordOffset * RecordGroups(count) +
         std::min(PackedBytes(count, shape), ColumnBytes(count, shape)) +
         suffixes + records + kSlack;
}

uint32_t Base(uint32_t base) noexcept { return base == kNoBase ? 0 : base; }

uint32_t MisfitMask(uint32_t lcp, uint32_t len, uint32_t base) noexcept {
  constexpr uint32_t kAll = (1U << kLenSplits) - 1;
  if (lcp < base) {
    return kAll;
  }
  const uint32_t delta = lcp - base;
  const auto low = static_cast<uint32_t>(std::bit_width(len));
  const auto delta_bits = static_cast<uint32_t>(std::bit_width(delta));
  if (low > kHeaderBits || delta_bits > kHeaderBits - low) {
    return kAll;
  }
  const uint32_t high = kHeaderBits - delta_bits;
  uint32_t fits = ((2U << high) - 1) & ~((1U << low) - 1);
  if (len == (1U << low) - 1 && delta == (kEscapedHeader >> low)) {
    fits &= ~(1U << low);
  }
  SDB_ASSERT(absl::c_all_of(
    std::views::iota(uint32_t{0}, kLenSplits), [&](uint32_t len_bits) {
      return ((fits >> len_bits & 1) != 0) == Packs(delta, len, len_bits);
    }));
  return kAll & ~fits;
}

void Count(Misfits& misfits, uint32_t lcp, uint32_t len,
           uint32_t base) noexcept {
  const uint32_t mask = MisfitMask(lcp, len, base);
  for (uint32_t len_bits = 0; len_bits != kLenSplits; ++len_bits) {
    misfits[len_bits] += mask >> len_bits & 1;
  }
}

struct StoredHeader {
  uint32_t lcp;
  uint32_t len;
};

StoredHeader StoredAt(const VarColumns& columns, size_t entry,
                      uint32_t base) noexcept {
  if (entry == 0) {
    SDB_ASSERT(columns.lcps[0] == 0 && base <= columns.lens[0]);
    return {.lcp = base, .len = columns.lens[0] - base};
  }
  return {.lcp = columns.lcps[entry], .len = columns.lens[entry]};
}

Misfits Recount(const VarColumns& columns, uint32_t base) noexcept {
  Misfits misfits{};
  for (size_t i = 0; i != columns.lcps.size(); ++i) {
    const auto stored = StoredAt(columns, i, base);
    Count(misfits, stored.lcp, stored.len, base);
  }
  return misfits;
}

uint32_t BaseWith(const VarColumns& columns, uint32_t lcp) noexcept {
  return columns.lcps.empty() ? columns.base : std::min(columns.base, lcp);
}

Misfits MisfitsAt(const VarColumns& columns, uint32_t base) noexcept {
  return base == columns.base ? columns.misfits : Recount(columns, Base(base));
}

uint32_t LenBits(const Misfits& misfits) noexcept {
  return static_cast<uint32_t>(absl::c_min_element(misfits) - misfits.begin());
}

Packing PackingOf(const VarColumns& columns) noexcept {
  return {
    .base = Base(columns.base),
    .escaped = columns.misfits[LenBits(columns.misfits)],
  };
}

Packing PackingWith(const VarColumns& columns, uint32_t lcp,
                    uint32_t len) noexcept {
  const uint32_t base = BaseWith(columns, lcp);
  auto misfits = MisfitsAt(columns, base);
  Count(misfits, lcp, len, Base(base));
  return {.base = Base(base), .escaped = misfits[LenBits(misfits)]};
}

Packing PackingBound(const VarColumns& columns, uint32_t lcp,
                     uint32_t deferred) noexcept {
  const uint32_t base = BaseWith(columns, lcp);
  const auto misfits = MisfitsAt(columns, base);
  return {.base = Base(base), .escaped = misfits[LenBits(misfits)] + deferred};
}

Shape ShapeOf(const VarColumns& columns) noexcept {
  return {
    .lcp_width = ColumnWidth(columns.max_lcp),
    .len_width = ColumnWidth(columns.max_len),
    .packing = PackingOf(columns),
  };
}

Shape ShapeWith(const VarColumns& columns, uint32_t lcp,
                uint32_t len) noexcept {
  return {
    .lcp_width = ColumnWidth(std::max(columns.max_lcp, lcp)),
    .len_width = ColumnWidth(std::max(columns.max_len, len)),
    .packing = PackingWith(columns, lcp, len),
  };
}

void Lower(VarColumns& columns, uint32_t lcp) noexcept {
  const uint32_t base = BaseWith(columns, lcp);
  if (base != columns.base) {
    columns.misfits = Recount(columns, Base(base));
    columns.base = base;
  }
}

void LowerTo(VarColumns& columns, uint32_t base) noexcept {
  if (base < columns.base) {
    columns.misfits = Recount(columns, base);
    columns.base = base;
  }
}

uint64_t StrideValue(bytes_view key, uint32_t prefix) noexcept {
  const auto stride = key.substr(prefix);
  SDB_ASSERT(PacksKeys(stride.size()));
  std::array<byte_type, sizeof(uint64_t)> word{};
  std::memcpy(word.data() + word.size() - stride.size(), stride.data(),
              stride.size());
  return absl::big_endian::Load64(word.data());
}

uint32_t KeyBits(bytes_view first, bytes_view last, uint32_t prefix) noexcept {
  if (!PacksKeys(first.size() - prefix)) {
    return 0;
  }
  return static_cast<uint32_t>(
    std::bit_width(StrideValue(last, prefix) - StrideValue(first, prefix)));
}

uint64_t FixedBytes(uint64_t count, uint64_t records, uint64_t width,
                    uint64_t prefix, uint32_t bits) noexcept {
  const uint64_t stride = width - prefix;
  const uint64_t groups = RecordGroups(count);
  const uint64_t keys = PacksKeys(stride)
                          ? 1 + stride + PackedKeyBytes(count, bits)
                          : count * stride;
  return 1 + VSize(count) + VSize(groups) + VSize(stride) + VSize(prefix) +
         prefix + (kRecordOffset + FixedHeadBytes(stride)) * groups + keys +
         records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

VarEntry VarEntryAt(const BlockBuilder& block, uint32_t index,
                    bytes_view key) noexcept {
  const bool restart = index % kRestartInterval == 0;
  const size_t lcp =
    index == 0 ? 0 : CommonPrefixLength(KeyAt(block, index - 1), key);
  uint32_t head_skip = block.head_skip;
  if (restart) {
    head_skip = static_cast<uint32_t>(
      index == 0 ? key.size() : CommonPrefixLength(KeyAt(block, 0), key));
  }
  return {
    .suffix = restart ? key : key.substr(lcp),
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
  columns.misfits = {};
  columns.base = kNoBase;
  columns.max_lcp = 0;
  columns.max_len = 0;
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  block.records.clear();
  block.record_starts.clear();
  Clear(block.columns);
  block.size = 0;
  block.deferred = 0;
  block.deferred_lcp = 0;
  block.deferred_len = 0;
  block.coded = 0;
  block.width = 0;
  block.prefix = 0;
  block.head_skip = 0;
  block.kind = BlockKind::Var;
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

void AppendEntry(VarColumns& columns, bool restart, size_t lcp,
                 bytes_view suffix) {
  if (restart) {
    columns.restarts.push_back(static_cast<uint16_t>(columns.suffixes.size()));
  }
  const auto len = static_cast<uint32_t>(suffix.size() - (restart ? lcp : 0));
  columns.max_lcp = std::max(columns.max_lcp, static_cast<uint32_t>(lcp));
  columns.max_len = std::max(columns.max_len, len);
  Lower(columns, static_cast<uint32_t>(lcp));
  const uint32_t base = Base(columns.base);
  if (columns.lcps.empty()) {
    SDB_ASSERT(lcp == 0 && base <= len);
    Count(columns.misfits, base, len - base, base);
  } else {
    Count(columns.misfits, static_cast<uint32_t>(lcp), len, base);
  }
  columns.lcps.push_back(static_cast<uint32_t>(lcp));
  columns.lens.push_back(len);
  columns.suffixes.append(suffix);
}

uint64_t SizeWith(const VarColumns& columns, uint32_t count,
                  const VarEntry& entry, size_t suffix,
                  uint64_t records) noexcept {
  const auto len =
    static_cast<uint32_t>(suffix - (entry.restart ? entry.lcp : 0));
  return VarBytes(count + 1, entry.head_skip, columns.suffixes.size() + suffix,
                  records,
                  ShapeWith(columns, static_cast<uint32_t>(entry.lcp), len));
}

void WriteValue(DataOutput& out, uint32_t value, uint32_t width) {
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

void WriteColumn(DataOutput& out, std::span<const uint32_t> values,
                 uint32_t width) {
  for (const auto value : values) {
    WriteValue(out, value, width);
  }
}

void WriteHeaders(DataOutput& out, const VarColumns& columns,
                  const Shape& shape, uint32_t len_bits) {
  const auto count = columns.lcps.size();
  const uint32_t base = shape.packing.base;
  const auto escapes = [&](StoredHeader stored) {
    return (MisfitMask(stored.lcp, stored.len, base) >> len_bits & 1) != 0;
  };
  for (size_t i = 0; i != count; ++i) {
    const auto stored = StoredAt(columns, i, base);
    out.WriteByte(escapes(stored)
                    ? kEscapedHeader
                    : Pack(stored.lcp - base, stored.len, len_bits));
  }
  for (size_t i = 0; i != count; ++i) {
    if (escapes(StoredAt(columns, i, base))) {
      out.WriteU16(static_cast<uint16_t>(i));
    }
  }
  for (size_t i = 0; i != count; ++i) {
    if (const auto stored = StoredAt(columns, i, base); escapes(stored)) {
      WriteValue(out, stored.lcp, shape.lcp_width);
    }
  }
  for (size_t i = 0; i != count; ++i) {
    if (const auto stored = StoredAt(columns, i, base); escapes(stored)) {
      WriteValue(out, stored.len, shape.len_width);
    }
  }
}

void WriteHeads(DataOutput& out, const BlockBuilder& block, size_t step,
                size_t skip) {
  for (size_t i = 0; i < block.ends.size(); i += step) {
    out.WriteU32(static_cast<uint32_t>(HeadAt(KeyAt(block, i), skip) >> 32));
  }
}

void WriteFixedHeads(DataOutput& out, const BlockBuilder& block) {
  if (FixedHeadBytes(block.width - block.prefix) == sizeof(uint32_t)) {
    WriteHeads(out, block, kRecordInterval, block.prefix);
    return;
  }
  for (size_t i = 0; i < block.ends.size(); i += kRecordInterval) {
    out.WriteU16(
      static_cast<uint16_t>(HeadAt(KeyAt(block, i), block.prefix) >> 48));
  }
}

void WritePackedKeys(DataOutput& out, const BlockBuilder& block,
                     uint32_t bits) {
  const uint64_t base = StrideValue(KeyAt(block, 0), block.prefix);
  std::array<byte_type, sizeof(uint64_t)> word;
  uint64_t pending = 0;
  uint32_t filled = 0;
  for (size_t i = 0; i != block.ends.size(); ++i) {
    const uint64_t delta = StrideValue(KeyAt(block, i), block.prefix) - base;
    SDB_ASSERT(static_cast<uint32_t>(std::bit_width(delta)) <= bits);
    pending |= delta << filled;
    filled += bits;
    if (filled >= 64) {
      absl::little_endian::Store64(word.data(), pending);
      out.WriteData(word.data(), word.size());
      filled -= 64;
      pending = filled == 0 ? 0 : delta >> (bits - filled);
    }
  }
  absl::little_endian::Store64(word.data(), pending);
  out.WriteData(word.data(), (filled + 7) / 8);
}

void Serialize(const BlockBuilder& block, DataOutput& out) {
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ASSERT(block.record_starts.size() == RecordGroups(count));
  out.WriteByte(static_cast<byte_type>(block.kind));
  out.WriteV32(count);
  if (block.kind != BlockKind::FixedStride) {
    SDB_ASSERT(block.kind == BlockKind::Var || block.coded == count);
    const auto& columns = block.columns;
    SDB_ASSERT(count <= std::numeric_limits<uint16_t>::max());
    const auto shape = ShapeOf(columns);
    const bool packed = Packed(count, shape);
    const uint32_t len_bits = LenBits(columns.misfits);
    out.WriteV32(static_cast<uint32_t>(columns.restarts.size()));
    out.WriteByte(static_cast<byte_type>(shape.lcp_width));
    out.WriteByte(static_cast<byte_type>(shape.len_width));
    out.WriteV64(columns.suffixes.size());
    out.WriteV32(block.head_skip);
    if (packed) {
      out.WriteByte(static_cast<byte_type>(len_bits));
      out.WriteV32(shape.packing.base);
      out.WriteV32(shape.packing.escaped);
    } else {
      out.WriteByte(kColumnHeaders);
    }
    for (const auto restart : columns.restarts) {
      out.WriteU16(restart);
    }
    WriteHeads(out, block, kRestartInterval, block.head_skip);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    if (packed) {
      WriteHeaders(out, columns, shape, len_bits);
    } else {
      WriteColumn(out, columns.lcps, shape.lcp_width);
      WriteColumn(out, columns.lens, shape.len_width);
    }
    out.WriteData(columns.suffixes.data(), columns.suffixes.size());
  } else {
    const uint32_t stride = block.width - block.prefix;
    out.WriteV32(static_cast<uint32_t>(block.record_starts.size()));
    out.WriteV32(stride);
    out.WriteV32(block.prefix);
    const auto bits =
      KeyBits(KeyAt(block, 0), KeyAt(block, count - 1), block.prefix);
    if (PacksKeys(stride)) {
      out.WriteByte(static_cast<byte_type>(bits));
      out.WriteData(block.keys.data() + block.prefix, stride);
    }
    out.WriteData(block.keys.data(), block.prefix);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    WriteFixedHeads(out, block);
    if (PacksKeys(stride)) {
      WritePackedKeys(out, block, bits);
    } else {
      for (size_t i = 0; i != count; ++i) {
        out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
      }
    }
  }
  out.WriteData(block.records.data(), block.records.size());
  out.WriteData(kZeros, kSlack);
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

duckdb_fsst_encoder_t* FieldWriter::Encoder() {
  if (!_trained) {
    _trained = true;
    TrainFsst();
  }
  return _fsst.get();
}

void FieldWriter::TrainFsst() {
  auto& arena = _samples;
  auto& lengths = _sample_lengths;
  arena.clear();
  lengths.clear();
  _reader->SampleTerms(kFsstSampleBytes, [&](std::span<const bytes_view> run) {
    for (size_t i = 1; i < run.size(); ++i) {
      const auto suffix = run[i].substr(CommonPrefixLength(run[i - 1], run[i]));
      if (!suffix.empty()) {
        arena.append(suffix);
        lengths.push_back(suffix.size());
      }
    }
  });
  if (lengths.empty()) {
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

bool FieldWriter::OwnsBlock() const noexcept {
  return _first_block != kNoBlock &&
         (_first_block != _blocks || _first_entry == 0);
}

void FieldWriter::EncodeEntries(VarColumns& columns, uint32_t from, uint32_t to,
                                const VarEntry* last) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ASSERT(to <= count || (to == count + 1 && last));
  _batch_entries.clear();
  _batch_lengths.clear();
  _batch_starts.clear();
  uint64_t raw = 0;
  uint32_t base = kNoBase;
  for (uint32_t i = from; i != to; ++i) {
    const auto& entry = _batch_entries.emplace_back(
      i < count ? VarEntryAt(block, i, KeyAt(block, i)) : *last);
    if (i != 0) {
      base = std::min(base, static_cast<uint32_t>(entry.lcp));
    }
    if (!entry.restart) {
      _batch_lengths.push_back(entry.suffix.size());
      _batch_starts.push_back(const_cast<unsigned char*>(entry.suffix.data()));
      raw += entry.suffix.size();
    }
  }
  const size_t n = _batch_lengths.size();
  _coded_lengths.resize(n);
  _coded_starts.resize(n);
  if (n != 0) {
    utils::StrResizeAmortized(_coded, 7 * n + 2 * raw);
    const size_t done =
      duckdb_fsst_compress(_fsst.get(), n, _batch_lengths.data(),
                           _batch_starts.data(), _coded.size(), _coded.data(),
                           _coded_lengths.data(), _coded_starts.data());
    SDB_ASSERT(done == n);
  }
  LowerTo(columns, base);
  size_t coded = 0;
  for (const auto& entry : _batch_entries) {
    if (entry.restart) {
      AppendEntry(columns, true, entry.lcp, entry.suffix);
    } else {
      AppendEntry(columns, false, entry.lcp,
                  {_coded_starts[coded], _coded_lengths[coded]});
      ++coded;
    }
  }
}

void FieldWriter::EncodeDeferred() {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  if (block.coded == count) {
    return;
  }
  EncodeEntries(block.columns, block.coded, count, nullptr);
  block.coded = count;
  block.deferred = 0;
  block.deferred_lcp = 0;
  block.deferred_len = 0;
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
  _record.Clear();
  _pw.Encode(_record, meta);
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
  _pw.SetTermPayloadWriter(reader.PayloadWriter());
  _pw.BeginField(props);
  _group_inline = false;
  _width = reader.key_width <= kMaxStride ? reader.key_width : 0;
  _reader = &reader;
  _trained = _width != 0;
  if (IsSubsetOf(IndexFeatures::Ranges, props.index_features) && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw, _docs);
  }

  const uint64_t first_inline = _inline.size();
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

  EndField(id, props, total_freq, docs_sum, first_inline);
  if (_block.kind == BlockKind::VarFsst) {
    Cut();
  }
  ResetField();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_width == 0) {
    _trie.Add(term);
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
    Cut();
    Append(term, meta);
  }
  _inline.append(meta.Inline());
  if (_first_block == kNoBlock) {
    _first_block = _blocks;
    _first_entry = static_cast<uint32_t>(_block.ends.size() - 1);
    _first_record =
      static_cast<uint32_t>(_block.records.size() - _record.View().size());
  }
}

bool FieldWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& block = _block;
  const bool record_start = block.ends.size() % kRecordInterval == 0;
  if (record_start) {
    _pw.BeginBlock();
    _group_inline = false;
  }
  _record.Clear();
  _pw.Encode(_record, meta);
  if (meta.inline_size != 0 && !_group_inline) {
    _record.WriteV64(_inline.size());
    _group_inline = true;
  }
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  const auto at = block.keys.size();
  block.keys.append(_key_prefix);
  block.keys.append(term);
  const auto key = bytes_view{block.keys}.substr(at);
  const bool fixed =
    _width != 0 &&
    (block.ends.empty() ||
     (block.kind == BlockKind::FixedStride && block.width == key.size()));
  if (!(fixed ? AppendFixedKey(key, records) : AppendVarKey(key, records))) {
    block.keys.resize(at);
    return false;
  }
  if (record_start) {
    block.record_starts.push_back(static_cast<uint16_t>(block.records.size()));
  }
  block.records.append(record);
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
  const uint32_t bits = count == 0 ? 0 : KeyBits(KeyAt(block, 0), key, prefix);
  const uint64_t size = FixedBytes(count + 1, records, width, prefix, bits);
  if (count != 0 && size > kPage) {
    return false;
  }
  block.size = size;
  block.width = width;
  block.prefix = prefix;
  block.kind = BlockKind::FixedStride;
  return true;
}

bool FieldWriter::AppendVarKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  if (block.kind == BlockKind::VarFsst) {
    return AppendCodedKey(VarEntryAt(block, count, key), records);
  }
  const bool converts = block.kind == BlockKind::FixedStride;
  if (converts) {
    for (uint32_t i = 0; i != count; ++i) {
      const auto entry = VarEntryAt(block, i, KeyAt(block, i));
      block.head_skip = entry.head_skip;
      AppendEntry(block.columns, entry.restart, entry.lcp, entry.suffix);
    }
  }
  const auto entry = VarEntryAt(block, count, key);
  const uint64_t size =
    SizeWith(block.columns, count, entry, entry.suffix.size(), records);
  if (count != 0 && size > kPage) {
    if (converts) {
      Clear(block.columns);
      block.head_skip = 0;
      return false;
    }
    return OwnsBlock() && EncodeBlock(entry, records);
  }
  block.kind = BlockKind::Var;
  AppendEntry(block.columns, entry.restart, entry.lcp, entry.suffix);
  block.size = size;
  block.head_skip = entry.head_skip;
  return true;
}

bool FieldWriter::EncodeBlock(const VarEntry& entry, uint64_t records) {
  if (!Encoder()) {
    return false;
  }
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  Clear(_encoded);
  EncodeEntries(_encoded, 0, count + 1, &entry);
  const auto head_skip = entry.head_skip;
  const uint64_t size = VarBytes(count + 1, head_skip, _encoded.suffixes.size(),
                                 records, ShapeOf(_encoded));
  if (size > kPage) {
    return false;
  }
  std::swap(block.columns, _encoded);
  block.kind = BlockKind::VarFsst;
  block.size = size;
  block.coded = count + 1;
  block.head_skip = head_skip;
  _fsst_used = true;
  return true;
}

bool FieldWriter::AppendCodedKey(const VarEntry& entry, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const uint64_t worst = entry.suffix.size() * (entry.restart ? 1 : 2);
  const auto lcp = static_cast<uint32_t>(entry.lcp);
  const auto deferred_lcp = std::max(block.deferred_lcp, lcp);
  const auto deferred_len =
    std::max(block.deferred_len, static_cast<uint32_t>(worst));
  const Shape shape{
    .lcp_width = ColumnWidth(std::max(block.columns.max_lcp, deferred_lcp)),
    .len_width = ColumnWidth(std::max(block.columns.max_len, deferred_len)),
    .packing = PackingBound(block.columns, lcp, count + 1 - block.coded),
  };
  const uint64_t bound = VarBytes(
    count + 1, entry.head_skip,
    block.columns.suffixes.size() + block.deferred + worst, records, shape);
  if (bound <= kPage) {
    Lower(block.columns, lcp);
    block.deferred += worst;
    block.deferred_lcp = deferred_lcp;
    block.deferred_len = deferred_len;
    block.size = bound;
    block.head_skip = entry.head_skip;
    return true;
  }
  EncodeDeferred();
  const auto suffix = entry.restart ? entry.suffix : EncodeSuffix(entry.suffix);
  const uint64_t size =
    SizeWith(block.columns, count, entry, suffix.size(), records);
  if (size > kPage) {
    return false;
  }
  AppendEntry(block.columns, entry.restart, entry.lcp, suffix);
  block.coded = count + 1;
  block.size = size;
  block.head_skip = entry.head_skip;
  return true;
}

void FieldWriter::Seal() {
  auto& block = _block;
  if (block.kind != BlockKind::VarFsst) {
    return;
  }
  EncodeDeferred();
  block.size =
    VarBytes(block.ends.size(), block.head_skip, block.columns.suffixes.size(),
             block.records.size(), ShapeOf(block.columns));
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
    StringOutput out{_separators};
    out.WriteV32(static_cast<uint32_t>(shared));
    WriteStr(out, separator.substr(shared));
    _separator.assign(separator);
  }
  WriteBlock();
  StringOutput{_inline_starts}.WriteV64(_inline_start - _inline_last);
  _inline_last = _inline_start;
  _inline_start = _inline.size();
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void FieldWriter::WriteBlock() {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  if (_blocks == 0) {
    _first_page = (position + pad) / kPage;
  }
  const uint64_t size = _block.size;
  const auto pages = static_cast<uint32_t>((size + kPage - 1) / kPage);
  if (pages != 1) {
    _spans.push_back({.block = _blocks, .extra = pages - 1});
  }
  Serialize(_block, *_out);
  SDB_ASSERT(_out->Position() == position + pad + size);
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_freq, uint64_t docs_sum,
                           uint64_t first_inline) {
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
    _record.Clear();
    _pw.Encode(_record, _single);
    head.record = ViewCast<char>(_record.View());
    head.record_inline = ViewCast<char>(_single.Inline());
    return;
  }
  const auto max =
    KeyAt(_block, _block.ends.size() - 1).substr(_key_prefix.size());
  head.max = ViewCast<char>(max);
  head.first_block = _first_block;
  head.first_entry = _first_entry;
  head.first_record = _first_record;
  head.last_block = _blocks;
  head.last_entry = static_cast<uint32_t>(_block.ends.size() - 1);
  if (_fsst_used) {
    head.fsst_table = std::move(_fsst_table);
  }
  if (_first_block == _blocks) {
    head.inline_base = first_inline;
    head.inline_size = _inline.size() - first_inline;
  } else if (text && _width == 0) {
    head.text.prefix_trie = _trie.Finish();
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
  _trie.Clear();
  _min.clear();
  _terms = 0;
  _first_block = kNoBlock;
  _fsst.reset();
  _fsst_table.clear();
  _reader = nullptr;
  _trained = false;
  _fsst_used = false;
}

void FieldWriter::Reset() noexcept {
  ResetField();
  Clear(_block);
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
      .separators = std::move(_separators),
      .inline_offset = out.Position(),
      .inline_size = _inline.size(),
      .ranges_size = _range_regions.size(),
      .fields = std::move(_heads),
      .inline_starts =
        _inline.empty() ? std::string{} : std::move(_inline_starts),
      .ordinal_bytes = _ordinal_bytes,
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
