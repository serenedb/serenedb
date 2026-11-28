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

#include "iresearch/formats/index/term_dict_block_writer.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <ranges>
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/text/fsst_footprint.hpp"
#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr uint64_t kFsstScratchPerSampleByte = 48;
constexpr uint64_t kBlockScratchPages = 16;

struct Packing {
  uint32_t base;
  uint32_t escaped;
};

struct Shape {
  uint32_t lcp_width;
  uint32_t len_width;
  Packing packing;
};

uint64_t HeaderBytes(uint64_t count, const Shape& shape) noexcept {
  return VSize(shape.packing.base) + VSize(shape.packing.escaped) + count +
         shape.packing.escaped *
           (kEscapedEntry + uint64_t{shape.lcp_width} + shape.len_width);
}

uint64_t ColumnBytes(uint64_t count, const Shape& shape) noexcept {
  return count * (uint64_t{shape.lcp_width} + shape.len_width);
}

bool Packed(uint64_t count, const Shape& shape) noexcept {
  return HeaderBytes(count, shape) <= ColumnBytes(count, shape);
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint64_t suffixes,
                  uint64_t records, const Shape& shape) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  SDB_ASSERT(suffixes >= restarts * head_skip);
  const uint64_t stored = suffixes - restarts * head_skip;
  return 4 + VSize(count) + VSize(restarts) + VSize(stored) + VSize(head_skip) +
         head_skip + (kVarRestart + kHeadBytes) * restarts +
         std::min(HeaderBytes(count, shape), ColumnBytes(count, shape)) +
         stored + records + kSlack;
}

uint32_t Base(uint32_t base) noexcept { return base == kNoBase ? 0 : base; }

uint32_t MisfitMask(uint32_t lcp, uint32_t len, uint32_t base) noexcept {
  constexpr uint32_t kAll = (1U << kLenSplits) - 1;
  SDB_ASSERT(lcp >= base);
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

uint64_t KeyRange(bytes_view first, bytes_view last, uint32_t prefix) noexcept {
  if (!PacksKeys(first.size() - prefix)) {
    return 0;
  }
  return StrideValue(last, prefix) - StrideValue(first, prefix);
}

uint64_t FixedBytes(uint64_t count, uint64_t records, uint64_t width,
                    uint64_t prefix, uint64_t universe) noexcept {
  const uint64_t stride = width - prefix;
  uint64_t keys = 0;
  if (PacksKeys(stride)) {
    const auto shape = EfShapeOf(count, universe);
    keys = 1 + VSize(shape.highs) + stride + EfBytes(count, shape);
  } else {
    keys = kHeadBytes * FixedHeads(count) + count * stride;
  }
  return 1 + VSize(count) + VSize(stride) + VSize(prefix) + prefix + keys +
         records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

uint32_t FixedWidth(const BlockBuilder& block) noexcept {
  SDB_ASSERT(block.kind == BlockKind::FixedStride);
  return static_cast<uint32_t>(KeyAt(block, 0).size());
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
  columns.suffix_bytes = 0;
  columns.restarts.clear();
  columns.misfits = {};
  columns.base = kNoBase;
  columns.max_lcp = 0;
  columns.max_len = 0;
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  Clear(block.columns);
  block.size = 0;
  block.deferred = 0;
  block.deferred_len = 0;
  block.coded = 0;
  block.prefix = 0;
  block.head_skip = 0;
  block.kind = BlockKind::Var;
}

TrackedVector<unsigned char*> SampleStarts(TrackedBstring& arena,
                                           std::span<const size_t> lengths) {
  TrackedVector<unsigned char*> starts(lengths.size(), arena.get_allocator());
  auto* start = arena.data();
  for (size_t i = 0; i != lengths.size(); ++i) {
    starts[i] = start;
    start += lengths[i];
  }
  return starts;
}

bool Compresses(duckdb_fsst_encoder_t* encoder, TrackedBstring& arena,
                TrackedVector<size_t>& lengths, uint64_t table_size) {
  const TrackedAllocator<> alloc = arena.get_allocator();
  auto starts = SampleStarts(arena, lengths);
  const uint64_t raw = arena.size();
  TrackedBstring out(7 + 2 * raw, 0, alloc);
  TrackedVector<size_t> out_lengths(lengths.size(), alloc);
  TrackedVector<unsigned char*> out_starts(lengths.size(), alloc);
  const size_t done = duckdb_fsst_compress(
    encoder, lengths.size(), lengths.data(), starts.data(), out.size(),
    out.data(), out_lengths.data(), out_starts.data());
  const uint64_t coded = absl::c_accumulate(out_lengths, uint64_t{0});
  return done == lengths.size() &&
         raw * 100 >= coded * kFsstMinRatioPct + table_size * 100;
}

void AppendEntry(VarColumns& columns, bool restart, size_t lcp,
                 bytes_view suffix, bool coded) {
  if (restart) {
    columns.restarts.push_back(static_cast<uint32_t>(columns.suffix_bytes));
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
  columns.suffix_bytes += suffix.size();
  if (coded) {
    columns.suffixes.append(suffix);
  }
}

uint64_t SizeWith(const VarColumns& columns, uint32_t count,
                  const VarEntry& entry, size_t suffix,
                  uint64_t records) noexcept {
  const auto len =
    static_cast<uint32_t>(suffix - (entry.restart ? entry.lcp : 0));
  return VarBytes(count + 1, entry.head_skip, columns.suffix_bytes + suffix,
                  records,
                  ShapeWith(columns, static_cast<uint32_t>(entry.lcp), len));
}

void WriteValue(duckdb::WriteStream& out, uint32_t value, uint32_t width) {
  switch (width) {
    case 1:
      out.Write<byte_type>(static_cast<byte_type>(value));
      break;
    case 2:
      out.Write<uint16_t>(static_cast<uint16_t>(value));
      break;
    default:
      out.Write<uint32_t>(value);
      break;
  }
}

void WriteColumn(duckdb::WriteStream& out, std::span<const uint32_t> values,
                 uint32_t width) {
  for (const auto value : values) {
    WriteValue(out, value, width);
  }
}

void WriteHeaders(duckdb::WriteStream& out, const VarColumns& columns,
                  const Shape& shape, uint32_t len_bits) {
  const auto count = columns.lcps.size();
  const uint32_t base = shape.packing.base;
  const auto escapes = [&](StoredHeader stored) {
    return (MisfitMask(stored.lcp, stored.len, base) >> len_bits & 1) != 0;
  };
  for (size_t i = 0; i != count; ++i) {
    const auto stored = StoredAt(columns, i, base);
    out.Write<byte_type>(escapes(stored)
                           ? kEscapedHeader
                           : Pack(stored.lcp - base, stored.len, len_bits));
  }
  for (size_t i = 0; i != count; ++i) {
    if (escapes(StoredAt(columns, i, base))) {
      out.Write<uint16_t>(static_cast<uint16_t>(i));
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

void WriteHeads(duckdb::WriteStream& out, const BlockBuilder& block,
                size_t step, size_t skip) {
  for (size_t i = 0; i < block.ends.size(); i += step) {
    out.Write<uint32_t>(
      static_cast<uint32_t>(HeadAt(KeyAt(block, i), skip) >> 32));
  }
}

void WriteEfKeys(duckdb::WriteStream& out, const BlockBuilder& block,
                 EfShape shape) {
  const auto count = block.ends.size();
  const uint64_t base = StrideValue(KeyAt(block, 0), block.prefix);
  const auto value = [&](uint64_t i) {
    return StrideValue(KeyAt(block, i), block.prefix) - base;
  };
  const auto position = [&](uint64_t i) {
    return (value(i) >> shape.low_bits) + i;
  };
  for (uint64_t i = 0; i < count; i += kEfSample) {
    SDB_ASSERT(position(i) <= std::numeric_limits<uint16_t>::max());
    out.Write<uint16_t>(static_cast<uint16_t>(position(i)));
  }
  const uint64_t words = EfUpperBytes(count, shape.highs) / sizeof(uint64_t);
  uint64_t word = 0;
  uint64_t bits = 0;
  for (uint64_t i = 0; i != count; ++i) {
    const uint64_t at = position(i);
    for (; word != at / kEfWordBits; ++word) {
      out.Write<uint64_t>(std::exchange(bits, 0));
    }
    bits |= uint64_t{1} << (at % kEfWordBits);
  }
  for (; word != words; ++word) {
    out.Write<uint64_t>(std::exchange(bits, 0));
  }
  const uint64_t mask = (uint64_t{1} << shape.low_bits) - 1;
  WritePacked(out, count, shape.low_bits,
              [&](uint64_t i) { return value(i) & mask; });
}

void Serialize(const BlockBuilder& block, const BlockRecords& records,
               duckdb::WriteStream& out) {
  const auto count = static_cast<uint32_t>(block.ends.size());
  out.Write<byte_type>(static_cast<byte_type>(block.kind));
  out.WriteVarint<uint32_t>(count);
  if (block.kind != BlockKind::FixedStride) {
    SDB_ASSERT(block.kind == BlockKind::Var || block.coded == count);
    const auto& columns = block.columns;
    SDB_ASSERT(count <= std::numeric_limits<uint16_t>::max());
    const auto shape = ShapeOf(columns);
    const bool packed = Packed(count, shape);
    const uint32_t len_bits = LenBits(columns.misfits);
    const uint32_t skip = block.head_skip;
    const auto restarts = std::span{columns.restarts};
    out.WriteVarint<uint32_t>(static_cast<uint32_t>(restarts.size()));
    out.Write<byte_type>(static_cast<byte_type>(shape.lcp_width));
    out.Write<byte_type>(static_cast<byte_type>(shape.len_width));
    out.WriteVarint<uint64_t>(columns.suffix_bytes - restarts.size() * skip);
    out.WriteVarint<uint32_t>(skip);
    if (packed) {
      out.Write<byte_type>(static_cast<byte_type>(len_bits));
      out.WriteVarint<uint32_t>(shape.packing.base);
      out.WriteVarint<uint32_t>(shape.packing.escaped);
    } else {
      out.Write<byte_type>(kColumnHeaders);
    }
    out.WriteData(block.keys.data(), skip);
    for (size_t i = 0; i != restarts.size(); ++i) {
      SDB_ASSERT(restarts[i] - i * skip <=
                 std::numeric_limits<uint16_t>::max());
      out.Write<uint16_t>(static_cast<uint16_t>(restarts[i] - i * skip));
    }
    WriteHeads(out, block, kRestartInterval, skip);
    if (packed) {
      WriteHeaders(out, columns, shape, len_bits);
    } else {
      WriteColumn(out, columns.lcps, shape.lcp_width);
      WriteColumn(out, columns.lens, shape.len_width);
    }
    if (block.kind == BlockKind::VarFsst) {
      const bytes_view suffixes{columns.suffixes};
      for (size_t i = 0; i != restarts.size(); ++i) {
        const size_t end =
          i + 1 == restarts.size() ? suffixes.size() : restarts[i + 1];
        out.WriteData(suffixes.data() + restarts[i] + skip,
                      end - restarts[i] - skip);
      }
    } else {
      for (uint32_t i = 0; i != count; ++i) {
        const auto key = KeyAt(block, i);
        const size_t from = i % kRestartInterval == 0 ? skip : columns.lcps[i];
        out.WriteData(key.data() + from, key.size() - from);
      }
    }
  } else {
    const uint32_t stride = FixedWidth(block) - block.prefix;
    out.WriteVarint<uint32_t>(stride);
    out.WriteVarint<uint32_t>(block.prefix);
    if (PacksKeys(stride)) {
      const auto shape = EfShapeOf(
        count,
        KeyRange(KeyAt(block, 0), KeyAt(block, count - 1), block.prefix));
      out.Write<byte_type>(static_cast<byte_type>(shape.low_bits));
      out.WriteVarint<uint64_t>(shape.highs);
      out.WriteData(block.keys.data() + block.prefix, stride);
      out.WriteData(block.keys.data(), block.prefix);
      WriteEfKeys(out, block, shape);
    } else {
      out.WriteData(block.keys.data(), block.prefix);
      WriteHeads(out, block, kFixedHeadInterval, block.prefix);
      for (size_t i = 0; i != count; ++i) {
        out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
      }
    }
  }
  records.Write(out);
  for (uint32_t i = 0; i != kSlack; ++i) {
    out.Write<byte_type>(0);
  }
}

}  // namespace

void FsstEncoderDelete::operator()(
  duckdb_fsst_encoder_t* encoder) const noexcept {
  duckdb_fsst_destroy(encoder);
}

uint64_t BlockEncoder::Bound() noexcept {
  return codecs::text::fsst::Fsst8Footprint(0).build +
         kFsstScratchPerSampleByte * kFsstSampleBytes +
         kBlockScratchPages * kPage;
}

duckdb_fsst_encoder_t* BlockEncoder::Encoder() {
  if (!_fsst.trained) {
    _fsst.trained = true;
    TrainFsst();
  }
  return _fsst.encoder.get();
}

void BlockEncoder::TrainFsst() {
  const TrackedAllocator<> alloc = _block.keys.get_allocator();
  TrackedBstring arena{alloc};
  TrackedVector<size_t> lengths{alloc};
  _fsst.reader->SampleTerms(
    kFsstSampleBytes, [&](std::span<const bytes_view> run) {
      for (size_t i = 1; i < run.size(); ++i) {
        const auto suffix =
          run[i].substr(CommonPrefixLength(run[i - 1], run[i]));
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
  const auto footprint = codecs::text::fsst::Fsst8Footprint(lengths.size());
  MemoryCharge charge{alloc, footprint.build};
  FsstEncoder encoder{
    duckdb_fsst_create(lengths.size(), lengths.data(), starts.data(), 0)};
  charge.Resize(footprint.kept);
  unsigned char table[FSST_MAXHEADER];
  const auto table_size = duckdb_fsst_export(encoder.get(), table);
  if (!Compresses(encoder.get(), arena, lengths, table_size)) {
    return;
  }
  _fsst.charge = std::move(charge);
  _fsst.encoder = std::move(encoder);
  _fsst.table.assign(reinterpret_cast<const char*>(table), table_size);
}

void BlockEncoder::EncodeEntries(VarColumns& columns, uint32_t from,
                                 uint32_t to, const VarEntry* last) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ASSERT(to <= count || (to == count + 1 && last));
  _scratch.entries.clear();
  _scratch.lengths.clear();
  _scratch.starts.clear();
  uint64_t raw = 0;
  uint32_t base = kNoBase;
  for (uint32_t i = from; i != to; ++i) {
    const auto& entry = _scratch.entries.emplace_back(
      i < count ? VarEntryAt(block, i, KeyAt(block, i)) : *last);
    if (i != 0) {
      base = std::min(base, static_cast<uint32_t>(entry.lcp));
    }
    if (!entry.restart) {
      _scratch.lengths.push_back(entry.suffix.size());
      _scratch.starts.push_back(
        const_cast<unsigned char*>(entry.suffix.data()));
      raw += entry.suffix.size();
    }
  }
  const size_t n = _scratch.lengths.size();
  _scratch.coded_lengths.resize(n);
  _scratch.coded_starts.resize(n);
  if (n != 0) {
    utils::StrResizeAmortized(_scratch.coded, 7 * n + 2 * raw);
    const size_t done = duckdb_fsst_compress(
      _fsst.encoder.get(), n, _scratch.lengths.data(), _scratch.starts.data(),
      _scratch.coded.size(), _scratch.coded.data(),
      _scratch.coded_lengths.data(), _scratch.coded_starts.data());
    SDB_ASSERT(done == n);
  }
  LowerTo(columns, base);
  size_t coded = 0;
  for (const auto& entry : _scratch.entries) {
    if (entry.restart) {
      AppendEntry(columns, true, entry.lcp, entry.suffix, true);
    } else {
      AppendEntry(columns, false, entry.lcp,
                  {_scratch.coded_starts[coded], _scratch.coded_lengths[coded]},
                  true);
      ++coded;
    }
  }
}

void BlockEncoder::EncodeDeferred() {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  if (block.coded == count) {
    return;
  }
  EncodeEntries(block.columns, block.coded, count, nullptr);
  block.coded = count;
  block.deferred = 0;
  block.deferred_len = 0;
}

bytes_view BlockEncoder::EncodeSuffix(bytes_view suffix) {
  SDB_ASSERT(!suffix.empty());
  size_t length = suffix.size();
  auto* input = const_cast<unsigned char*>(suffix.data());
  utils::StrResizeAmortized(_scratch.coded, 7 + 2 * length);
  size_t out_length = 0;
  unsigned char* out = nullptr;
  const size_t done = duckdb_fsst_compress(
    _fsst.encoder.get(), 1, &length, &input, _scratch.coded.size(),
    _scratch.coded.data(), &out_length, &out);
  SDB_ASSERT(done == 1);
  return {out, out_length};
}

void BlockEncoder::StartField(const BasicTermReader& reader,
                              uint32_t width) noexcept {
  _fsst.reader = &reader;
  _fsst.trained = width != 0;
  _width = width;
}

std::string BlockEncoder::TakeFsstTable() noexcept {
  return _fsst.used ? std::move(_fsst.table) : std::string{};
}

void BlockEncoder::EndField() noexcept { _fsst = {}; }

bytes_view BlockEncoder::Key(uint32_t entry) const noexcept {
  return term_dict::KeyAt(_block, entry);
}

bool BlockEncoder::Append(bytes_view prefix, bytes_view term, uint64_t records,
                          bool codes) {
  auto& block = _block;
  const auto at = block.keys.size();
  block.keys.append(prefix);
  block.keys.append(term);
  const auto key = bytes_view{block.keys}.substr(at);
  const bool fixed =
    _width != 0 &&
    (block.ends.empty() ||
     (block.kind == BlockKind::FixedStride && FixedWidth(block) == key.size()));
  if (!(fixed ? AppendFixedKey(key, records)
              : AppendVarKey(key, records, codes))) {
    block.keys.resize(at);
    return false;
  }
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  return true;
}

bool BlockEncoder::AppendFixedKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const auto width = static_cast<uint32_t>(key.size());
  const auto prefix =
    count == 0
      ? width
      : static_cast<uint32_t>(CommonPrefixLength(KeyAt(block, 0), key));
  const uint64_t universe =
    count == 0 ? 0 : KeyRange(KeyAt(block, 0), key, prefix);
  const uint64_t size = FixedBytes(count + 1, records, width, prefix, universe);
  if (count != 0 &&
      (size > kPage || (PacksKeys(width - prefix) &&
                        !EfFits(count + 1, EfShapeOf(count + 1, universe))))) {
    return false;
  }
  block.size = size;
  block.prefix = prefix;
  block.kind = BlockKind::FixedStride;
  return true;
}

bool BlockEncoder::AppendVarKey(bytes_view key, uint64_t records, bool codes) {
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
      AppendEntry(block.columns, entry.restart, entry.lcp, entry.suffix, false);
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
    return codes && EncodeBlock(entry, records);
  }
  block.kind = BlockKind::Var;
  AppendEntry(block.columns, entry.restart, entry.lcp, entry.suffix, false);
  block.size = size;
  block.head_skip = entry.head_skip;
  return true;
}

bool BlockEncoder::EncodeBlock(const VarEntry& entry, uint64_t records) {
  if (!Encoder()) {
    return false;
  }
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  Clear(_scratch.encoded);
  EncodeEntries(_scratch.encoded, 0, count + 1, &entry);
  const auto head_skip = entry.head_skip;
  const uint64_t size =
    VarBytes(count + 1, head_skip, _scratch.encoded.suffix_bytes, records,
             ShapeOf(_scratch.encoded));
  if (size > kPage) {
    return false;
  }
  std::swap(block.columns, _scratch.encoded);
  block.kind = BlockKind::VarFsst;
  block.size = size;
  block.coded = count + 1;
  block.head_skip = head_skip;
  _fsst.used = true;
  return true;
}

bool BlockEncoder::AppendCodedKey(const VarEntry& entry, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const uint64_t worst = entry.suffix.size() * (entry.restart ? 1 : 2);
  const auto lcp = static_cast<uint32_t>(entry.lcp);
  const auto deferred_len =
    std::max(block.deferred_len, static_cast<uint32_t>(worst));
  const Shape shape{
    .lcp_width = ColumnWidth(std::max(block.columns.max_lcp, lcp)),
    .len_width = ColumnWidth(std::max(block.columns.max_len, deferred_len)),
    .packing = PackingBound(block.columns, lcp, count + 1 - block.coded),
  };
  const uint64_t bound = VarBytes(
    count + 1, entry.head_skip,
    block.columns.suffix_bytes + block.deferred + worst, records, shape);
  if (bound <= kPage) {
    Lower(block.columns, lcp);
    block.columns.max_lcp = std::max(block.columns.max_lcp, lcp);
    block.deferred += worst;
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
  AppendEntry(block.columns, entry.restart, entry.lcp, suffix, true);
  block.coded = count + 1;
  block.size = size;
  block.head_skip = entry.head_skip;
  return true;
}

uint64_t BlockEncoder::Seal(uint64_t records) {
  auto& block = _block;
  if (block.kind == BlockKind::VarFsst) {
    EncodeDeferred();
    block.size =
      VarBytes(block.ends.size(), block.head_skip, block.columns.suffix_bytes,
               records, ShapeOf(block.columns));
  }
  return block.size;
}

void BlockEncoder::Serialize(duckdb::WriteStream& out,
                             const BlockRecords& records) const {
  term_dict::Serialize(_block, records, out);
}

void BlockEncoder::Reset() noexcept { Clear(_block); }

}  // namespace irs::term_dict
