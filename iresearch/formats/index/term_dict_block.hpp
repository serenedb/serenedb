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

#include <absl/base/internal/endian.h>
#include <fsst.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>

#include "iresearch/formats/index/term_dict_block_format.hpp"
#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kNoSkip = std::numeric_limits<uint32_t>::max();

struct Column {
  const byte_type* data;
  uint32_t width;
};

struct PackedHeaders {
  const byte_type* codes;
  const byte_type* escaped_entries;
  uint32_t escaped;
  uint32_t lcp_base;
  uint32_t len_bits;
};

struct FixedBound {
  uint32_t entry;
  uint32_t at;
};

struct EfKeys {
  IRS_FORCE_INLINE uint64_t Word(uint32_t word) const noexcept {
    return absl::little_endian::Load64(upper + sizeof(uint64_t) * word);
  }
  IRS_FORCE_INLINE bool Bit(uint32_t at) const noexcept {
    return (Word(at / kEfWordBits) >> (at % kEfWordBits) & 1) != 0;
  }
  IRS_FORCE_INLINE uint32_t Sample(uint32_t sample) const noexcept {
    return absl::little_endian::Load16(samples + kEfSampleBytes * sample);
  }
  IRS_FORCE_INLINE uint64_t Low(uint32_t entry) const noexcept {
    const uint64_t bit = uint64_t{entry} * low_bits;
    const auto* p = lows + bit / 8;
    const auto shift = static_cast<uint32_t>(bit % 8);
    uint64_t word = absl::little_endian::Load64(p) >> shift;
    if (low_bits + shift > 64) [[unlikely]] {
      word |= uint64_t{p[sizeof(uint64_t)]} << (64 - shift);
    }
    return word & low_mask;
  }
  IRS_FORCE_INLINE uint64_t ValueAt(uint32_t entry,
                                    uint32_t at) const noexcept {
    return uint64_t{at - entry} << low_bits | Low(entry);
  }

  uint32_t At(uint32_t entry) const noexcept {
    const uint32_t from = Sample(entry / kEfSample);
    uint64_t rank = entry % kEfSample;
    if (rank == 0) {
      return from;
    }
    uint32_t word = from / kEfWordBits;
    uint64_t bits = Word(word) & (~uint64_t{0} << (from % kEfWordBits));
    for (auto count = static_cast<uint64_t>(std::popcount(bits)); count <= rank;
         count = static_cast<uint64_t>(std::popcount(bits))) {
      rank -= count;
      bits = Word(++word);
    }
    return word * kEfWordBits + static_cast<uint32_t>(SelectInWord(bits, rank));
  }

  uint32_t After(uint32_t at) const noexcept {
    uint32_t word = at / kEfWordBits;
    uint64_t bits = Word(word) & (~uint64_t{1} << (at % kEfWordBits));
    while (bits == 0) {
      bits = Word(++word);
    }
    return word * kEfWordBits + static_cast<uint32_t>(std::countr_zero(bits));
  }

  uint32_t Zero(uint32_t zero) const noexcept {
    uint32_t lo = 0;
    for (auto size = static_cast<uint32_t>(EfSamples(count)); size > 1;) {
      const uint32_t half = size / 2;
      const uint32_t mid = lo + half;
      lo = Sample(mid) - kEfSample * mid <= zero ? mid : lo;
      size -= half;
    }
    const uint32_t from = Sample(lo);
    uint64_t rank = zero - (from - kEfSample * lo);
    uint32_t word = from / kEfWordBits;
    uint64_t bits = ~Word(word) & (~uint64_t{0} << (from % kEfWordBits));
    for (auto count = static_cast<uint64_t>(std::popcount(bits)); count <= rank;
         count = static_cast<uint64_t>(std::popcount(bits))) {
      rank -= count;
      bits = ~Word(++word);
    }
    return word * kEfWordBits + static_cast<uint32_t>(SelectInWord(bits, rank));
  }

  FixedBound LowerBound(uint64_t value) const noexcept {
    const uint64_t high = value >> low_bits;
    if (high > highs) {
      return {.entry = count, .at = 0};
    }
    uint32_t entry = 0;
    uint32_t at = 0;
    if (high != 0) {
      at = Zero(static_cast<uint32_t>(high - 1)) + 1;
      entry = at - static_cast<uint32_t>(high);
    }
    const uint64_t low = value & low_mask;
    for (; entry != count && Bit(at); ++entry, ++at) {
      if (Low(entry) >= low) {
        return {.entry = entry, .at = at};
      }
    }
    return {.entry = entry, .at = entry == count ? 0 : After(at)};
  }

  uint64_t base;
  uint64_t low_mask;
  const byte_type* samples;
  const byte_type* lows;
  const byte_type* upper;
  uint64_t highs;
  uint32_t low_bits;
  uint32_t count;
};

struct BlockView {
  bool fixed;
  bool coded;
  uint32_t count;
  uint32_t restarts;
  uint32_t stride;
  uint32_t prefix;
  Column lcps;
  Column lens;
  PackedHeaders headers;
  EfKeys ef;
  const byte_type* table;
  const byte_type* heads;
  const byte_type* suffixes;
  const byte_type* keys;
  const byte_type* records;
};

inline uint32_t SuffixOffset(const BlockView& view, uint32_t restart) noexcept {
  return absl::little_endian::Load16(view.table + kVarRestart * restart);
}

inline bytes_view Prefix(const BlockView& view) noexcept {
  return {view.table - view.prefix, view.prefix};
}

IRS_FORCE_INLINE inline uint64_t LoadStride(const byte_type* p,
                                            uint32_t stride) noexcept {
  SDB_ASSERT(PacksKeys(stride));
  return absl::big_endian::Load64(p) >> (8 * (sizeof(uint64_t) - stride));
}

using StrideBuffer = std::array<byte_type, sizeof(uint64_t)>;

inline bytes_view FixedKey(const BlockView& view, uint32_t entry, uint32_t at,
                           StrideBuffer& buffer) noexcept {
  if (!PacksKeys(view.stride)) {
    return {view.keys + uint64_t{entry} * view.stride, view.stride};
  }
  absl::big_endian::Store64(buffer.data(),
                            (view.ef.base + view.ef.ValueAt(entry, at))
                              << (8 * (sizeof(uint64_t) - view.stride)));
  return {buffer.data(), view.stride};
}

inline byte_type BlockByte(const byte_type*& p, const byte_type* end) {
  if (p == end) [[unlikely]] {
    Corrupted("block header");
  }
  return *p++;
}

template<typename T>
T BlockVarint(const byte_type*& p, const byte_type* end) {
  const auto value = ReadVarint<T>(p, end);
  if (!value) [[unlikely]] {
    Corrupted("block header");
  }
  return *value;
}

inline const byte_type* BlockSection(const byte_type* at, const byte_type* end,
                                     uint64_t bytes) {
  if (bytes > static_cast<uint64_t>(end - at)) [[unlikely]] {
    Corrupted("block size");
  }
  return at + bytes;
}

inline BlockView Parse(bytes_view block, const duckdb_fsst_decoder_t* fsst) {
  const auto* p = block.data();
  const auto* const end = p + block.size();
  BlockView view{};
  const auto kind = static_cast<BlockKind>(BlockByte(p, end));
  view.fixed = kind == BlockKind::FixedStride;
  view.coded = kind == BlockKind::VarFsst;
  if (view.coded && !fsst) [[unlikely]] {
    Corrupted("FSST-coded block without an FSST table");
  }
  view.count = BlockVarint<uint32_t>(p, end);
  if (kind == BlockKind::Var || kind == BlockKind::VarFsst) {
    view.restarts = BlockVarint<uint32_t>(p, end);
    if (view.restarts != (uint64_t{view.count} + kRestartInterval - 1) /
                           kRestartInterval) [[unlikely]] {
      Corrupted("block restart count");
    }
    const auto width = [&] {
      const uint32_t width = BlockByte(p, end);
      if (width != 1 && width != 2 && width != 4) [[unlikely]] {
        Corrupted("block column width");
      }
      return width;
    };
    view.lcps.width = width();
    view.lens.width = width();
    const auto suffixes = BlockVarint<uint64_t>(p, end);
    view.prefix = BlockVarint<uint32_t>(p, end);
    const uint32_t layout = BlockByte(p, end);
    const bool packed = layout != kColumnHeaders;
    auto& headers = view.headers;
    uint32_t columns = view.count;
    if (packed) {
      if (layout > kHeaderBits) [[unlikely]] {
        Corrupted("block header layout");
      }
      headers.len_bits = layout;
      headers.lcp_base = BlockVarint<uint32_t>(p, end);
      headers.escaped = BlockVarint<uint32_t>(p, end);
      if (headers.escaped > view.count) [[unlikely]] {
        Corrupted("block escaped headers");
      }
      columns = headers.escaped;
    }
    view.table = BlockSection(p, end, view.prefix);
    view.heads =
      BlockSection(view.table, end, uint64_t{kVarRestart} * view.restarts);
    view.lcps.data =
      BlockSection(view.heads, end, uint64_t{kHeadBytes} * view.restarts);
    if (packed) {
      headers.codes = view.lcps.data;
      headers.escaped_entries = BlockSection(headers.codes, end, view.count);
      view.lcps.data = BlockSection(headers.escaped_entries, end,
                                    uint64_t{kEscapedEntry} * headers.escaped);
    }
    view.lens.data =
      BlockSection(view.lcps.data, end, uint64_t{columns} * view.lcps.width);
    view.suffixes =
      BlockSection(view.lens.data, end, uint64_t{columns} * view.lens.width);
    view.records = BlockSection(view.suffixes, end, suffixes);
    BlockSection(view.records, end, kSlack);
    return view;
  }
  if (view.fixed) {
    view.stride = BlockVarint<uint32_t>(p, end);
    view.prefix = BlockVarint<uint32_t>(p, end);
    if (!PacksKeys(view.stride)) {
      view.table = BlockSection(p, end, view.prefix);
      view.restarts = static_cast<uint32_t>(FixedHeads(view.count));
      view.heads = view.table;
      view.keys =
        BlockSection(view.heads, end, uint64_t{kHeadBytes} * view.restarts);
      view.records =
        BlockSection(view.keys, end, uint64_t{view.count} * view.stride);
      BlockSection(view.records, end, kSlack);
      return view;
    }
    auto& ef = view.ef;
    ef.count = view.count;
    ef.low_bits = BlockByte(p, end);
    if (ef.low_bits >= 8 * view.stride) [[unlikely]] {
      Corrupted("block key low bits");
    }
    ef.highs = BlockVarint<uint64_t>(p, end);
    const uint64_t max = ~uint64_t{0} >> (8 * (sizeof(uint64_t) - view.stride));
    if (ef.count == 0 || ef.highs > max >> ef.low_bits ||
        !EfFits(ef.count, {.low_bits = ef.low_bits, .highs = ef.highs}))
      [[unlikely]] {
      Corrupted("block key high bits");
    }
    ef.low_mask = (uint64_t{1} << ef.low_bits) - 1;
    const auto* const base = p;
    p = BlockSection(p, end, view.stride);
    ef.base = LoadStride(base, view.stride);
    view.table = BlockSection(p, end, view.prefix);
    ef.samples = view.table;
    ef.upper = BlockSection(ef.samples, end,
                            uint64_t{kEfSampleBytes} * EfSamples(ef.count));
    ef.lows = BlockSection(ef.upper, end, EfUpperBytes(ef.count, ef.highs));
    view.records =
      BlockSection(ef.lows, end, PackedBytes(ef.count, ef.low_bits));
    BlockSection(view.records, end, kSlack);
    return view;
  }
  Corrupted("block kind");
}

struct Header {
  uint32_t lcp;
  uint32_t len;
};

IRS_FORCE_INLINE inline uint32_t ColumnAt(Column column,
                                          uint32_t entry) noexcept {
  if (column.width == 1) [[likely]] {
    return column.data[entry];
  }
  if (column.width == 2) {
    return absl::little_endian::Load16(column.data + 2 * uint64_t{entry});
  }
  return absl::little_endian::Load32(column.data + 4 * uint64_t{entry});
}

IRS_FORCE_INLINE inline Header ColumnsAt(const BlockView& view,
                                         uint32_t i) noexcept {
  return {
    .lcp = ColumnAt(view.lcps, i),
    .len = ColumnAt(view.lens, i),
  };
}

IRS_NO_INLINE inline Header EscapedHeaderAt(const BlockView& view,
                                            uint32_t entry) noexcept {
  const auto& headers = view.headers;
  const auto at =
    static_cast<uint32_t>(PartitionIndex(0, headers.escaped, [&](size_t i) {
      return absl::little_endian::Load16(headers.escaped_entries +
                                         kEscapedEntry * i) < entry;
    }));
  SDB_ASSERT(at < headers.escaped &&
             absl::little_endian::Load16(headers.escaped_entries +
                                         kEscapedEntry * at) == entry);
  return ColumnsAt(view, at);
}

IRS_FORCE_INLINE inline Header HeaderAt(const BlockView& view,
                                        uint32_t entry) noexcept {
  const auto& headers = view.headers;
  Header header;
  if (!headers.codes) {
    header = ColumnsAt(view, entry);
  } else if (const uint32_t code = headers.codes[entry]; code == kEscapedHeader)
    [[unlikely]] {
    header = EscapedHeaderAt(view, entry);
  } else {
    header = {
      .lcp = headers.lcp_base + (code >> headers.len_bits),
      .len = code & ((uint32_t{1} << headers.len_bits) - 1),
    };
  }
  if (entry % kRestartInterval == 0) {
    header.len = header.len + header.lcp - view.prefix;
  }
  return header;
}

inline bytes_view SymbolAt(const duckdb_fsst_decoder_t& decoder,
                           size_t code) noexcept {
  static_assert(std::endian::native == std::endian::little);
  return {reinterpret_cast<const byte_type*>(&decoder.symbol[code]),
          decoder.len[code]};
}

struct Symbol {
  uint64_t word;
  uint32_t len;
};

IRS_FORCE_INLINE inline Symbol NextSymbol(const duckdb_fsst_decoder_t& decoder,
                                          const byte_type* codes,
                                          size_t& i) noexcept {
  const size_t code = codes[i++];
  if (code != FSST_ESC) [[likely]] {
    return {
      .word = absl::little_endian::Load64(SymbolAt(decoder, code).data()),
      .len = decoder.len[code],
    };
  }
  return {.word = codes[i++], .len = 1};
}

inline size_t DecodeInto(const duckdb_fsst_decoder_t& decoder, bytes_view codes,
                         byte_type* out, size_t limit) noexcept {
  size_t pos = 0;
  for (size_t i = 0; i != codes.size() && pos < limit;) {
    const auto symbol = NextSymbol(decoder, codes.data(), i);
    absl::little_endian::Store64(out + pos, symbol.word);
    pos += symbol.len;
  }
  return pos;
}

inline void DecodeRange(const duckdb_fsst_decoder_t& decoder, bytes_view codes,
                        size_t from, size_t to, byte_type* out) noexcept {
  size_t pos = 0;
  for (size_t i = 0; i != codes.size() && pos < to;) {
    const auto symbol = NextSymbol(decoder, codes.data(), i);
    const size_t end = pos + symbol.len;
    for (size_t at = std::max(pos, from); at < std::min(end, to); ++at) {
      out[at - from] = static_cast<byte_type>(symbol.word >> (8 * (at - pos)));
    }
    pos = end;
  }
  SDB_ASSERT(pos >= to);
}

inline void RestartBytes(const BlockView& view, const byte_type* rest,
                         uint32_t from, uint32_t to, byte_type* out) noexcept {
  SDB_ASSERT(from <= to);
  const uint32_t prefix = view.prefix;
  if (from < prefix) {
    const uint32_t n = std::min(to, prefix) - from;
    std::memcpy(out, Prefix(view).data() + from, n);
    out += n;
    from += n;
  }
  if (from < to) {
    std::memcpy(out, rest + (from - prefix), to - from);
  }
}

inline void BytesBefore(const BlockView& view,
                        const duckdb_fsst_decoder_t* decoder, uint32_t entry,
                        const byte_type* data, uint32_t lcp, uint32_t count,
                        byte_type* out) noexcept {
  SDB_ASSERT(count <= lcp);
  if (entry % kRestartInterval == 0) {
    SDB_ASSERT(lcp >= view.prefix);
    RestartBytes(view, data - (lcp - view.prefix), lcp - count, lcp, out);
    return;
  }
  const uint32_t lo = lcp - count;
  uint32_t hi = lcp;
  for (uint32_t j = entry; hi > lo;) {
    SDB_ASSERT(j % kRestartInterval != 0);
    --j;
    const auto header = HeaderAt(view, j);
    data -= header.len;
    const bool restart = j % kRestartInterval == 0;
    const uint32_t start = restart ? 0 : header.lcp;
    if (start >= hi) {
      continue;
    }
    const uint32_t from = std::max(start, lo);
    if (restart) {
      RestartBytes(view, data, from, hi, out + (from - lo));
    } else if (!view.coded) {
      std::memcpy(out + (from - lo), data + (from - start), hi - from);
    } else {
      SDB_ASSERT(decoder);
      DecodeRange(*decoder, {data, header.len}, from - start, hi - start,
                  out + (from - lo));
    }
    hi = from;
  }
}

enum class Order : uint8_t {
  Less,
  Equal,
  Greater,
};

struct Compared {
  size_t common;
  Order order;
};

IRS_FORCE_INLINE inline Compared CompareRaw(const byte_type* suffix,
                                            size_t size, Probe rest) noexcept {
  const size_t n = std::min(size, rest.size);
  const size_t common = PaddedMismatch(suffix, rest.data, n);
  if (common != n) {
    return {
      .common = common,
      .order =
        suffix[common] < rest.data[common] ? Order::Less : Order::Greater,
    };
  }
  return {
    .common = common,
    .order = size == rest.size
               ? Order::Equal
               : (size < rest.size ? Order::Less : Order::Greater),
  };
}

struct CodedCompared {
  Compared compared;
  size_t code;
  size_t decoded;
};

template<bool Materialize>
IRS_FORCE_INLINE CodedCompared
CompareCoded(const duckdb_fsst_decoder_t& decoder, bytes_view codes, Probe rest,
             byte_type* out) noexcept {
  size_t pos = 0;
  for (size_t i = 0; i != codes.size();) {
    const auto [symbol, len] = NextSymbol(decoder, codes.data(), i);
    if constexpr (Materialize) {
      absl::little_endian::Store64(out + pos, symbol);
    }
    const size_t left = rest.size - pos;
    const uint64_t diff = symbol ^ absl::little_endian::Load64(rest.data + pos);
    const size_t at = std::countr_zero(diff) / 8;
    if (at < std::min<size_t>(len, left)) {
      const auto byte = static_cast<byte_type>(symbol >> (8 * at));
      return {
        .compared = {.common = pos + at,
                     .order = byte < rest.data[pos + at] ? Order::Less
                                                         : Order::Greater},
        .code = i,
        .decoded = pos + len,
      };
    }
    if (len > left) {
      return {
        .compared = {.common = rest.size, .order = Order::Greater},
        .code = i,
        .decoded = pos + len,
      };
    }
    pos += len;
  }
  return {
    .compared = {.common = pos,
                 .order = pos == rest.size ? Order::Equal : Order::Less},
    .code = codes.size(),
    .decoded = pos,
  };
}

inline bytes_view RestartKey(const BlockView& view, uint32_t restart) noexcept {
  const auto header = HeaderAt(view, restart * kRestartInterval);
  return {view.suffixes + SuffixOffset(view, restart), header.len};
}

struct HeadCounts {
  uint32_t less;
  uint32_t not_after;
};

IRS_FORCE_INLINE inline HeadCounts CountHeads(const byte_type* heads,
                                              uint32_t from, uint32_t count,
                                              uint32_t head) noexcept {
  static_assert(std::endian::native == std::endian::little);
  constexpr uint32_t kLanes = 16 / sizeof(head);
  constexpr uint32_t kMaskBits = 64 / kLanes;
  using Lanes = uint32_t __attribute__((vector_size(16)));
  using Mask = int16_t __attribute__((vector_size(8)));
  const auto load = [&](uint32_t i) {
    uint32_t value;
    std::memcpy(&value, heads + i * sizeof(value), sizeof(value));
    return value;
  };
  uint32_t not_after = from;
  for (uint32_t i = from; i < count; i += kLanes) {
    Lanes lanes;
    std::memcpy(&lanes, heads + i * sizeof(head), sizeof(lanes));
    const auto bits =
      std::bit_cast<uint64_t>(__builtin_convertvector(lanes <= head, Mask));
    const auto run = std::min<uint32_t>(
      count - i, static_cast<uint32_t>(std::countr_one(bits)) / kMaskBits);
    not_after = i + run;
    if (run != kLanes) {
      break;
    }
  }
  uint32_t less = not_after;
  while (less != from && load(less - 1) == head) {
    --less;
  }
  return {.less = less, .not_after = not_after};
}

inline Compared ComparePrefix(const BlockView& view, Probe probe) noexcept {
  const auto prefix = Prefix(view);
  const size_t shared = std::min(prefix.size(), probe.size);
  if (const size_t common = PaddedMismatch(prefix.data(), probe.data, shared);
      common != shared) {
    return {
      .common = common,
      .order =
        prefix[common] < probe.data[common] ? Order::Less : Order::Greater,
    };
  }
  return {
    .common = shared,
    .order = probe.size < prefix.size() ? Order::Greater : Order::Equal,
  };
}

inline uint32_t HeadsNotAfter(const BlockView& view, Probe probe,
                              uint32_t from) noexcept {
  const size_t skip = view.prefix;
  SDB_ASSERT(SuffixOffset(view, 0) == 0);
  SDB_ASSERT(probe.size >= skip);
  const auto counts = CountHeads(view.heads, from, view.restarts,
                                 absl::big_endian::Load32(probe.data + skip));
  const Probe rest{probe.data + skip, probe.size - skip};
  return static_cast<uint32_t>(
    PartitionIndex(counts.less, counts.not_after, [&](size_t restart) {
      const auto key = RestartKey(view, static_cast<uint32_t>(restart));
      return CompareRaw(key.data(), key.size(), rest).order != Order::Greater;
    }));
}

inline uint32_t RestartsNotAfter(const BlockView& view, Probe probe,
                                 uint32_t from = 0) noexcept {
  switch (ComparePrefix(view, probe).order) {
    case Order::Less:
      return view.restarts;
    case Order::Greater:
      return 0;
    case Order::Equal:
      break;
  }
  return HeadsNotAfter(view, probe, from);
}

IRS_FORCE_INLINE inline Order Locate(const byte_type* suffix, uint32_t size,
                                     uint32_t lcp, Probe probe,
                                     const duckdb_fsst_decoder_t* fsst,
                                     size_t& matched) noexcept {
  if (lcp != matched) {
    return lcp > matched ? Order::Less : Order::Greater;
  }
  const Probe rest{probe.data + matched, probe.size - matched};
  const auto compared =
    fsst ? CompareCoded<false>(*fsst, {suffix, size}, rest, nullptr).compared
         : CompareRaw(suffix, size, rest);
  matched += compared.common;
  return compared.order;
}

struct Bound {
  uint32_t entry;
  uint32_t restarts;
  bool equal;
};

inline Bound VarLowerBound(const BlockView& view, Probe probe,
                           const duckdb_fsst_decoder_t* fsst,
                           uint32_t from = 0) noexcept {
  const auto prefixed = ComparePrefix(view, probe);
  if (prefixed.order == Order::Greater) {
    return {.entry = 0, .restarts = 0, .equal = false};
  }
  const bool past = prefixed.order == Order::Less;
  const uint32_t restarts =
    past ? view.restarts : HeadsNotAfter(view, probe, from);
  if (restarts == 0) {
    return {.entry = 0, .restarts = 0, .equal = false};
  }
  const uint32_t group = restarts - 1;
  const uint32_t first = group * kRestartInterval;
  const uint32_t end = std::min(first + kRestartInterval, view.count);
  const auto* sp = view.suffixes + SuffixOffset(view, group);
  size_t matched = prefixed.common;
  for (uint32_t entry = first; entry != end; ++entry) {
    const auto header = HeaderAt(view, entry);
    const auto* suffix = sp;
    sp += header.len;
    const bool restart = entry == first;
    if (restart && past) {
      continue;
    }
    const auto order =
      Locate(suffix, header.len, restart ? view.prefix : header.lcp, probe,
             view.coded && !restart ? fsst : nullptr, matched);
    if (order != Order::Less) {
      return {
        .entry = entry, .restarts = restarts, .equal = order == Order::Equal};
    }
  }
  return {.entry = end, .restarts = restarts, .equal = false};
}

inline FixedBound FixedLowerBound(const BlockView& view, Probe probe,
                                  uint32_t from = 0) noexcept {
  constexpr FixedBound kFirst{.entry = 0, .at = 0};
  const FixedBound end{.entry = view.count, .at = 0};
  const auto prefix = Prefix(view);
  const size_t head = std::min(prefix.size(), probe.size);
  if (const size_t common = PaddedMismatch(prefix.data(), probe.data, head);
      common != head) {
    return prefix[common] < probe.data[common] ? end : kFirst;
  }
  if (probe.size < prefix.size()) {
    return kFirst;
  }
  const Probe rest{probe.data + prefix.size(), probe.size - prefix.size()};
  const uint32_t stride = view.stride;
  const bool longer = rest.size > stride;
  if (stride == 0) {
    return rest.size == 0 ? kFirst : end;
  }
  if (PacksKeys(stride)) {
    const uint64_t value = LoadStride(rest.data, stride);
    if (value < view.ef.base) {
      return kFirst;
    }
    uint64_t delta = value - view.ef.base;
    if (longer) {
      if (delta == std::numeric_limits<uint64_t>::max()) {
        return end;
      }
      ++delta;
    }
    return view.ef.LowerBound(delta);
  }
  const auto search = [&](auto&& less) {
    const auto counts = CountHeads(view.heads, from, view.restarts,
                                   absl::big_endian::Load32(rest.data));
    const auto groups = static_cast<uint32_t>(PartitionIndex(
      counts.less, counts.not_after,
      [&](size_t group) { return less(group * kFixedHeadInterval); }));
    if (groups == 0) {
      return kFirst;
    }
    const uint32_t first = (groups - 1) * kFixedHeadInterval + 1;
    const uint32_t last = std::min(groups * kFixedHeadInterval, view.count);
    return FixedBound{
      .entry = static_cast<uint32_t>(PartitionIndex(first, last, less)),
      .at = 0,
    };
  };
  if (stride <= 2 * sizeof(uint64_t)) {
    const uint32_t shift = 8 * (2 * sizeof(uint64_t) - stride);
    const uint64_t high = absl::big_endian::Load64(rest.data);
    const uint64_t low =
      absl::big_endian::Load64(rest.data + sizeof(uint64_t)) >> shift;
    return search([&](size_t i) {
      const auto* key = view.keys + i * stride;
      const uint64_t key_high = absl::big_endian::Load64(key);
      const uint64_t key_low =
        absl::big_endian::Load64(key + sizeof(uint64_t)) >> shift;
      return key_high < high || (key_high == high &&
                                 (key_low < low || (longer && key_low == low)));
    });
  }
  return search([&](size_t i) {
    return CompareRaw(view.keys + i * stride, stride, rest).order ==
           Order::Less;
  });
}

inline constexpr size_t kInlineKey = 64;
using KeyBuffer = containers::SmallVector<byte_type, kInlineKey>;

inline byte_type* Grow(KeyBuffer& buffer, size_t size) {
  if (buffer.size() < size) {
    buffer.resize(size, boost::container::default_init);
  }
  return buffer.data();
}

struct PendingSuffix {
  const byte_type* data;
  uint32_t lcp;
  uint32_t size;
  bool restart = false;
};

inline uint32_t Find(const BlockView& view, Probe key,
                     const duckdb_fsst_decoder_t* fsst,
                     uint32_t& from) noexcept {
  if (!view.fixed) {
    const auto bound = VarLowerBound(view, key, fsst, from);
    from = bound.restarts == 0 ? 0 : bound.restarts - 1;
    return bound.equal ? bound.entry : kNoEntry;
  }
  if (key.size != size_t{view.prefix} + view.stride ||
      PaddedMismatch(Prefix(view).data(), key.data, view.prefix) !=
        view.prefix) {
    return kNoEntry;
  }
  const auto [entry, at] = FixedLowerBound(view, key, from);
  from = entry / kFixedHeadInterval;
  if (entry == view.count) {
    return kNoEntry;
  }
  const auto* rest = key.data + view.prefix;
  const bool equal =
    PacksKeys(view.stride)
      ? view.ef.base + view.ef.ValueAt(entry, at) ==
          LoadStride(rest, view.stride)
      : PaddedMismatch(view.keys + uint64_t{entry} * view.stride, rest,
                       view.stride) == view.stride;
  return equal ? entry : kNoEntry;
}

class BlockCursor {
 public:
  explicit BlockCursor(const duckdb_fsst_decoder_t* fsst) noexcept
    : _fsst{fsst} {}

  void Open(bytes_view block, uint32_t limit) {
    ++_generation;
    _pending.count = 0;
    _partial = false;
    _size = _mat;
    _entry = kNoEntry;
    _fixed.entry = kNoEntry;
    _restart = 0;
    _view = Parse(block, _fsst);
    _stop = std::min(limit, _view.count);
    _sp = _view.suffixes;
  }

  const duckdb_fsst_decoder_t* Fsst() const noexcept { return _fsst; }
  uint64_t Generation() const noexcept { return _generation; }
  bytes_view Key() const noexcept {
    SDB_ASSERT(_pending.count == 0);
    return {_key.data(), _size};
  }
  const PendingSuffix* Pending() const noexcept {
    return _pending.count == 0 ? nullptr
                               : &_pending.suffixes[_pending.count - 1];
  }
  const BlockView& View() const noexcept { return _view; }
  uint32_t Entry() const noexcept { return _entry; }
  const byte_type* NextSuffix() const noexcept { return _sp; }
  bool AtEnd() const noexcept {
    return _entry != kNoEntry && _entry + 1 == _stop;
  }
  uint32_t Stop() const noexcept { return _stop; }

  void Group(uint32_t group) {
    _entry = group == 0 ? kNoEntry : group * kRestartInterval - 1;
    _restart = group * kRestartInterval;
    _sp = _view.suffixes + SuffixOffset(_view, group);
    _jumped = true;
    Next();
  }

  int ByteAt(size_t depth) {
    if (depth < _mat) {
      return _key[depth];
    }
    if (const auto* last = Pending(); last && depth == last->lcp) {
      const auto* data = last->data;
      SDB_ASSERT(last->size != 0);
      if (!_view.coded || last->restart) {
        return data[0];
      }
      if (data[0] == FSST_ESC) {
        return data[1];
      }
      return static_cast<byte_type>(_fsst->symbol[data[0]]);
    }
    Materialize();
    return depth < _size ? _key[depth] : -1;
  }

  IRS_FORCE_INLINE uint32_t Skip() {
    if (_view.fixed) {
      return Next();
    }
    const auto entry = _entry == kNoEntry ? 0 : _entry + 1;
    if (entry == _restart) {
      if (entry == 0) {
        return Next();
      }
      return SkipRestart(entry);
    }
    _entry = entry;
    SDB_ASSERT(_entry < _stop);
    const auto header = HeaderAt(_view, entry);
    PushPending(_sp, header);
    _mat = std::min<size_t>(_mat, header.lcp);
    _sp += header.len;
    return header.lcp;
  }

  IRS_FORCE_INLINE uint32_t SkipRestart(uint32_t entry) {
    _entry = entry;
    SDB_ASSERT(_entry < _stop);
    _restart += kRestartInterval;
    const auto header = HeaderAt(_view, entry);
    uint32_t shared = header.lcp;
    if (std::exchange(_jumped, false)) {
      shared = static_cast<uint32_t>(RestartMismatch(
        _sp, std::min<size_t>(_mat, header.len + _view.prefix)));
    }
    PushRestart(_sp, header);
    _mat = std::min<size_t>(_mat, shared);
    _sp += header.len;
    return shared;
  }

  template<typename Reject>
  IRS_FORCE_INLINE uint32_t SkipWhile(Reject&& reject) {
    const auto [first, end] = SkipSpan();
    if (first >= end) {
      return kNoSkip;
    }
    const auto* sp = _sp;
    uint32_t entry = first;
    uint32_t lcp = kNoSkip;
    for (; entry < end; ++entry) {
      const auto header = HeaderAt(_view, entry);
      if (!Rejects(reject, entry, sp, header)) {
        break;
      }
      lcp = std::min(lcp, header.lcp);
      sp += header.len;
    }
    return Skipped(first, entry, end, sp, lcp);
  }

  void Materialize() {
    if (_pending.count == 0) {
      return;
    }
    std::array<uint32_t, kRestartInterval> needs;
    uint32_t need = std::numeric_limits<uint32_t>::max();
    for (uint32_t i = _pending.count; i-- != 0;) {
      const auto lcp = _pending.suffixes[i].lcp;
      needs[i] = need > lcp ? need - lcp : 0;
      need = std::min(need, lcp);
    }
    for (uint32_t i = 0; i != _pending.count; ++i) {
      const auto& pending = _pending.suffixes[i];
      if (pending.restart) {
        SDB_ASSERT(i == 0 && _mat <= pending.lcp &&
                   pending.lcp >= _view.prefix);
        const size_t end = pending.lcp + std::min(pending.size, needs[i]);
        RestartBytes(_view, pending.data - (pending.lcp - _view.prefix),
                     static_cast<uint32_t>(_mat), static_cast<uint32_t>(end),
                     Reserve(end) + _mat);
        _size = end;
      } else if (needs[i] != 0) {
        _size = pending.lcp +
                Expand({pending.data, pending.size}, pending.lcp, needs[i]);
      }
    }
    _mat = _size;
    _pending.count = 0;
  }

  uint32_t Next() {
    _partial = false;
    _entry = _entry == kNoEntry ? 0 : _entry + 1;
    SDB_ASSERT(_entry < _stop);
    if (_view.fixed) {
      return SetFixed(_entry);
    }
    return NextVar();
  }

  size_t Shared() const noexcept { return _shared; }

  bool LowerBound(Probe probe) {
    if (_view.fixed) {
      const auto bound = FixedLowerBound(_view, probe);
      if (bound.entry >= _stop) {
        return false;
      }
      _entry = bound.entry;
      SetFixed(bound.entry, bound.at);
      _shared = Common(probe);
      return true;
    }
    if (_entry != kNoEntry && _pending.count == 0 && !_partial) {
      if (const size_t common = Common(probe); Before(probe, common)) {
        return SeekAhead(probe, common);
      }
    }
    const uint32_t restarts = RestartsNotAfter(_view, probe);
    Group(restarts == 0 ? 0 : restarts - 1);
    _shared = Common(probe);
    if (restarts == 0 || !Before(probe, _shared)) {
      return true;
    }
    return ScanTo(probe, _shared);
  }

  bool SeekAhead(Probe probe, size_t matched) {
    if (_view.fixed) {
      return LowerBound(probe);
    }
    SDB_ASSERT(_entry != kNoEntry);
    SDB_ASSERT(_pending.count == 0);
    const uint32_t from = _entry / kRestartInterval + 1;
    const uint32_t to = RestartsNotAfter(_view, probe, from);
    SDB_ASSERT(to >= from);
    if (to != from) {
      Group(to - 1);
      _shared = Common(probe);
      if (!Before(probe, _shared)) {
        return true;
      }
      matched = _shared;
    }
    return ScanTo(probe, matched);
  }

 private:
  template<typename Reject>
  IRS_FORCE_INLINE bool Rejects(Reject& reject, uint32_t entry,
                                const byte_type* sp, Header header) const {
    if (entry % kRestartInterval != 0) [[likely]] {
      return reject(sp, header, false, entry);
    }
    const uint32_t skip = header.lcp - _view.prefix;
    return reject(sp + skip,
                  Header{.lcp = header.lcp, .len = header.len - skip}, true,
                  entry);
  }

  size_t RestartMismatch(const byte_type* rest, size_t limit) const noexcept {
    const size_t prefix = _view.prefix;
    const size_t head = std::min(limit, prefix);
    const size_t common =
      PaddedMismatch(_key.data(), Prefix(_view).data(), head);
    if (common != head || limit <= prefix) {
      return common;
    }
    return prefix + PaddedMismatch(_key.data() + prefix, rest, limit - prefix);
  }

  size_t Common(Probe probe) const noexcept {
    return PaddedMismatch(_key.data(), probe.data, std::min(_size, probe.size));
  }

  bool Before(Probe probe, size_t common) const noexcept {
    return common == std::min(_size, probe.size)
             ? _size < probe.size
             : _key[common] < probe.data[common];
  }

  struct Span {
    uint32_t first;
    uint32_t end;
  };

  Span SkipSpan() const noexcept {
    if (_view.fixed || _entry == kNoEntry || _jumped) {
      return {.first = 0, .end = 0};
    }
    return {.first = _entry + 1, .end = _stop};
  }

  uint32_t Skipped(uint32_t first, uint32_t entry, uint32_t end,
                   const byte_type* sp, uint32_t lcp) noexcept {
    if (entry == first) {
      return kNoSkip;
    }
    _mat = std::min<size_t>(_mat, lcp);
    const uint32_t group = (entry - 1) / kRestartInterval;
    if (entry == end) {
      _size = _mat;
      _pending.count = 0;
      _partial = true;
    } else {
      uint32_t skipped = first;
      const auto* pending = _sp;
      if (group * kRestartInterval > first) {
        skipped = group * kRestartInterval;
        pending = _view.suffixes + SuffixOffset(_view, group);
      }
      for (; skipped != entry; ++skipped) {
        const auto header = HeaderAt(_view, skipped);
        if (skipped % kRestartInterval == 0) {
          PushRestart(pending, header);
        } else {
          PushPending(pending, header);
        }
        pending += header.len;
      }
    }
    _entry = entry - 1;
    _restart = (group + 1) * kRestartInterval;
    _sp = sp;
    return lcp;
  }

  void PushRestart(const byte_type* rest, Header header) noexcept {
    SDB_ASSERT(header.lcp >= _view.prefix &&
               header.lcp - _view.prefix < header.len);
    _partial = false;
    _pending.count = 0;
    const uint32_t skip = header.lcp - _view.prefix;
    _pending.suffixes[_pending.count++] = {
      .data = rest + skip,
      .lcp = header.lcp,
      .size = header.len - skip,
      .restart = true,
    };
  }

  void PushPending(const byte_type* data, Header header) noexcept {
    SDB_ASSERT(_pending.count < _pending.suffixes.size());
    _pending.suffixes[_pending.count++] = {
      .data = data,
      .lcp = header.lcp,
      .size = header.len,
    };
  }

  bool ScanTo(Probe probe, size_t matched) {
    SDB_ASSERT(_pending.count == 0);
    const auto* fsst = _view.coded ? _fsst : nullptr;
    const uint32_t end = std::min(_restart, _stop);
    const auto* sp = _sp;
    for (uint32_t entry = _entry + 1; entry != end; ++entry) {
      const auto header = HeaderAt(_view, entry);
      const auto* suffix = sp;
      sp += header.len;
      if (header.lcp > matched) {
        continue;
      }
      _sp = sp;
      _entry = entry;
      if (header.lcp < matched) {
        return Land(probe, header.lcp,
                    Expand({suffix, header.len}, header.lcp,
                           std::numeric_limits<uint32_t>::max()),
                    header.lcp);
      }
      const Probe rest{probe.data + matched, probe.size - matched};
      if (fsst) {
        auto* out =
          Reserve(header.lcp + size_t{header.len} * sizeof(uint64_t)) +
          header.lcp;
        const auto coded =
          CompareCoded<true>(*fsst, {suffix, header.len}, rest, out);
        matched += coded.compared.common;
        if (coded.compared.order == Order::Less) {
          continue;
        }
        size_t size = coded.decoded;
        if (coded.code != header.len) {
          size +=
            DecodeInto(*fsst, {suffix + coded.code, header.len - coded.code},
                       out + size, std::numeric_limits<uint32_t>::max());
        }
        return Land(probe, header.lcp, size, matched);
      }
      const auto compared = CompareRaw(suffix, header.len, rest);
      matched += compared.common;
      if (compared.order == Order::Less) {
        continue;
      }
      std::memcpy(Reserve(header.lcp + header.len) + header.lcp, suffix,
                  header.len);
      return Land(probe, header.lcp, header.len, matched);
    }
    if (end == _stop) {
      _entry = kNoEntry;
      return false;
    }
    _sp = sp;
    _entry = end - 1;
    std::memcpy(Reserve(matched), probe.data, matched);
    _mat = matched;
    Next();
    _shared = Common(probe);
    return true;
  }

  bool Land(Probe probe, size_t lcp, size_t size, size_t shared) {
    _partial = false;
    std::memcpy(_key.data(), probe.data, lcp);
    _size = lcp + size;
    _mat = _size;
    _shared = shared;
    return true;
  }

  byte_type* Reserve(size_t size) { return Grow(_key, size + kKeyPad); }

  IRS_NO_INLINE uint32_t NextVar() {
    const auto header = HeaderAt(_view, _entry);
    const bytes_view suffix{_sp, header.len};
    _sp += header.len;
    const bool jumped = std::exchange(_jumped, false);
    if (_entry == _restart) {
      _restart += kRestartInterval;
      return Restart(suffix, jumped || _entry == 0 ? kNoSkip : header.lcp);
    }
    Materialize();
    _size = header.lcp +
            Expand(suffix, header.lcp, std::numeric_limits<uint32_t>::max());
    _mat = _size;
    return header.lcp;
  }

  uint32_t Restart(bytes_view rest, uint32_t lcp) {
    const size_t prefix = _view.prefix;
    const size_t size = prefix + rest.size();
    const auto shared = lcp != kNoSkip
                          ? lcp
                          : static_cast<uint32_t>(RestartMismatch(
                              rest.data(), std::min<size_t>(_mat, size)));
    _pending.count = 0;
    const auto from = static_cast<uint32_t>(std::min<size_t>(shared, _mat));
    RestartBytes(_view, rest.data(), from, static_cast<uint32_t>(size),
                 Reserve(size) + from);
    _size = size;
    _mat = _size;
    return shared;
  }

  size_t Expand(bytes_view suffix, uint32_t lcp, uint32_t need) {
    if (!_view.coded) {
      const size_t size = std::min<size_t>(suffix.size(), need);
      std::memcpy(Reserve(lcp + size) + lcp, suffix.data(), size);
      return size;
    }
    auto* key = Reserve(lcp + suffix.size() * sizeof(uint64_t)) + lcp;
    return DecodeInto(*_fsst, suffix, key, need);
  }

  uint32_t SetFixed(uint32_t entry, uint32_t at = kNoEntry) {
    if (!PacksKeys(_view.stride)) {
      StrideBuffer buffer;
      return SetKey(FixedKey(_view, entry, 0, buffer));
    }
    const auto& ef = _view.ef;
    if (at == kNoEntry) {
      at = _fixed.entry == entry ? _fixed.at
           : _fixed.entry != kNoEntry && _fixed.entry + 1 == entry
             ? ef.After(_fixed.at)
             : ef.At(entry);
    }
    const uint64_t value = ef.base + ef.ValueAt(entry, at);
    const uint32_t stride = _view.stride;
    const uint32_t shift = 8 * (sizeof(uint64_t) - stride);
    if (_fixed.entry == kNoEntry) {
      StrideBuffer buffer;
      absl::big_endian::Store64(buffer.data(), value << shift);
      _fixed.entry = entry;
      _fixed.at = at;
      _fixed.value = value;
      return SetKey({buffer.data(), stride});
    }
    const uint64_t diff = (value ^ _fixed.value) << shift;
    const uint32_t shared =
      diff == 0 ? stride : static_cast<uint32_t>(std::countl_zero(diff)) / 8;
    absl::big_endian::Store64(Reserve(_view.prefix + stride) + _view.prefix,
                              value << shift);
    _size = _view.prefix + stride;
    _mat = _size;
    _fixed.entry = entry;
    _fixed.at = at;
    _fixed.value = value;
    return _view.prefix + shared;
  }

  uint32_t SetKey(bytes_view stride) {
    const bytes_view prefix = Prefix(_view);
    size_t lcp = CommonPrefixLength(Key(), prefix);
    const bool same = lcp == prefix.size();
    if (same) {
      lcp += CommonPrefixLength(Key().substr(lcp), stride);
    }
    auto* key = Reserve(prefix.size() + stride.size());
    if (!same) {
      std::memcpy(key, prefix.data(), prefix.size());
    }
    std::memcpy(key + prefix.size(), stride.data(), stride.size());
    _size = prefix.size() + stride.size();
    _mat = _size;
    return static_cast<uint32_t>(lcp);
  }

  struct FixedAt {
    uint64_t value = 0;
    uint32_t entry = kNoEntry;
    uint32_t at = 0;
  };

  struct Pendings {
    std::array<PendingSuffix, kRestartInterval> suffixes;
    uint32_t count = 0;
  };

  const duckdb_fsst_decoder_t* _fsst;
  uint64_t _generation = 0;
  BlockView _view{};
  const byte_type* _sp = nullptr;
  size_t _size = 0;
  size_t _mat = 0;
  size_t _shared = 0;
  uint32_t _entry = kNoEntry;
  uint32_t _restart = 0;
  uint32_t _stop = 0;
  bool _partial = false;
  bool _jumped = false;
  FixedAt _fixed;
  Pendings _pending;
  KeyBuffer _key;
};

}  // namespace irs::term_dict
