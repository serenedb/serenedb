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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kSlack = block_codec::kInSlack;
inline constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kVarRestart = sizeof(uint16_t);

constexpr uint32_t ColumnWidth(uint32_t max) noexcept {
  return max <= 0xFF ? 1 : max <= 0xFFFF ? 2 : 4;
}

enum class Position : uint8_t {
  Before,
  At,
  After,
};

struct BlockView {
  BlockKind kind;
  bool coded;
  uint32_t count;
  uint32_t restarts;
  uint32_t stride;
  uint32_t prefix;
  uint32_t head_skip;
  uint32_t width;
  const byte_type* table;
  const byte_type* heads;
  const byte_type* record_offsets;
  const byte_type* lcps;
  const byte_type* lens;
  const byte_type* tails;
  const byte_type* befores;
  const byte_type* suffixes;
  const byte_type* keys;
  const byte_type* records;
};

inline uint32_t SuffixOffset(const BlockView& view, uint32_t restart) noexcept {
  return absl::little_endian::Load16(view.table + kVarRestart * restart);
}

inline uint32_t RecordOffset(const BlockView& view, uint32_t group) noexcept {
  return absl::little_endian::Load16(view.record_offsets +
                                     kRecordOffset * group);
}

inline bytes_view Prefix(const BlockView& view) noexcept {
  return {view.table - view.prefix, view.prefix};
}

inline bytes_view FixedKey(const BlockView& view, uint32_t entry) noexcept {
  return {view.keys + uint64_t{entry} * view.stride, view.stride};
}

inline BlockView Parse(const byte_type* p) {
  BlockView view{};
  view.kind = static_cast<BlockKind>(*p++);
  view.coded = view.kind == BlockKind::VarFsst;
  if (view.coded) {
    view.kind = BlockKind::Var;
  }
  view.count = vread<uint32_t>(p);
  if (view.kind == BlockKind::Var) {
    view.restarts = vread<uint32_t>(p);
    view.width = *p++;
    const auto suffixes = vread<uint64_t>(p);
    vskip<uint64_t>(p);
    view.head_skip = vread<uint32_t>(p);
    view.table = p;
    view.heads = p + kVarRestart * view.restarts;
    view.record_offsets = view.heads + kHeadBytes * view.restarts;
    view.lcps = view.record_offsets + kRecordOffset * RecordGroups(view.count);
    view.lens = view.lcps + uint64_t{view.count} * view.width;
    view.tails = view.lens + uint64_t{view.count} * view.width;
    view.befores = view.tails + view.count;
    view.suffixes = view.befores + view.count;
    view.records = view.suffixes + suffixes;
    return view;
  }
  if (view.kind == BlockKind::FixedStride) {
    const auto groups = vread<uint32_t>(p);
    vskip<uint64_t>(p);
    view.stride = vread<uint32_t>(p);
    view.prefix = vread<uint32_t>(p);
    view.table = p + view.prefix;
    view.restarts = groups;
    view.record_offsets = view.table;
    view.heads = view.record_offsets + kRecordOffset * groups;
    view.keys = view.heads + kHeadBytes * groups;
    view.records = view.keys + uint64_t{view.count} * view.stride;
    return view;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: unknown block kind ",
                          static_cast<uint32_t>(view.kind)));
}

struct Header {
  uint32_t lcp;
  uint32_t len;
};

IRS_FORCE_INLINE inline uint32_t ColumnAt(const byte_type* column,
                                          uint32_t width,
                                          uint32_t entry) noexcept {
  if (width == 1) [[likely]] {
    return column[entry];
  }
  if (width == 2) {
    return absl::little_endian::Load16(column + 2 * uint64_t{entry});
  }
  return absl::little_endian::Load32(column + 4 * uint64_t{entry});
}

IRS_FORCE_INLINE inline Header HeaderAt(const BlockView& view,
                                        uint32_t entry) noexcept {
  return {
    .lcp = ColumnAt(view.lcps, view.width, entry),
    .len = ColumnAt(view.lens, view.width, entry),
  };
}

class TailSet {
 public:
  void Add(byte_type b) noexcept {
    _bits[b >> 6] |= uint64_t{1} << (b & 63);
    auto& rows = b >> 4 < 8 ? _low : _high;
    rows[b & 0x0F] |= static_cast<byte_type>(1 << ((b >> 4) & 7));
  }

  bool Contains(byte_type b) const noexcept {
    return (_bits[b >> 6] >> (b & 63)) & 1;
  }

  IRS_FORCE_INLINE uint32_t Find(const byte_type* tails,
                                 uint32_t count) const noexcept {
    SDB_ASSERT(count <= kRestartInterval);
#ifdef __AVX2__
    const auto bytes =
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(tails));
    const auto nibble = _mm_set1_epi8(0x0F);
    const auto lo = _mm_and_si128(bytes, nibble);
    const auto hi = _mm_and_si128(_mm_srli_epi16(bytes, 4), nibble);
    const auto rows = _mm_blendv_epi8(
      _mm_shuffle_epi8(Load(_low), lo), _mm_shuffle_epi8(Load(_high), lo),
      _mm_cmpgt_epi8(hi, _mm_set1_epi8(7)));
    const auto bits = _mm_shuffle_epi8(
      _mm_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128),
      hi);
    const auto found = _mm_cmpeq_epi8(_mm_and_si128(rows, bits), bits);
    return static_cast<uint32_t>(_mm_movemask_epi8(found)) &
           ((uint32_t{1} << count) - 1);
#else
    uint32_t found = 0;
    for (uint32_t i = 0; i != count; ++i) {
      found |= uint32_t{Contains(tails[i])} << i;
    }
    return found;
#endif
  }

 private:
#ifdef __AVX2__
  static __m128i Load(const std::array<byte_type, 16>& rows) noexcept {
    return _mm_load_si128(reinterpret_cast<const __m128i*>(rows.data()));
  }
#endif

  std::array<uint64_t, 4> _bits{};
  alignas(16) std::array<byte_type, 16> _low{};
  alignas(16) std::array<byte_type, 16> _high{};
};

inline size_t DecodeInto(const duckdb_fsst_decoder_t& decoder, bytes_view codes,
                         byte_type* out, size_t limit) noexcept {
  size_t pos = 0;
  for (size_t i = 0; i != codes.size() && pos < limit;) {
    const size_t code = codes[i++];
    if (code != FSST_ESC) [[likely]] {
      std::memcpy(out + pos, &decoder.symbol[code], sizeof(uint64_t));
      pos += decoder.len[code];
    } else {
      out[pos++] = codes[i++];
    }
  }
  return pos;
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
    const size_t code = codes[i++];
    uint64_t symbol;
    size_t len;
    if (code != FSST_ESC) [[likely]] {
      symbol = absl::little_endian::Load64(
        reinterpret_cast<const byte_type*>(&decoder.symbol[code]));
      len = decoder.len[code];
    } else {
      symbol = codes[i++];
      len = 1;
    }
    if constexpr (Materialize) {
      absl::little_endian::Store64(out + pos, symbol);
    }
    const size_t left = rest.size - pos;
    const uint64_t diff = symbol ^ absl::little_endian::Load64(rest.data + pos);
    const size_t at = std::countr_zero(diff) / 8;
    if (at < std::min(len, left)) {
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
  SDB_ASSERT(header.lcp == 0);
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
  using U32x4 = uint32_t __attribute__((vector_size(16)));
  using I16x4 = int16_t __attribute__((vector_size(8)));
  constexpr uint32_t kLanes = 4;
  uint32_t not_after = from;
  for (uint32_t i = from; i < count; i += kLanes) {
    U32x4 lanes;
    std::memcpy(&lanes, heads + i * kHeadBytes, sizeof(lanes));
    const auto bits =
      std::bit_cast<uint64_t>(__builtin_convertvector(lanes <= head, I16x4));
    const auto run = std::min<uint32_t>(
      count - i, static_cast<uint32_t>(std::countr_one(bits)) / 16);
    not_after = i + run;
    if (run != kLanes) {
      break;
    }
  }
  uint32_t less = not_after;
  while (less != from &&
         absl::little_endian::Load32(heads + (less - 1) * kHeadBytes) == head) {
    --less;
  }
  return {.less = less, .not_after = not_after};
}

inline uint32_t RestartsNotAfter(const BlockView& view, Probe probe,
                                 uint32_t from = 0) noexcept {
  const size_t skip = view.head_skip;
  const auto* first = view.suffixes;
  SDB_ASSERT(SuffixOffset(view, 0) == 0);
  SDB_ASSERT(skip <= RestartKey(view, 0).size());
  const size_t shared = std::min(skip, probe.size);
  if (const size_t common = PaddedMismatch(first, probe.data, shared);
      common != shared) {
    return first[common] < probe.data[common] ? view.restarts : 0;
  }
  if (probe.size < skip) {
    return 0;
  }
  const auto counts = CountHeads(view.heads, from, view.restarts,
                                 absl::big_endian::Load32(probe.data + skip));
  return static_cast<uint32_t>(
    PartitionIndex(counts.less, counts.not_after, [&](size_t restart) {
      const auto key = RestartKey(view, static_cast<uint32_t>(restart));
      return CompareRaw(key.data(), key.size(), probe).order != Order::Greater;
    }));
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
  bool equal;
};

inline Bound VarLowerBound(const BlockView& view, Probe probe,
                           const duckdb_fsst_decoder_t* fsst) noexcept {
  const uint32_t restarts = RestartsNotAfter(view, probe);
  if (restarts == 0) {
    return {.entry = 0, .equal = false};
  }
  const uint32_t group = restarts - 1;
  const uint32_t first = group * kRestartInterval;
  const uint32_t end = std::min(first + kRestartInterval, view.count);
  const auto* sp = view.suffixes + SuffixOffset(view, group);
  size_t matched = 0;
  for (uint32_t entry = first; entry != end; ++entry) {
    const auto header = HeaderAt(view, entry);
    const auto* suffix = sp;
    sp += header.len;
    const auto order =
      Locate(suffix, header.len, header.lcp, probe,
             view.coded && entry != first ? fsst : nullptr, matched);
    if (order != Order::Less) {
      return {.entry = entry, .equal = order == Order::Equal};
    }
  }
  return {.entry = end, .equal = false};
}

inline uint32_t FixedLowerBound(const BlockView& view, Probe probe) noexcept {
  const auto prefix = Prefix(view);
  const size_t head = std::min(prefix.size(), probe.size);
  if (const size_t common = PaddedMismatch(prefix.data(), probe.data, head);
      common != head) {
    return prefix[common] < probe.data[common] ? view.count : 0;
  }
  if (probe.size < prefix.size()) {
    return 0;
  }
  const Probe rest{probe.data + prefix.size(), probe.size - prefix.size()};
  const uint32_t stride = view.stride;
  const bool longer = rest.size > stride;
  const auto search = [&](auto&& less) {
    const auto counts = CountHeads(view.heads, 0, view.restarts,
                                   absl::big_endian::Load32(rest.data));
    const auto groups = static_cast<uint32_t>(PartitionIndex(
      counts.less, counts.not_after,
      [&](size_t group) { return less(group * kRecordInterval); }));
    if (groups == 0) {
      return uint32_t{0};
    }
    const uint32_t from = (groups - 1) * kRecordInterval + 1;
    const uint32_t to = std::min(groups * kRecordInterval, view.count);
    return static_cast<uint32_t>(PartitionIndex(from, to, less));
  };
  if (stride == 0) {
    return rest.size == 0 ? 0 : view.count;
  }
  if (stride <= sizeof(uint64_t)) {
    const uint32_t shift = 8 * (sizeof(uint64_t) - stride);
    const uint64_t value = absl::big_endian::Load64(rest.data) >> shift;
    return search([&](size_t i) {
      const uint64_t key =
        absl::big_endian::Load64(view.keys + i * stride) >> shift;
      return key < value || (longer && key == value);
    });
  }
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

struct PendingSuffix {
  const byte_type* data;
  uint32_t lcp;
  uint32_t size;
};

inline const byte_type* GroupStart(const BlockView& view,
                                   uint32_t group) noexcept {
  return view.records + RecordOffset(view, group);
}

inline void DecodeRecords(const byte_type*& record, uint32_t count,
                          IndexFeatures features, PostingMeta& meta) noexcept {
  for (; count != 0; --count) {
    record += PostingsReader::DecodeDetached(record, features, meta);
  }
}

inline const byte_type* FieldRecords(const TermReaderImpl& field,
                                     const BlockView& view, uint32_t block,
                                     uint32_t entry, uint32_t& count) noexcept {
  const uint32_t group = entry / kRecordInterval;
  const uint32_t first = field.FirstEntry();
  if (block == 0 && first > group * kRecordInterval) {
    SDB_ASSERT(entry >= first);
    count = entry - first + 1;
    return view.records + field.FirstRecord();
  }
  count = entry % kRecordInterval + 1;
  return GroupStart(view, group);
}

class Cursor {
 public:
  explicit Cursor(const TermReaderImpl& field) noexcept
    : _field{&field}, _end{field.Blocks()} {}

  void ReadaheadUntil(uint32_t end, bool eager) noexcept {
    _end = std::min(end, _field->Blocks());
    _eager = eager;
    _inline_ahead = 0;
  }

  const TermReaderImpl& Field() const noexcept { return *_field; }
  bytes_view Key() const noexcept {
    SDB_ASSERT(_pendings == 0);
    return {_key.data(), _size};
  }
  bytes_view Term() const noexcept {
    return Key().substr(_field->KeyPrefix().size());
  }
  const PendingSuffix* Pending() const noexcept {
    return _pendings == 0 ? nullptr : &_pending[_pendings - 1];
  }
  const BlockView& View() const noexcept { return _view; }
  uint32_t BlockIndex() const noexcept { return _block; }
  uint32_t Entry() const noexcept { return _entry; }
  const byte_type* NextSuffix() const noexcept { return _sp; }
  bool AtBlockEnd() const noexcept {
    return _entry != kNoEntry && _entry + 1 == _stop;
  }

  int ByteAt(size_t depth) {
    if (depth < _mat) {
      return _key[depth];
    }
    if (const auto* last = Pending(); last && depth == last->lcp) {
      const auto* data = last->data;
      SDB_ASSERT(last->size != 0);
      if (!_view.coded) {
        return data[0];
      }
      if (data[0] == FSST_ESC) {
        return data[1];
      }
      return static_cast<byte_type>(_field->Fsst()->symbol[data[0]]);
    }
    Materialize();
    return depth < _size ? _key[depth] : -1;
  }

  IRS_FORCE_INLINE uint32_t Skip() {
    if (_view.kind != BlockKind::Var) {
      return Next();
    }
    const auto entry = _entry == kNoEntry ? 0 : _entry + 1;
    if (entry == _restart) {
      return Next();
    }
    _entry = entry;
    SDB_ASSERT(_entry < _view.count);
    const auto header = HeaderAt(_view, entry);
    PushPending(_sp, header);
    _mat = std::min<size_t>(_mat, header.lcp);
    _sp += header.len;
    return header.lcp;
  }

  template<typename Reject>
  IRS_FORCE_INLINE uint32_t SkipWhile(Reject&& reject) {
    constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
    if (_view.kind != BlockKind::Var || _entry == kNoEntry) {
      return kNone;
    }
    const uint32_t first = _entry + 1;
    const uint32_t end = std::min(_restart, _stop);
    const auto* sp = _sp;
    uint32_t entry = first;
    uint32_t lcp = kNone;
    for (; entry < end; ++entry) {
      const auto header = HeaderAt(_view, entry);
      if (!reject(sp, header.lcp, header.len, _view.befores[entry])) {
        break;
      }
      lcp = std::min(lcp, header.lcp);
      sp += header.len;
    }
    return Skipped(first, entry, end, sp, lcp);
  }

  template<typename Reject>
  IRS_FORCE_INLINE uint32_t SkipTails(size_t prefix, const TailSet& tails,
                                      Reject&& reject) {
    constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
    if (_view.kind != BlockKind::Var || _entry == kNoEntry) {
      return kNone;
    }
    const uint32_t first = _entry + 1;
    const uint32_t end = std::min(_restart, _stop);
    if (first >= end) {
      return kNone;
    }
    uint32_t candidates = tails.Find(_view.tails + first, end - first);
    const auto* sp = _sp;
    uint32_t entry = first;
    uint32_t lcp = kNone;
    for (;;) {
      const uint32_t next =
        candidates == 0 ? end : first + std::countr_zero(candidates);
      for (; entry != next; ++entry) {
        const auto header = HeaderAt(_view, entry);
        if (header.lcp < prefix) {
          return Skipped(first, entry, end, sp, lcp);
        }
        lcp = std::min(lcp, header.lcp);
        sp += header.len;
      }
      if (entry == end) {
        break;
      }
      candidates &= candidates - 1;
      const auto header = HeaderAt(_view, entry);
      if (header.lcp < prefix ||
          !reject(sp, header.len, header.lcp, _view.befores[entry])) {
        break;
      }
      lcp = std::min(lcp, header.lcp);
      sp += header.len;
      ++entry;
    }
    return Skipped(first, entry, end, sp, lcp);
  }

  bytes_view PeekNext() {
    SDB_ASSERT(_view.kind == BlockKind::Var);
    SDB_ASSERT(_entry != kNoEntry && _entry + 1 < std::min(_restart, _stop));
    Materialize();
    const auto header = HeaderAt(_view, _entry + 1);
    const size_t bound =
      header.lcp + size_t{header.len} * (_view.coded ? sizeof(uint64_t) : 1);
    if (_peek.size() < bound + kKeyPad) {
      utils::StrResizeAmortized(_peek, bound + kKeyPad);
    }
    auto* out = _peek.data();
    std::memcpy(out, _key.data(), header.lcp);
    if (_view.coded) {
      _peeked = header.lcp + DecodeInto(*_field->Fsst(), {_sp, header.len},
                                        out + header.lcp,
                                        std::numeric_limits<uint32_t>::max());
    } else {
      std::memcpy(out + header.lcp, _sp, header.len);
      _peeked = header.lcp + header.len;
    }
    return {out, _peeked};
  }

  uint32_t TakePeeked() noexcept {
    const auto header = HeaderAt(_view, ++_entry);
    _sp += header.len;
    std::swap(_key, _peek);
    _size = _peeked;
    _mat = _size;
    return header.lcp;
  }

  void Materialize() {
    if (_pendings == 0) {
      return;
    }
    std::array<uint32_t, kRestartInterval> needs;
    uint32_t need = std::numeric_limits<uint32_t>::max();
    for (uint32_t i = _pendings; i-- != 0;) {
      const auto lcp = _pending[i].lcp;
      needs[i] = need > lcp ? need - lcp : 0;
      need = std::min(need, lcp);
    }
    for (uint32_t i = 0; i != _pendings; ++i) {
      const auto& pending = _pending[i];
      if (needs[i] != 0) {
        _size = pending.lcp +
                Expand({pending.data, pending.size}, pending.lcp, needs[i]);
      }
    }
    _mat = _size;
    _pendings = 0;
  }

  void Load(uint32_t block) {
    _pendings = 0;
    _partial = false;
    _size = _mat;
    if (_block != kNoEntry && block == _block + 1) {
      ReadAhead(block);
      FetchLines(block + 1);
    } else if (_block != kNoEntry && block > _block) {
      _ahead = std::max(_ahead, block + 1);
      _window = std::max(kFirstWindow, _window / 2);
    } else {
      _ahead = block + 1;
      _window = _eager ? kLastWindow : kFirstWindow;
      _issued = 0;
      if (_eager) {
        ReadAhead(block);
      }
    }
    _block = kNoEntry;
    _entry = kNoEntry;
    _record = kNoEntry;
    _restart = 0;
    _view = Parse(_field->Block(block, _buffer));
    _stop =
      block + 1 == _field->Blocks() ? _field->LastEntry() + 1 : _view.count;
    _block = block;
    _sp = _view.suffixes;
  }

  void First() {
    Load(0);
    if (_field->FirstEntry() == 0) {
      Next();
      return;
    }
    LowerBound(_probe.Assign(_field->MinKey()));
    SDB_ASSERT(_entry == _field->FirstEntry());
  }

  uint32_t Next() {
    _partial = false;
    _entry = _entry == kNoEntry ? 0 : _entry + 1;
    SDB_ASSERT(_entry < _view.count);
    if (_view.kind != BlockKind::Var) {
      return SetFixed(_entry);
    }
    const auto header = HeaderAt(_view, _entry);
    const bytes_view suffix{_sp, header.len};
    _sp += header.len;
    if (_entry == _restart) {
      _restart += kRestartInterval;
      return Restart(suffix);
    }
    Materialize();
    _size = header.lcp +
            Expand(suffix, header.lcp, std::numeric_limits<uint32_t>::max());
    _mat = _size;
    return header.lcp;
  }

  bool Advance() {
    if (AtBlockEnd()) {
      if (_block + 1 >= _field->Blocks()) {
        return false;
      }
      Load(_block + 1);
    }
    Next();
    return true;
  }

  size_t Shared() const noexcept { return _shared; }

  SeekResult SeekGe(bytes_view key) {
    const auto& field = *_field;
    if (field.size() == 0 || key > field.max()) {
      return SeekResult::End;
    }
    const auto probe = _probe.Assign(field.KeyPrefix(), key);
    uint32_t block = 0;
    if (!field.Small() && key > field.min()) {
      block = static_cast<uint32_t>(field.GetRoot().Find(_probe.Key()));
    }
    if (block != _block || _entry == kNoEntry) {
      Load(block);
    }
    if (!LowerBound(probe)) {
      SDB_ASSERT(block + 1 < field.Blocks());
      Load(block + 1);
      Next();
      return SeekResult::NotFound;
    }
    return _shared == probe.size && _size == probe.size ? SeekResult::Found
                                                        : SeekResult::NotFound;
  }

  const PostingMeta& Meta() {
    if (_record == _entry) {
      return _meta;
    }
    const auto features = _field->meta().index_features;
    if (_record != kNoEntry && _record < _entry &&
        _record / kRecordInterval == _entry / kRecordInterval) {
      DecodeRecords(_records, _entry - _record, features, _meta);
    } else {
      uint32_t count = 0;
      _records = FieldRecords(*_field, _view, _block, _entry, count);
      _meta.clear();
      DecodeRecords(_records, count, features, _meta);
    }
    if (_meta.inline_size != 0) {
      if (!_field->Small()) {
        InlineAhead(_field->InlineBegin() + _meta.inline_end -
                    _meta.inline_size);
      }
      _field->ReadInline(_meta, _buffer);
    }
    _record = _entry;
    return _meta;
  }

  bool LowerBound(Probe probe) {
    if (_view.kind == BlockKind::FixedStride) {
      const auto entry = FixedLowerBound(_view, probe);
      if (entry == _view.count) {
        return false;
      }
      _entry = entry;
      SetFixed(entry);
      _shared = Common(probe);
      return true;
    }
    if (_entry != kNoEntry && _pendings == 0 && !_partial) {
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
    if (_view.kind != BlockKind::Var) {
      return LowerBound(probe);
    }
    SDB_ASSERT(_entry != kNoEntry);
    SDB_ASSERT(_pendings == 0);
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
  size_t Common(Probe probe) const noexcept {
    return PaddedMismatch(_key.data(), probe.data, std::min(_size, probe.size));
  }

  bool Before(Probe probe, size_t common) const noexcept {
    return common == std::min(_size, probe.size)
             ? _size < probe.size
             : _key[common] < probe.data[common];
  }

  uint32_t Skipped(uint32_t first, uint32_t entry, uint32_t end,
                   const byte_type* sp, uint32_t lcp) noexcept {
    if (entry == first) {
      return std::numeric_limits<uint32_t>::max();
    }
    _mat = std::min<size_t>(_mat, lcp);
    if (entry == end) {
      _size = _mat;
      _pendings = 0;
      _partial = true;
    } else {
      const auto* pending = _sp;
      for (uint32_t skipped = first; skipped != entry; ++skipped) {
        const auto header = HeaderAt(_view, skipped);
        PushPending(pending, header);
        pending += header.len;
      }
    }
    _entry = entry - 1;
    _sp = sp;
    return lcp;
  }

  void PushPending(const byte_type* data, Header header) noexcept {
    SDB_ASSERT(_pendings < _pending.size());
    _pending[_pendings++] = {
      .data = data,
      .lcp = header.lcp,
      .size = header.len,
    };
  }

  void Group(uint32_t group) {
    _entry = group == 0 ? kNoEntry : group * kRestartInterval - 1;
    _restart = group * kRestartInterval;
    _sp = _view.suffixes + SuffixOffset(_view, group);
    Next();
  }

  bool ScanTo(Probe probe, size_t matched) {
    SDB_ASSERT(_pendings == 0);
    const auto* fsst = _view.coded ? _field->Fsst() : nullptr;
    const uint32_t end = std::min(_restart, _view.count);
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
    if (end == _view.count) {
      _entry = kNoEntry;
      return false;
    }
    _sp = sp;
    _entry = end - 1;
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

  byte_type* Reserve(size_t size) {
    if (_key.size() < size + kKeyPad) {
      utils::StrResizeAmortized(_key, size + kKeyPad);
    }
    return _key.data();
  }

  uint32_t Restart(bytes_view key) {
    const auto shared = static_cast<uint32_t>(PaddedMismatch(
      _key.data(), key.data(), std::min<size_t>(_mat, key.size())));
    _pendings = 0;
    std::memcpy(Reserve(key.size()), key.data(), key.size());
    _size = key.size();
    _mat = _size;
    return shared;
  }

  size_t Expand(bytes_view suffix, uint32_t lcp, uint32_t need) {
    if (!_view.coded) {
      const size_t size = std::min<size_t>(suffix.size(), need);
      std::memcpy(Reserve(lcp + size) + lcp, suffix.data(), size);
      return size;
    }
    auto* key = Reserve(lcp + suffix.size() * sizeof(uint64_t));
    return DecodeInto(*_field->Fsst(), suffix, key + lcp, need);
  }

  uint32_t SetFixed(uint32_t entry) {
    const bytes_view prefix = Prefix(_view);
    const bytes_view stride = FixedKey(_view, entry);
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

  void FetchLines(uint32_t block) const noexcept {
    constexpr uint64_t kLine = 64;
    const auto* base = _field->Owner().Base();
    if (!base || block >= _field->Blocks()) {
      return;
    }
    const uint64_t end = _field->BlockOffset(block + 1);
    for (uint64_t at = _field->BlockOffset(block); at < end; at += kLine) {
      __builtin_prefetch(base + at);
    }
  }

  void ReadAhead(uint32_t block) noexcept {
    if (_ahead >= _end || block + 1 + _issued / 2 < _ahead) {
      return;
    }
    const uint32_t from = std::max(block + 1, _ahead);
    const uint32_t to = std::min(_end, from + _window);
    if (from >= to) {
      return;
    }
    const uint64_t offset = _field->BlockOffset(from);
    file_utils::Hint(_field->Owner().Input(), offset,
                     _field->BlockOffset(to) - offset, _issued == 0);
    _ahead = to;
    _issued = to - from;
    _window = std::min(2 * _window, kLastWindow);
  }

  void InlineAhead(uint64_t at) noexcept {
    if (at + _inline_issued / 2 < _inline_ahead) {
      return;
    }
    if (_inline_ahead == 0) {
      _inline_ahead = at - at % file_utils::kPage + file_utils::kPage;
      return;
    }
    const uint64_t from = std::max(at, _inline_ahead);
    const uint64_t to = std::min(_field->InlineEnd(), from + _inline_window);
    if (from >= to) {
      return;
    }
    file_utils::Hint(_field->Owner().Input(), from, to - from,
                     _inline_issued == 0);
    _inline_ahead = to;
    _inline_issued = to - from;
    _inline_window = std::min(2 * _inline_window, file_utils::kMaxReadahead);
  }

  static constexpr uint32_t kFirstWindow = 4;
  static constexpr uint32_t kLastWindow =
    file_utils::kMaxReadahead / file_utils::kPage;

  const TermReaderImpl* _field;
  BlockBuffer _buffer;
  BlockView _view{};
  const byte_type* _sp = nullptr;
  const byte_type* _records = nullptr;
  bstring _key;
  bstring _peek;
  size_t _peeked = 0;
  PaddedKey _probe;
  PostingMeta _meta;
  std::array<PendingSuffix, kRestartInterval> _pending;
  uint32_t _pendings = 0;
  size_t _size = 0;
  size_t _mat = 0;
  size_t _shared = 0;
  uint32_t _block = kNoEntry;
  uint32_t _entry = kNoEntry;
  uint32_t _record = kNoEntry;
  uint32_t _restart = 0;
  uint32_t _stop = 0;
  bool _partial = false;
  uint32_t _ahead = 0;
  uint32_t _window = kFirstWindow;
  uint32_t _issued = 0;
  uint64_t _inline_ahead = std::numeric_limits<uint64_t>::max();
  uint64_t _inline_issued = 0;
  uint64_t _inline_window = file_utils::kPrefetchChunk;
  uint32_t _end;
  bool _eager = false;
};

template<bool HasPayload>
class SingleIterator : public SeekTermIterator {
 public:
  explicit SingleIterator(const TermReaderImpl& field,
                          byte_type payload = 0) noexcept
    : _field{&field}, _payload{payload} {
    if constexpr (HasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (HasPayload) {
      if (type == irs::Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    if (_position != Position::Before) {
      return End();
    }
    _position = Position::At;
    _term.value = _field->min();
    return true;
  }

  SeekResult seek_ge(bytes_view target) final {
    const auto term = _field->min();
    if (term < target) {
      End();
      return SeekResult::End;
    }
    _position = Position::At;
    _term.value = term;
    return term == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _field->SingleMeta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _field->Owner().Postings().Postings(
      _field->meta().index_features, features, _field->SingleMeta(),
      _field->HasScoreBounds(), std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  const TermReaderImpl* _field;
  TermAttr _term;
  PayAttr _pay;
  byte_type _payload;
  Position _position = Position::Before;
};

template<typename A>
SeekTermIterator::ptr SingleFor(const TermReaderImpl& field, const A& a) {
  if constexpr (A::kHasPayload) {
    typename A::PayloadType payload{};
    if (a.Matches(field.min(), payload)) {
      return memory::make_managed<SingleIterator<true>>(field, payload);
    }
  } else if (a.Matches(field.min())) {
    return memory::make_managed<SingleIterator<false>>(field);
  }
  return SeekTermIterator::empty();
}

}  // namespace irs::term_dict
