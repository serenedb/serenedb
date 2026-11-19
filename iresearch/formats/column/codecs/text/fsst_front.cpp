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

#include "iresearch/formats/column/codecs/text/fsst_front.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/text_encoder.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text::fsst_front {

constexpr uint64_t kChainGroupCost = 180;
constexpr uint64_t kChainMinRowBytes = 8;

struct ChainEnd {
  uint64_t written;
  uint64_t enc_off;
};

inline uint64_t ChainCapacity(PackedReader& lengths, PackedReader& lcps,
                              uint32_t first, uint32_t end) {
  uint64_t capacity = 0;
  for (auto e = first; e < end; ++e) {
    capacity += (e == first ? 0 : lcps.At(e)) +
                uint64_t{lengths.At(e)} * fsst::kMaxExpansion;
  }
  return capacity;
}

template<typename Impl>
uint64_t DecodeSuffix(const fsst::Decoder<Impl>& fsst, const char* in,
                      uint64_t enc, uint64_t lcp, std::string& buf) {
  const auto room = enc * fsst::kMaxExpansion;
  auto* at = Reserve(buf, lcp + room) + lcp;
  const auto dec = enc == 0 ? 0 : fsst.Decode(in, enc, at, room);
  SDB_ENSURE(dec <= room, "col codec: corrupted entry");
  return lcp + dec;
}

template<typename Impl, typename Visit>
ChainEnd WalkChain(const fsst::Decoder<Impl>& fsst, const char* in,
                   PackedReader& lengths, PackedReader& lcps, uint32_t first,
                   uint32_t end, uint64_t enc_off, uint64_t enc_end, char* out,
                   uint64_t capacity, Visit&& visit) {
  uint64_t off = 0;
  uint64_t prev_off = 0;
  uint64_t prev_len = 0;
  for (auto e = first; e < end; ++e) {
    const uint64_t lcp = e == first ? 0 : lcps.At(e);
    const uint64_t enc = lengths.At(e);
    SDB_ENSURE(
      lcp <= prev_len && enc_off + enc <= enc_end && off + lcp <= capacity,
      "col codec: corrupted front coding");
    if (lcp != 0) {
      std::memcpy(out + off, out + prev_off, lcp);
    }
    const auto room = capacity - (off + lcp);
    const auto dec =
      enc == 0 ? 0 : fsst.Decode(in + enc_off, enc, out + off + lcp, room);
    SDB_ENSURE(dec <= room, "col codec: corrupted entry");
    const std::string_view value{out + off, lcp + dec};
    visit(e, value);
    prev_off = off;
    prev_len = value.size();
    off += value.size();
    enc_off += enc;
  }
  return {off, enc_off};
}

template<typename Impl, typename Visit>
uint32_t WalkInPlace(const fsst::Decoder<Impl>& fsst, const char* in,
                     PackedReader& lengths, PackedReader& lcps, uint32_t first,
                     uint32_t end, uint64_t enc_off, uint64_t enc_end,
                     std::string& buf, Visit&& visit) {
  uint64_t len = 0;
  for (auto e = first; e < end; ++e) {
    const uint64_t lcp = e == first ? 0 : lcps.At(e);
    const uint64_t enc = lengths.At(e);
    SDB_ENSURE(lcp <= len && enc_off + enc <= enc_end,
               "col codec: corrupted front coding");
    len = DecodeSuffix(fsst, in + enc_off, enc, lcp, buf);
    if (!visit(e, std::string_view{buf.data(), len})) {
      return e;
    }
    enc_off += enc;
  }
  return end;
}

template<typename Impl>
std::string_view DecodeChained(const fsst::Decoder<Impl>& fsst, const char* in,
                               PackedReader& lengths, PackedReader& lcps,
                               uint32_t restart, uint32_t entry,
                               uint64_t enc_off, uint64_t enc_end,
                               std::string& buf, uint64_t& walked) {
  const uint32_t n = entry - restart + 1;
  SDB_ASSERT(n <= kChainRestart);
  std::array<uint64_t, kChainRestart + 1> off;
  std::array<uint32_t, kChainRestart> lcp;
  std::array<uint32_t, kChainRestart> enc;
  lcps.Read(restart, n, lcp.data());
  lengths.Read(restart, n, enc.data());
  lcp[0] = 0;
  off[0] = enc_off;
  for (uint32_t i = 0; i < n; ++i) {
    off[i + 1] = off[i] + enc[i];
  }
  SDB_ENSURE(off[n] <= enc_end, "col codec: corrupted front coding");
  std::array<uint32_t, kChainRestart> used;
  uint32_t count = 0;
  uint64_t need = std::numeric_limits<uint64_t>::max();
  for (uint32_t i = n; need != 0 && i-- > 0;) {
    if (lcp[i] < need) {
      used[count++] = i;
      need = lcp[i];
    }
  }
  uint64_t len = 0;
  while (count != 0) {
    const auto i = used[--count];
    SDB_ENSURE(lcp[i] <= len, "col codec: corrupted front coding");
    len = DecodeSuffix(fsst, in + off[i], off[i + 1] - off[i], lcp[i], buf);
    ++walked;
  }
  return {buf.data(), len};
}

template<typename Impl>
struct Fetch;

template<typename Impl>
struct Scan {
  static constexpr ByteCodec kCodec = Impl::kCodec;
  using Fetch = fsst_front::Fetch<Impl>;

  template<typename Frames>
  Scan(const ColumnSegment& segment, const Frames& s)
    : lcps{s.base + s.header.off_lcps, s.header.lcp_width} {
    SDB_ENSURE(
      s.header.symtab_size == 0 ||
        decoder.Import(segment.cache_slot.cache,
                       std::string_view{reinterpret_cast<const char*>(
                                          s.base + s.header.off_symtab),
                                        s.header.symtab_size}),
      "col codec: corrupted symbol table");
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    group_decoded.assign(groups.Count(), 0);
  }

  template<typename Frames, typename Visit>
  void WalkFrame(Frames& s, uint32_t f, char* out, Visit&& visit) {
    const auto& m = s.frames[f];
    const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    const auto chain =
      WalkChain(decoder, s.Data(), s.length_reader, lcps, m.first_entry,
                s.FrameEnd(f), m.comp_off, enc_end, out, m.raw_len, visit);
    SDB_ENSURE(chain.written == m.raw_len && chain.enc_off == enc_end,
               "col codec: corrupted frame");
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto first = s.frames[f].first_entry;
    WalkFrame(s, f, out, [&](uint32_t e, std::string_view v) {
      SDB_ASSERT(!Frames::kDedup || e == first || View(s.values[e - 1]) < v);
      s.Keep(e, v);
    });
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    const auto g = (e - s.frames[f].first_entry) / kChainRestart;
    if (!group_decoded[groups.Base(f) + g]) {
      DecodeGroup(s, f, g);
    }
  }

  template<typename Frames, typename Entry>
  void Ensure(Frames& s, duckdb::idx_t count, Entry entry_at,
              uint32_t window_first, uint32_t window_end) {
    if (count <= kSparseEntries) {
      EnsureGroups(s, count, entry_at, window_first, window_end);
    } else {
      s.EnsureEach(count, entry_at);
    }
  }

  template<typename Frames, typename Entry>
  void EnsureGroups(Frames& s, duckdb::idx_t count, Entry entry_at,
                    uint32_t window_first, uint32_t window_end) {
    if (s.Complete()) {
      return;
    }
    auto f = group_frame;
    for (duckdb::idx_t i = 0; i < count;) {
      const auto e = static_cast<uint32_t>(entry_at(i));
      if (f == kNoFrame || e < s.frames[f].first_entry || e >= s.FrameEnd(f)) {
        f = s.FrameOf(e);
      }
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        if (next < first || next >= end) {
          break;
        }
      }
      if (!s.decoded[f]) {
        const auto lo = std::max(window_first, first);
        const auto hi = std::min(window_end, end);
        SDB_ASSERT(lo < hi);
        const uint64_t range =
          (hi - 1 - first) / kChainRestart - (lo - first) / kChainRestart + 1;
        const uint64_t total =
          (end - first + kChainRestart - 1) / kChainRestart;
        if (range < total) {
          const auto group_of = [&](duckdb::idx_t k) {
            return (static_cast<uint32_t>(entry_at(k)) - first) / kChainRestart;
          };
          for (auto k = i; k < j;) {
            const auto group = group_of(k);
            uint32_t rows = 0;
            for (; k < j && group_of(k) == group; ++k) {
              rows |=
                uint32_t{1}
                << (static_cast<uint32_t>(entry_at(k)) - first) % kChainRestart;
            }
            EnsureRows(s, f, group, rows);
          }
        } else {
          s.DecodeFrame(f);
        }
      }
      i = j;
    }
    group_frame = f;
  }

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count,
                                                               result_offset);
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    bool referenced = false;
    auto f = group_frame;
    for (duckdb::idx_t i = 0; i < count;) {
      const auto e = static_cast<uint32_t>(entry_at(i));
      if (f == kNoFrame || e < s.frames[f].first_entry || e >= s.FrameEnd(f)) {
        f = s.FrameOf(e);
      }
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto last = (e - first) / kChainRestart;
      uint64_t touched = 1;
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        if (next < first || next >= end) {
          break;
        }
        const auto group = (next - first) / kChainRestart;
        touched += group != last;
        last = group;
      }
      if (!s.decoded[f]) {
        const auto lo = std::max(window_first, first);
        const auto hi = std::min(window_end, end);
        SDB_ASSERT(lo < hi);
        const uint64_t range =
          (hi - 1 - first) / kChainRestart - (lo - first) / kChainRestart + 1;
        const uint64_t entry_bytes = std::max<uint64_t>(
          s.frames[f].raw_len / (end - first), kChainMinRowBytes);
        const uint64_t total =
          (end - first + kChainRestart - 1) / kChainRestart;
        if ((touched * kChainRestart <= kSparseEntries && range < total) ||
            (j - i) * entry_bytes + touched * kChainGroupCost <=
              range * kChainRestart * entry_bytes) {
          while (i < j) {
            const auto group =
              (static_cast<uint32_t>(entry_at(i)) - first) / kChainRestart;
            uint32_t rows = 0;
            for (; i < j; ++i) {
              const auto at = static_cast<uint32_t>(entry_at(i)) - first;
              if (at / kChainRestart != group) {
                break;
              }
              rows |= uint32_t{1} << at % kChainRestart;
            }
            DecodeRows(s, f, group, rows, 0, arena,
                       [&](uint32_t row, std::string_view v) {
                         if ((rows >> row & 1) != 0) {
                           writer.WriteStringRef(duckdb::string_t{
                             v.data(), static_cast<uint32_t>(v.size())});
                         }
                       });
          }
          continue;
        }
        s.DecodeFrame(f);
      }
      referenced = true;
      for (; i < j; ++i) {
        writer.WriteStringRef(s.values[entry_at(i)]);
      }
    }
    group_frame = f;
    if (referenced) {
      s.ReferenceHeap(result);
    }
  }

  template<typename Frames>
  void EnsureRows(Frames& s, uint32_t f, uint32_t g, uint32_t rows) {
    const auto index = groups.Base(f) + g;
    if (group_decoded[index]) {
      return;
    }
    if (group_rows.empty()) {
      group_rows.assign(groups.Count(), 0);
    }
    const uint32_t kept = group_rows[index];
    rows &= ~kept;
    if (rows == 0) {
      return;
    }
    const auto restart = s.frames[f].first_entry + g * kChainRestart;
    const auto used =
      DecodeRows(s, f, g, rows, kept,
                 duckdb::StringVector::GetStringAllocator(s.dictionary->data),
                 [&](uint32_t row, std::string_view v) {
                   s.Keep(restart + row, v);
                   group_heap_bytes += v.size();
                 });
    group_rows[index] = static_cast<uint16_t>(kept | used);
  }

  template<typename Frames, typename Visit>
  IRS_FORCE_INLINE uint32_t DecodeRows(Frames& s, uint32_t f, uint32_t g,
                                       uint32_t rows, uint32_t kept,
                                       duckdb::ArenaAllocator& arena,
                                       Visit&& visit) {
    const auto restart = s.frames[f].first_entry + g * kChainRestart;
    const uint32_t n = std::bit_width(rows);
    SDB_ASSERT(n != 0 && n <= kChainRestart && restart + n <= s.FrameEnd(f));
    std::array<uint64_t, kChainRestart + 1> off;
    std::array<uint32_t, kChainRestart> lcp;
    std::array<uint32_t, kChainRestart> enc;
    lcps.Read(restart, n, lcp.data());
    s.length_reader.Read(restart, n, enc.data());
    lcp[0] = 0;
    off[0] = groups.Offset(s.length_reader, s.frames[f], f, g);
    for (uint32_t i = 0; i < n; ++i) {
      off[i + 1] = off[i] + enc[i];
    }
    SDB_ENSURE(off[n] <= s.header.data_size,
               "col codec: corrupted front coding");
    uint32_t used = 0;
    uint32_t sources = 0;
    uint64_t need = 0;
    uint64_t capacity = 0;
    for (uint32_t i = n; i-- > 0;) {
      const uint32_t wanted = (rows >> i & 1) | uint32_t{lcp[i] < need};
      const uint32_t source = wanted & (kept >> i);
      const uint32_t decode = wanted ^ source;
      sources |= source << i;
      used |= decode << i;
      need = wanted != 0 ? (source != 0 ? 0 : lcp[i]) : need;
      capacity += decode * (lcp[i] + uint64_t{enc[i]} * fsst::kMaxExpansion);
    }
    auto* out = duckdb::char_ptr_cast(arena.Allocate(capacity));
    uint64_t written = 0;
    const char* prev = nullptr;
    uint64_t prev_len = 0;
    for (auto left = used | sources; left != 0; left &= left - 1) {
      const auto i = static_cast<uint32_t>(std::countr_zero(left));
      if ((sources >> i & 1) != 0) {
        const auto value = View(s.values[restart + i]);
        prev = value.data();
        prev_len = value.size();
        continue;
      }
      SDB_ENSURE(lcp[i] <= prev_len, "col codec: corrupted front coding");
      auto* at = out + written;
      if (lcp[i] != 0) {
        std::memcpy(at, prev, lcp[i]);
      }
      const auto room = capacity - (written + lcp[i]);
      const auto dec = enc[i] == 0 ? 0
                                   : decoder.Decode(s.Data() + off[i], enc[i],
                                                    at + lcp[i], room);
      SDB_ENSURE(dec <= room, "col codec: corrupted entry");
      prev = at;
      prev_len = lcp[i] + dec;
      written += prev_len;
      visit(i, std::string_view{at, prev_len});
    }
    arena.ShrinkHead(capacity - written);
    return used;
  }

  template<typename Frames>
  void DecodeGroup(Frames& s, uint32_t f, uint32_t g) {
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    const auto capacity = ChainCapacity(s.length_reader, lcps, first, end);
    auto& arena = duckdb::StringVector::GetStringAllocator(s.dictionary->data);
    auto* out = duckdb::char_ptr_cast(arena.Allocate(capacity));
    const auto chain = WalkChain(
      decoder, s.Data(), s.length_reader, lcps, first, end,
      groups.Offset(s.length_reader, s.frames[f], f, g), s.header.data_size,
      out, capacity, [&](uint32_t e, std::string_view v) { s.Keep(e, v); });
    arena.ShrinkHead(capacity - chain.written);
    group_heap_bytes += chain.written;
    group_decoded[groups.Base(f) + g] = 1;
  }

  template<typename Frames>
  std::string_view DecodeHead(Frames& s, uint32_t f, uint32_t g) {
    const auto e = s.frames[f].first_entry + g * kChainRestart;
    if (s.decoded[f] || group_decoded[groups.Base(f) + g]) {
      return View(s.values[e]);
    }
    std::string_view head;
    WalkInPlace(decoder, s.Data(), s.length_reader, lcps, e, e + 1,
                groups.Offset(s.length_reader, s.frames[f], f, g),
                s.header.data_size, probe, [&](uint32_t, std::string_view v) {
                  head = v;
                  return true;
                });
    return head;
  }

  template<typename Frames, typename Before>
  uint32_t Bound(Frames& s, Before before) {
    if (s.frames.empty()) {
      return s.header.entry_count;
    }
    const auto lo = PartitionIndex(0, s.frames.size(), [&](size_t f) {
      return before(DecodeHead(s, static_cast<uint32_t>(f), 0));
    });
    if (lo == 0) {
      return s.frames[0].first_entry;
    }
    const auto f = static_cast<uint32_t>(lo - 1);
    const auto glo = PartitionIndex(1, groups.Groups(f), [&](size_t i) {
      return before(DecodeHead(s, f, static_cast<uint32_t>(i)));
    });
    const auto g = static_cast<uint32_t>(glo - 1);
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    if (s.decoded[f] || group_decoded[groups.Base(f) + g]) {
      for (auto e = first; e < end; ++e) {
        if (!before(View(s.values[e]))) {
          return e;
        }
      }
      return end;
    }
    return WalkInPlace(decoder, s.Data(), s.length_reader, lcps, first, end,
                       groups.Offset(s.length_reader, s.frames[f], f, g),
                       s.header.data_size, probe,
                       [&](uint32_t, std::string_view v) { return before(v); });
  }

  uint64_t HeapBytes() const noexcept { return group_heap_bytes; }

  fsst::Decoder<Impl> decoder;
  PackedReader lcps{nullptr, 0};
  ChainGroups groups;
  std::vector<uint8_t> group_decoded;
  std::vector<uint16_t> group_rows;
  static_assert(kChainRestart <= 8 * sizeof(uint16_t));
  uint64_t group_heap_bytes = 0;
  uint32_t group_frame = kNoFrame;
  std::string probe;
};

template<typename Impl>
struct Fetch {
  void Reset(const Header& h, duckdb::data_ptr_t base) {
    decoder.reset();
    groups.Clear();
    lcps = PackedReader{base + h.off_lcps, h.lcp_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment& segment, const Header& h,
            duckdb::data_ptr_t base, PackedReader& lengths, uint32_t lo,
            const FrameMeta& f, uint32_t entry, duckdb::Vector& result,
            duckdb::idx_t result_idx, bool) {
    if (groups.Empty()) {
      Open(segment.cache_slot.cache, h, base);
    }
    const auto* in = reinterpret_cast<const char*>(base + h.off_data);
    const uint32_t group = (entry - f.first_entry) / kChainRestart;
    const uint32_t restart = f.first_entry + group * kChainRestart;
    const auto value = DecodeChained(
      *decoder, in, lengths, lcps, restart, entry,
      groups.Offset(lengths, f, lo, group), h.data_size, chain, walked);
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, value.data(), value.size());
  }

  void Open(duckdb::ObjectCache* cache, const Header& h,
            duckdb::const_data_ptr_t base) {
    decoder.emplace();
    SDB_ENSURE(
      h.symtab_size == 0 ||
        decoder->Import(cache, std::string_view{reinterpret_cast<const char*>(
                                                  base + h.off_symtab),
                                                h.symtab_size}),
      "col codec: corrupted symbol table");
    const auto* table = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(table, i).first_entry,
                       i + 1 < h.frame_count ? FrameAt(table, i + 1).first_entry
                                             : h.entry_count};
    });
  }

  std::optional<fsst::Decoder<Impl>> decoder;
  PackedReader lcps{nullptr, 0};
  ChainGroups groups;
  std::string chain;
  uint64_t walked = 0;
};

}  // namespace irs::codecs::text::fsst_front
namespace irs::codecs {

/*
Data layout per PlainScan segment, one block of one column; a text column's
row group is cut into segments of about kDefaultColSegmentTarget encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags                             |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each row's FSST-encoded suffix,          |
|   0 for a NULL row                                   |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table: at most 2066 bytes      |   |
|   |   with 8-bit codes, 36872 with 12-bit ones   |   |
|   +----------------------------------------------+   |
|   trained once per row group; each FSST segment      |
|   stores a copy                                      |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries          |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

namespace {

template<typename Impl>
using PlainFsst = text::fsst_front::CompressionState<Impl, text::PlainRows>;

template<typename Impl>
using DictFsst = text::fsst_front::CompressionState<Impl, text::DictionaryRows>;

double AnalyzeFsst(ColumnDataCheckpointData& data, double bound) {
  return text::AnalyzeTrial<PlainFsst<text::fsst::Fsst8>>(data, bound);
}

bool CompressFsst(ColumnDataCheckpointData& data) {
  return text::CompressContested<PlainFsst<text::fsst::Fsst8>>(data);
}

double AnalyzeDictFsst(ColumnDataCheckpointData& data, double bound) {
  return text::AnalyzeTrial<DictFsst<text::fsst::Fsst8>>(data, bound);
}

bool CompressDictFsst(ColumnDataCheckpointData& data) {
  return text::CompressContested<DictFsst<text::fsst::Fsst8>>(data);
}

constexpr CompressionFunction kFsst{
  .analyze = AnalyzeFsst,
  .compress = CompressFsst,
  .codec = CodecType::Fsst,
};

constexpr CompressionFunction kDictFsst{
  .analyze = AnalyzeDictFsst,
  .compress = CompressDictFsst,
  .codec = CodecType::DictFsst,
};

constexpr const CompressionFunction* kFsstContenders[] = {&kFsst};
constexpr const CompressionFunction* kDictFsstContenders[] = {&kDictFsst};

}  // namespace

CompressionFunctions FsstTextContenders(Shape shape) {
  if (shape == Shape::Plain) {
    return kFsstContenders;
  }
  return kDictFsstContenders;
}

constinit const Codec kFsstFrontCodec =
  text::StringCodec<text::PlainScan<text::fsst_front::Scan<text::fsst::Fsst8>>,
                    PlainFsst<text::fsst::Fsst8>>();

constinit const Codec kFsst12FrontCodec =
  text::StringCodec<text::PlainScan<text::fsst_front::Scan<text::fsst::Fsst12>>,
                    PlainFsst<text::fsst::Fsst12>>();

/*
Data layout per DictionaryScan segment, one block of one column; a text
column's row group is cut into segments of about kDefaultColSegmentTarget
encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags                             |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each entry's FSST-encoded suffix;        |
|   entry 0 is NULL, then the distinct values in       |
|   sorted order                                       |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                      Row codes                       |
|   +----------------------------------------------+   |
|   |   numeric codec segment of code[row_count]   |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table: at most 2066 bytes      |   |
|   |   with 8-bit codes, 36872 with 12-bit ones   |   |
|   +----------------------------------------------+   |
|   trained once per row group; each FSST segment      |
|   stores a copy                                      |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries          |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

constinit const Codec kDictFsstFrontCodec = text::StringCodec<
  text::DictionaryScan<text::fsst_front::Scan<text::fsst::Fsst8>>,
  DictFsst<text::fsst::Fsst8>>();

constinit const Codec kDictFsst12FrontCodec = text::StringCodec<
  text::DictionaryScan<text::fsst_front::Scan<text::fsst::Fsst12>>,
  DictFsst<text::fsst::Fsst12>>();

}  // namespace irs::codecs
