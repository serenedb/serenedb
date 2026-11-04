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

#include <algorithm>
#include <array>
#include <cstring>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/fsst.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr size_t kFsstMaxExpansion = 8;

struct ChainEnd {
  uint64_t written;
  uint64_t enc_off;
};

inline uint64_t ChainCapacity(PackedReader& lengths, PackedReader& lcps,
                              uint32_t first, uint32_t end) {
  uint64_t capacity = 0;
  for (auto e = first; e < end; ++e) {
    capacity += (e == first ? 0 : lcps.At(e)) +
                uint64_t{lengths.At(e)} * kFsstMaxExpansion;
  }
  return capacity;
}

inline uint64_t DecodeSuffix(const FsstDecoder& fsst, const char* in,
                             uint64_t enc, uint64_t lcp, std::string& buf) {
  if (const auto need = lcp + enc * kFsstMaxExpansion; buf.size() < need) {
    buf.resize(std::max<uint64_t>(need, buf.size() * 2));
  }
  const auto room = buf.size() - lcp;
  const auto dec = enc == 0 ? 0 : fsst.Decode(in, enc, buf.data() + lcp, room);
  SDB_ENSURE(dec <= room, "col codec: corrupted entry");
  return lcp + dec;
}

template<typename Visit>
ChainEnd WalkChain(const FsstDecoder& fsst, const char* in,
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
    std::memcpy(out + off, out + prev_off, lcp);
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

template<typename Visit>
uint32_t WalkInPlace(const FsstDecoder& fsst, const char* in,
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

class ChainGroups {
 public:
  template<typename Entries>
  void Reset(uint32_t frame_count, Entries entries) {
    _base.resize(frame_count + 1);
    uint32_t groups = 0;
    for (uint32_t f = 0; f < frame_count; ++f) {
      const auto [first, end] = entries(f);
      SDB_ENSURE(end > first, "col codec: corrupted frame table");
      _base[f] = groups;
      groups += (end - first + kChainRestart - 1) / kChainRestart;
    }
    _base[frame_count] = groups;
    _offsets.resize(groups);
    _known.assign(frame_count, 0);
  }

  void Clear() noexcept { _base.clear(); }
  bool Empty() const noexcept { return _base.empty(); }
  uint32_t Count() const noexcept { return _base.back(); }
  uint32_t Base(uint32_t f) const noexcept { return _base[f]; }
  uint32_t Groups(uint32_t f) const noexcept { return _base[f + 1] - _base[f]; }

  uint64_t Offset(PackedReader& lengths, const FrameMeta& m, uint32_t f,
                  uint32_t g) {
    auto* offsets = _offsets.data() + _base[f];
    auto& known = _known[f];
    if (known == 0) {
      offsets[0] = m.comp_off;
      known = 1;
    }
    const uint64_t end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    for (; known <= g; ++known) {
      const auto from = m.first_entry + (known - 1) * kChainRestart;
      uint64_t offset = offsets[known - 1];
      for (uint32_t e = from; e < from + kChainRestart; ++e) {
        offset += lengths.At(e);
      }
      SDB_ENSURE(offset <= end, "col codec: corrupted front coding");
      offsets[known] = offset;
    }
    return offsets[g];
  }

 private:
  std::vector<uint32_t> _base;
  std::vector<uint32_t> _known;
  std::vector<uint64_t> _offsets;
};

struct FsstScan {
  template<typename Frames>
  FsstScan(duckdb::ColumnSegment&, const Frames& s)
    : lcps{s.base + s.header.off_lcps, s.header.lcp_width} {
    SDB_ENSURE(s.header.symtab_size == 0 ||
                 decoder.Import(std::string_view{
                   reinterpret_cast<const char*>(s.base + s.header.off_symtab),
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
      SDB_ASSERT(!Frames::kDedup || e == first || View(s.values[e - 1]) < v,
                 "col codec: dictionary entries out of order");
      s.Keep(e, v);
    });
  }

  template<typename Frames>
  void Stream(Frames& s, uint32_t f, char* out, std::string_view,
              uint32_t* ends) {
    WalkFrame(s, f, out, [&](uint32_t, std::string_view v) {
      if (ends) {
        *ends++ = static_cast<uint32_t>(v.data() + v.size() - out);
      }
    });
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    const auto g = (e - s.frames[f].first_entry) / kChainRestart;
    if (!group_decoded[groups.Base(f) + g]) {
      DecodeGroup(s, f, g);
    }
  }

  template<typename Frames>
  void DecodeGroup(Frames& s, uint32_t f, uint32_t g) {
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    const auto capacity = ChainCapacity(s.length_reader, lcps, first, end);
    group_heap_bytes += capacity;
    char* out = duckdb::char_ptr_cast(
      duckdb::StringVector::GetStringAllocator(s.dictionary->data)
        .Allocate(capacity));
    WalkChain(decoder, s.Data(), s.length_reader, lcps, first, end,
              groups.Offset(s.length_reader, s.frames[f], f, g),
              s.header.data_size, out, capacity,
              [&](uint32_t e, std::string_view v) { s.Keep(e, v); });
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

  FsstDecoder decoder;
  PackedReader lcps{nullptr, 0};
  ChainGroups groups;
  std::vector<uint8_t> group_decoded;
  uint64_t group_heap_bytes = 0;
  std::string probe;
};

struct FsstFetch {
  void Reset(const Header& h, duckdb::data_ptr_t base) {
    decoder.reset();
    groups.Clear();
    lcps = PackedReader{base + h.off_lcps, h.lcp_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(duckdb::ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& lengths, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (groups.Empty()) {
      Open(h, base);
    }
    const auto* in = reinterpret_cast<const char*>(base + h.off_data);
    const uint32_t group = (entry - f.first_entry) / kChainRestart;
    const uint32_t restart = f.first_entry + group * kChainRestart;
    const auto value =
      DecodeEntry(in, lengths, restart, entry,
                  groups.Offset(lengths, f, lo, group), h.data_size);
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, value.data(), value.size());
  }

  std::string_view DecodeEntry(const char* in, PackedReader& lengths,
                               uint32_t restart, uint32_t entry,
                               uint64_t enc_off, uint64_t enc_end) {
    const uint32_t n = entry - restart + 1;
    SDB_ASSERT(n <= kChainRestart);
    std::array<uint64_t, kChainRestart + 1> off;
    std::array<uint32_t, kChainRestart> lcp;
    off[0] = enc_off;
    for (uint32_t i = 0; i < n; ++i) {
      lcp[i] = i == 0 ? 0 : lcps.At(restart + i);
      off[i + 1] = off[i] + lengths.At(restart + i);
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
      len =
        DecodeSuffix(*decoder, in + off[i], off[i + 1] - off[i], lcp[i], chain);
      ++walked;
    }
    return {chain.data(), len};
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    decoder.emplace();
    SDB_ENSURE(
      h.symtab_size == 0 ||
        decoder->Import(std::string_view{
          reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size}),
      "col codec: corrupted symbol table");
    const auto* table = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(table, i).first_entry,
                       i + 1 < h.frame_count ? FrameAt(table, i + 1).first_entry
                                             : h.entry_count};
    });
  }

  std::optional<FsstDecoder> decoder;
  PackedReader lcps{nullptr, 0};
  ChainGroups groups;
  std::string chain;
  uint64_t walked = 0;
};

struct FsstLeaf {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;
  using Scan = FsstScan;
  using Fetch = FsstFetch;
};

}  // namespace irs::codecs::text
