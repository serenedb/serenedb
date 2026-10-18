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
#include <cstring>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
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
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

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
    if (!visit(e, value)) {
      return {e, off, enc_off};
    }
    prev_off = off;
    prev_len = value.size();
    off += value.size();
    enc_off += enc;
  }
  return {end, off, enc_off};
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
    if (const auto need = lcp + enc * kFsstMaxExpansion; buf.size() < need) {
      buf.resize(std::max<uint64_t>(need, buf.size() * 2));
    }
    const auto room = buf.size() - lcp;
    const auto dec =
      enc == 0 ? 0 : fsst.Decode(in + enc_off, enc, buf.data() + lcp, room);
    SDB_ENSURE(dec <= room, "col codec: corrupted entry");
    len = lcp + dec;
    if (!visit(e, std::string_view{buf.data(), len})) {
      return e;
    }
    enc_off += enc;
  }
  return end;
}

struct FsstScan {
  template<typename Frames>
  FsstScan(duckdb::ColumnSegment&, const Frames& s)
    : lcps{s.base + s.header.off_lcps, s.header.lcp_width} {
    SDB_ENSURE(s.header.symtab_size == 0 ||
                 decoder.Import(std::string_view{
                   reinterpret_cast<const char*>(s.base + s.header.off_symtab),
                   s.header.symtab_size}),
               "col codec: corrupted symbol table");
    group_base.resize(s.frames.size() + 1);
    uint32_t groups = 0;
    for (uint32_t f = 0; f < s.frames.size(); ++f) {
      group_base[f] = groups;
      groups += (s.FrameEnd(f) - s.frames[f].first_entry + kChainRestart - 1) /
                kChainRestart;
    }
    group_base[s.frames.size()] = groups;
    group_decoded.assign(groups, 0);
    group_enc_off.resize(groups);
    group_offsets_ready.assign(s.frames.size(), 0);
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto end = s.FrameEnd(f);
    const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    const auto chain = WalkChain(
      decoder, s.Data(), s.length_reader, lcps, m.first_entry, end, m.comp_off,
      enc_end, out, m.raw_len, [&](uint32_t e, std::string_view v) {
        SDB_ASSERT(
          !Frames::kDedup || e == m.first_entry || View(s.values[e - 1]) < v,
          "col codec: dictionary entries out of order");
        return s.Keep(e, v);
      });
    SDB_ENSURE(chain.written == m.raw_len && chain.enc_off == enc_end,
               "col codec: corrupted frame");
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    const auto g = (e - s.frames[f].first_entry) / kChainRestart;
    if (!group_decoded[group_base[f] + g]) {
      DecodeGroup(s, f, g);
    }
  }

  template<typename Frames>
  void EnsureGroupOffsets(Frames& s, uint32_t f) {
    if (group_offsets_ready[f]) {
      return;
    }
    const auto first = s.frames[f].first_entry;
    const auto end = s.FrameEnd(f);
    uint64_t off = s.frames[f].comp_off;
    auto* out = group_enc_off.data() + group_base[f];
    for (auto e = first; e < end; ++e) {
      if ((e - first) % kChainRestart == 0) {
        *out++ = static_cast<uint32_t>(off);
      }
      off += s.length_reader.At(e);
    }
    SDB_ENSURE(
      off == static_cast<uint64_t>(s.frames[f].comp_off) + s.frames[f].comp_len,
      "col codec: corrupted front coding");
    group_offsets_ready[f] = 1;
  }

  template<typename Frames>
  void DecodeGroup(Frames& s, uint32_t f, uint32_t g) {
    EnsureGroupOffsets(s, f);
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    const auto capacity = ChainCapacity(s.length_reader, lcps, first, end);
    group_heap_bytes += capacity;
    char* out = duckdb::StringVector::EmptyString(s.dictionary->data, capacity)
                  .GetDataWriteable();
    WalkChain(decoder, s.Data(), s.length_reader, lcps, first, end,
              group_enc_off[group_base[f] + g], s.header.data_size, out,
              capacity,
              [&](uint32_t e, std::string_view v) { return s.Keep(e, v); });
    group_decoded[group_base[f] + g] = 1;
  }

  template<typename Frames>
  std::string_view DecodeHead(Frames& s, uint32_t f, uint32_t g) {
    const auto e = s.frames[f].first_entry + g * kChainRestart;
    if (s.decoded[f] || group_decoded[group_base[f] + g]) {
      return View(s.values[e]);
    }
    uint64_t enc_off = s.frames[f].comp_off;
    if (g != 0) {
      EnsureGroupOffsets(s, f);
      enc_off = group_enc_off[group_base[f] + g];
    }
    std::string_view head;
    WalkInPlace(decoder, s.Data(), s.length_reader, lcps, e, e + 1, enc_off,
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
    uint32_t lo = 0;
    uint32_t hi = static_cast<uint32_t>(s.frames.size());
    while (lo < hi) {
      const auto mid = lo + (hi - lo) / 2;
      if (before(DecodeHead(s, mid, 0))) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo == 0) {
      return s.frames[0].first_entry;
    }
    const uint32_t f = lo - 1;
    uint32_t glo = 1;
    uint32_t ghi = group_base[f + 1] - group_base[f];
    while (glo < ghi) {
      const auto mid = glo + (ghi - glo) / 2;
      if (before(DecodeHead(s, f, mid))) {
        glo = mid + 1;
      } else {
        ghi = mid;
      }
    }
    const uint32_t g = glo - 1;
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    if (s.decoded[f] || group_decoded[group_base[f] + g]) {
      for (auto e = first; e < end; ++e) {
        if (!before(View(s.values[e]))) {
          return e;
        }
      }
      return end;
    }
    EnsureGroupOffsets(s, f);
    return WalkInPlace(decoder, s.Data(), s.length_reader, lcps, first, end,
                       group_enc_off[group_base[f] + g], s.header.data_size,
                       probe,
                       [&](uint32_t, std::string_view v) { return before(v); });
  }

  uint64_t HeapBytes() const noexcept { return group_heap_bytes; }

  FsstDecoder decoder;
  PackedReader lcps{nullptr, 0};
  std::vector<uint32_t> group_base;
  std::vector<uint8_t> group_decoded;
  std::vector<uint32_t> group_enc_off;
  std::vector<uint8_t> group_offsets_ready;
  uint64_t group_heap_bytes = 0;
  std::string probe;
};

struct FsstFetch {
  void Reset(const Header& h, duckdb::const_data_ptr_t base) {
    decoder.reset();
    group_base.clear();
    lcps = PackedReader{const_cast<duckdb::data_ptr_t>(base) + h.off_lcps,
                        h.lcp_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(duckdb::ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& lengths, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (group_base.empty()) {
      Open(h, base);
    }
    const auto* in = reinterpret_cast<const char*>(base + h.off_data);
    const uint32_t group = (entry - f.first_entry) / kChainRestart;
    const uint32_t restart = f.first_entry + group * kChainRestart;
    auto* frame_offsets = offsets.data() + group_base[lo];
    auto& frame_known = known[lo];
    if (frame_known == 0) {
      frame_offsets[0] = f.comp_off;
      frame_known = 1;
    }
    for (; frame_known <= group; ++frame_known) {
      const auto from = f.first_entry + (frame_known - 1) * kChainRestart;
      uint64_t off = frame_offsets[frame_known - 1];
      for (uint32_t e = from; e < from + kChainRestart; ++e) {
        off += lengths.At(e);
      }
      frame_offsets[frame_known] = off;
    }
    std::string_view value;
    WalkInPlace(*decoder, in, lengths, lcps, restart, entry + 1,
                frame_offsets[group], h.data_size, chain,
                [&](uint32_t, std::string_view v) {
                  value = v;
                  return true;
                });
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, value.data(), value.size());
    walked += entry + 1 - restart;
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    decoder.emplace();
    SDB_ENSURE(
      h.symtab_size == 0 ||
        decoder->Import(std::string_view{
          reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size}),
      "col codec: corrupted symbol table");
    const auto* frames = base + h.off_frames;
    group_base.resize(h.frame_count + 1);
    uint32_t groups = 0;
    for (uint32_t i = 0; i < h.frame_count; ++i) {
      const auto first =
        FrameMeta::Load(frames + i * kFrameMetaSize).first_entry;
      const auto end =
        i + 1 < h.frame_count
          ? FrameMeta::Load(frames + (i + 1) * kFrameMetaSize).first_entry
          : h.entry_count;
      SDB_ENSURE(end > first, "col codec: corrupted frame table");
      group_base[i] = groups;
      groups += (end - first + kChainRestart - 1) / kChainRestart;
    }
    group_base[h.frame_count] = groups;
    offsets.resize(groups);
    known.assign(h.frame_count, 0);
  }

  std::optional<FsstDecoder> decoder;
  PackedReader lcps{nullptr, 0};
  std::vector<uint32_t> group_base;
  std::vector<uint32_t> known;
  std::vector<uint64_t> offsets;
  std::string chain;
  uint64_t walked = 0;
};

struct FsstLeaf {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;
  static constexpr bool kSorted = true;
  static constexpr bool kSparseGroups = true;
  using Scan = FsstScan;
  using Fetch = FsstFetch;
};

}  // namespace irs::codecs::text
