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
#include <cstdint>
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
#include "iresearch/formats/column/codecs/onpair.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {

inline std::string_view OnPairSide(const Header& h,
                                   duckdb::const_data_ptr_t base) noexcept {
  return {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size};
}

inline FrameMeta TokenFrame(const FrameMeta& m, uint8_t bits) noexcept {
  const auto stream =
    m.comp_len < kOnPairPadding ? 0 : m.comp_len - kOnPairPadding;
  return {m.first_entry, m.raw_len, 0,
          static_cast<uint32_t>(uint64_t{stream} * 8 / bits)};
}

inline uint64_t FirstToken(ChainGroups& groups, PackedReader& counts,
                           const FrameMeta& tokens, uint32_t f, uint32_t e) {
  const auto g = (e - tokens.first_entry) / kChainRestart;
  uint64_t first = groups.Offset(counts, tokens, f, g);
  for (auto k = tokens.first_entry + g * kChainRestart; k < e; ++k) {
    first += counts.At(k);
  }
  return first;
}

struct OnPairScan {
  static constexpr uint32_t kEntryGroup = 1;

  template<typename Frames>
  OnPairScan(const ColumnSegment&, const Frames& s) {
    SDB_ENSURE(table.Open(OnPairSide(s.header, s.base)),
               "col codec: corrupted symbol table");
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    entry_decoded.assign(s.header.entry_count, 0);
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto tokens = TokenFrame(m, table.Bits());
    const auto* stream =
      reinterpret_cast<const uint8_t*>(s.Data()) + m.comp_off;
    const auto end = s.FrameEnd(f);
    uint64_t token = 0;
    uint64_t off = 0;
    for (auto e = m.first_entry; e < end; ++e) {
      const uint64_t count = s.length_reader.At(e);
      SDB_ENSURE(token + count <= tokens.comp_len,
                 "col codec: corrupted entry");
      const auto n =
        table.Decode(stream, token, count, out + off, m.raw_len - off);
      const std::string_view v{out + off, n};
      SDB_ASSERT(!Frames::kDedup || e == m.first_entry ||
                 View(s.values[e - 1]) < v);
      s.Keep(e, v);
      off += n;
      token += count;
    }
    SDB_ENSURE(off == m.raw_len, "col codec: corrupted frame");
  }

  template<typename Frames>
  std::string_view Decode(Frames& s, uint32_t e, uint32_t f, std::string& buf) {
    const auto& m = s.frames[f];
    const auto tokens = TokenFrame(m, table.Bits());
    const auto first = FirstToken(groups, s.length_reader, tokens, f, e);
    const uint64_t count = s.length_reader.At(e);
    SDB_ENSURE(first + count <= tokens.comp_len, "col codec: corrupted entry");
    if (const auto need = table.Capacity(count); buf.size() < need) {
      buf.resize(std::max<uint64_t>(need, buf.size() * 2));
    }
    const auto n =
      table.Decode(reinterpret_cast<const uint8_t*>(s.Data()) + m.comp_off,
                   first, count, buf.data(), buf.size());
    return {buf.data(), n};
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    if (entry_decoded[e]) {
      return;
    }
    const auto v = Decode(s, e, f, probe);
    char* out = duckdb::char_ptr_cast(
      duckdb::StringVector::GetStringAllocator(s.dictionary->data)
        .Allocate(v.size()));
    if (!v.empty()) {
      std::memcpy(out, v.data(), v.size());
    }
    s.Keep(e, std::string_view{out, v.size()});
    entry_heap_bytes += v.size();
    entry_decoded[e] = 1;
  }

  template<typename Frames, typename Before>
  uint32_t Bound(Frames& s, Before before) {
    if (s.frames.empty()) {
      return s.header.entry_count;
    }
    return static_cast<uint32_t>(PartitionIndex(
      s.frames[0].first_entry, s.header.entry_count, [&](size_t i) {
        const auto e = static_cast<uint32_t>(i);
        const auto f = s.FrameOf(e);
        if (s.decoded[f] || entry_decoded[e]) {
          return before(View(s.values[e]));
        }
        return before(Decode(s, e, f, probe));
      }));
  }

  uint64_t HeapBytes() const noexcept {
    return entry_heap_bytes + table.Bytes();
  }

  OnPairTable table;
  ChainGroups groups;
  std::vector<uint8_t> entry_decoded;
  uint64_t entry_heap_bytes = 0;
  std::string probe;
};

struct OnPairFetch {
  void Reset(const Header&, duckdb::data_ptr_t) {
    table.reset();
    groups.Clear();
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& counts, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (!table) {
      Open(h, base);
    }
    const auto tokens = TokenFrame(f, table->Bits());
    const auto first = FirstToken(groups, counts, tokens, lo, entry);
    const uint64_t count = counts.At(entry);
    SDB_ENSURE(first + count <= tokens.comp_len, "col codec: corrupted entry");
    if (const auto need = table->Capacity(count); buf.size() < need) {
      buf.resize(std::max<uint64_t>(need, buf.size() * 2));
    }
    const auto n = table->Decode(
      reinterpret_cast<const uint8_t*>(base + h.off_data) + f.comp_off, first,
      count, buf.data(), buf.size());
    ++walked;
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, buf.data(), n);
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    table.emplace();
    SDB_ENSURE(table->Open(OnPairSide(h, base)),
               "col codec: corrupted symbol table");
    const auto* frames = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(frames, i).first_entry,
                       i + 1 < h.frame_count
                         ? FrameAt(frames, i + 1).first_entry
                         : h.entry_count};
    });
  }

  std::optional<OnPairTable> table;
  ChainGroups groups;
  std::string buf;
  uint64_t walked = 0;
};

struct OnPairLeaf {
  static constexpr ByteCodec kCodec = ByteCodec::OnPair;
  using Scan = OnPairScan;
  using Fetch = OnPairFetch;
};

}  // namespace irs::codecs::text
