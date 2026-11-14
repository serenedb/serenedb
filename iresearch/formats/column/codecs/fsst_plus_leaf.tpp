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
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {

class FsstPlusPrefixes {
 public:
  bool Open(std::string_view side, uint32_t table_size, FsstDecoder& decoder) {
    if (side.size() < static_cast<uint64_t>(table_size) + sizeof(uint32_t) ||
        (table_size != 0 && !decoder.Import(side.substr(0, table_size)))) {
      return false;
    }
    const auto rest = side.substr(table_size);
    _count = duckdb::Load<uint32_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(rest.data()));
    const auto head = sizeof(uint32_t) * (uint64_t{_count} + 1);
    if (head > rest.size() || (table_size == 0 && _count != 0)) {
      return false;
    }
    _ends = rest.data() + sizeof(uint32_t);
    _bytes = rest.substr(head);
    uint32_t prev = 0;
    for (uint32_t g = 0; g < _count; ++g) {
      const auto end = End(g);
      if (end <= prev || end > _bytes.size()) {
        return false;
      }
      prev = end;
    }
    return prev == _bytes.size();
  }

  uint32_t Count() const noexcept { return _count; }

  std::string_view Encoded(uint32_t id) const noexcept {
    SDB_ASSERT(id != 0 && id <= _count);
    const auto from = id == 1 ? 0 : End(id - 2);
    return _bytes.substr(from, End(id - 1) - from);
  }

 private:
  uint32_t End(uint32_t g) const noexcept {
    return duckdb::Load<uint32_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(_ends + g * sizeof(uint32_t)));
  }

  const char* _ends = nullptr;
  std::string_view _bytes;
  uint32_t _count = 0;
};

inline std::string_view SideSection(const Header& h,
                                    duckdb::const_data_ptr_t base) noexcept {
  return {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size};
}

inline uint64_t EntryOffset(ChainGroups& groups, PackedReader& lengths,
                            const FrameMeta& m, uint32_t f, uint32_t e) {
  const auto g = (e - m.first_entry) / kChainRestart;
  uint64_t offset = groups.Offset(lengths, m, f, g);
  for (auto k = m.first_entry + g * kChainRestart; k < e; ++k) {
    offset += lengths.At(k);
  }
  return offset;
}

inline std::string_view DecodePlusEntry(const FsstDecoder& fsst,
                                        std::string_view prefix, const char* in,
                                        uint64_t enc, std::string& buf) {
  if (const auto need = prefix.size() + enc * kFsstMaxExpansion;
      buf.size() < need) {
    buf.resize(std::max<uint64_t>(need, buf.size() * 2));
  }
  if (!prefix.empty()) {
    std::memcpy(buf.data(), prefix.data(), prefix.size());
  }
  const auto room = buf.size() - prefix.size();
  const auto dec =
    enc == 0 ? 0 : fsst.Decode(in, enc, buf.data() + prefix.size(), room);
  SDB_ENSURE(dec <= room, "col codec: corrupted entry");
  return {buf.data(), prefix.size() + dec};
}

struct FsstPlusScan {
  static constexpr uint32_t kEntryGroup = 1;

  template<typename Frames>
  FsstPlusScan(const ColumnSegment&, const Frames& s)
    : ids{s.base + s.header.off_lcps, s.header.lcp_width} {
    SDB_ENSURE(prefixes.Open(SideSection(s.header, s.base),
                             s.header.dictionary_size, decoder),
               "col codec: corrupted symbol table");
    prefix_ends.assign(prefixes.Count() + 1, 0);
    for (uint32_t id = 1; id <= prefixes.Count(); ++id) {
      const auto encoded = prefixes.Encoded(id);
      const auto at = heap.size();
      heap.resize(at + encoded.size() * kFsstMaxExpansion);
      const auto room = heap.size() - at;
      const auto n =
        decoder.Decode(encoded.data(), encoded.size(), heap.data() + at, room);
      SDB_ENSURE(n <= room, "col codec: corrupted prefix");
      heap.resize(at + n);
      prefix_ends[id] = heap.size();
    }
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    entry_decoded.assign(s.header.entry_count, 0);
  }

  std::string_view Prefix(uint32_t id) const noexcept {
    return id == 0
             ? std::string_view{}
             : std::string_view{heap}.substr(
                 prefix_ends[id - 1], prefix_ends[id] - prefix_ends[id - 1]);
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto end = s.FrameEnd(f);
    const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    uint64_t enc_off = m.comp_off;
    uint64_t off = 0;
    for (auto e = m.first_entry; e < end; ++e) {
      const auto id = ids.At(e);
      const uint64_t enc = s.length_reader.At(e);
      SDB_ENSURE(id <= prefixes.Count() && enc_off + enc <= enc_end,
                 "col codec: corrupted entry");
      const auto prefix = Prefix(id);
      SDB_ENSURE(off + prefix.size() <= m.raw_len,
                 "col codec: corrupted frame");
      if (!prefix.empty()) {
        std::memcpy(out + off, prefix.data(), prefix.size());
      }
      const auto room = m.raw_len - off - prefix.size();
      const auto dec = enc == 0
                         ? 0
                         : decoder.Decode(s.Data() + enc_off, enc,
                                          out + off + prefix.size(), room);
      SDB_ENSURE(dec <= room, "col codec: corrupted entry");
      const std::string_view v{out + off, prefix.size() + dec};
      SDB_ASSERT(!Frames::kDedup || e == m.first_entry ||
                 View(s.values[e - 1]) < v);
      s.Keep(e, v);
      off += v.size();
      enc_off += enc;
    }
    SDB_ENSURE(off == m.raw_len && enc_off == enc_end,
               "col codec: corrupted frame");
  }

  template<typename Frames>
  std::string_view Decode(Frames& s, uint32_t e, uint32_t f, std::string& buf) {
    const auto& m = s.frames[f];
    const auto enc_off = EntryOffset(groups, s.length_reader, m, f, e);
    const uint64_t enc = s.length_reader.At(e);
    const auto id = ids.At(e);
    SDB_ENSURE(
      id <= prefixes.Count() &&
        enc_off + enc <= static_cast<uint64_t>(m.comp_off) + m.comp_len,
      "col codec: corrupted entry");
    return DecodePlusEntry(decoder, Prefix(id), s.Data() + enc_off, enc, buf);
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

  uint64_t HeapBytes() const noexcept { return entry_heap_bytes + heap.size(); }

  FsstDecoder decoder;
  FsstPlusPrefixes prefixes;
  PackedReader ids{nullptr, 0};
  ChainGroups groups;
  std::string heap;
  std::vector<uint64_t> prefix_ends;
  std::vector<uint8_t> entry_decoded;
  uint64_t entry_heap_bytes = 0;
  std::string probe;
};

struct FsstPlusFetch {
  void Reset(const Header& h, duckdb::data_ptr_t base) {
    decoder.reset();
    groups.Clear();
    ids = PackedReader{base + h.off_lcps, h.lcp_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& lengths, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (groups.Empty()) {
      Open(h, base);
    }
    const auto enc_off = EntryOffset(groups, lengths, f, lo, entry);
    const uint64_t enc = lengths.At(entry);
    const auto id = ids.At(entry);
    SDB_ENSURE(
      id <= prefixes.Count() &&
        enc_off + enc <= static_cast<uint64_t>(f.comp_off) + f.comp_len,
      "col codec: corrupted entry");
    std::string_view prefix;
    if (id != 0) {
      const auto encoded = prefixes.Encoded(id);
      prefix =
        DecodePlusEntry(*decoder, {}, encoded.data(), encoded.size(), head);
    }
    const auto value = DecodePlusEntry(
      *decoder, prefix,
      reinterpret_cast<const char*>(base + h.off_data) + enc_off, enc,
      value_buf);
    ++walked;
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, value.data(), value.size());
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    decoder.emplace();
    SDB_ENSURE(prefixes.Open(SideSection(h, base), h.dictionary_size, *decoder),
               "col codec: corrupted symbol table");
    const auto* table = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(table, i).first_entry,
                       i + 1 < h.frame_count ? FrameAt(table, i + 1).first_entry
                                             : h.entry_count};
    });
  }

  std::optional<FsstDecoder> decoder;
  FsstPlusPrefixes prefixes;
  PackedReader ids{nullptr, 0};
  ChainGroups groups;
  std::string head;
  std::string value_buf;
  uint64_t walked = 0;
};

struct FsstPlusLeaf {
  static constexpr ByteCodec kCodec = ByteCodec::FsstPlus;
  using Scan = FsstPlusScan;
  using Fetch = FsstPlusFetch;
};

}  // namespace irs::codecs::text
