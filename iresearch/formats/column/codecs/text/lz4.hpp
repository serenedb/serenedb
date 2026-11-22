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
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::blob {

inline constexpr uint64_t kInflateByteCost = 6;
inline constexpr uint64_t kFrameCopyByteCost = 2;
inline constexpr uint64_t kFrameEntryCost = 72;
inline constexpr uint64_t kPrefixEntryCost = 16;
inline constexpr uint64_t kHitByteCost = 2;
inline constexpr uint64_t kHitCost = 128;

struct Lz4Scan final : Scan<Lz4> {
  using Scan<Lz4>::Scan;

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count,
                                                               result_offset);
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    bool referenced = false;
    uint32_t f = kNoFrame;
    for (duckdb::idx_t i = 0; i < count;) {
      auto e = static_cast<uint32_t>(entry_at(i));
      if (f == kNoFrame || e < s.frames[f].first_entry || e >= s.FrameEnd(f)) {
        f = s.FrameOf(e);
      }
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        if (next < e || next >= end) {
          break;
        }
        e = next;
      }
      if (!s.decoded[f]) {
        const uint64_t rows = end - first;
        const uint64_t prefix = e - first + 1;
        const uint64_t hits = j - i;
        const uint64_t entry_bytes = s.frames[f].raw_len / rows;
        const uint64_t frame_byte_cost =
          kInflateByteCost +
          (s.header.flags == FrameDictionary::None ? 0 : kFrameCopyByteCost);
        const bool done =
          end <= window_end ||
          count * uint64_t{end - window_end} < window_end - window_first;
        if (done &&
            prefix * (entry_bytes * kInflateByteCost + kPrefixEntryCost) +
                hits * (entry_bytes * kHitByteCost + kHitCost) <=
              rows * (entry_bytes * frame_byte_cost + kFrameEntryCost)) {
          EmitRows(s, f, i, j, entry_at, arena, writer);
          i = j;
          continue;
        }
        s.DecodeFrame(f);
      }
      referenced = true;
      for (; i < j; ++i) {
        writer.WriteStringRef(s.values[entry_at(i)]);
      }
    }
    if (referenced) {
      s.ReferenceHeap(result);
    }
  }

  template<typename Frames, typename Entry, typename Writer>
  void EmitRows(Frames& s, uint32_t f, duckdb::idx_t i, duckdb::idx_t j,
                Entry entry_at, duckdb::ArenaAllocator& arena, Writer& writer) {
    const auto& m = s.frames[f];
    const auto first = m.first_entry;
    const auto prefix = static_cast<uint32_t>(entry_at(j - 1)) - first + 1;
    if (ends.size() < prefix) {
      ends.resize(prefix);
    }
    s.length_reader.Read(first, prefix, ends.data());
    uint64_t end = 0;
    for (uint32_t k = 0; k < prefix; ++k) {
      end += ends[k];
      ends[k] = static_cast<uint32_t>(end);
    }
    SDB_ENSURE(end <= m.raw_len, "col codec: corrupted entry lengths");
    if (raw.size() < m.raw_len) {
      raw.resize_and_overwrite(m.raw_len, [](char*, size_t n) { return n; });
    }
    const auto got = inflater.Inflate(s.header, s.base, m, f, raw.data(), end);
    SDB_ENSURE(got.size() >= end, "col codec: corrupted frame");
    const auto span = [&](duckdb::idx_t k) {
      const auto at = static_cast<uint32_t>(entry_at(k)) - first;
      const auto begin = at == 0 ? 0 : ends[at - 1];
      return std::string_view{got.data() + begin, ends[at] - begin};
    };
    uint64_t bytes = 0;
    for (auto k = i; k < j; ++k) {
      const auto len = span(k).size();
      bytes += len > duckdb::string_t::INLINE_LENGTH ? len : 0;
    }
    auto* out =
      bytes != 0 ? duckdb::char_ptr_cast(arena.Allocate(bytes)) : nullptr;
    for (auto k = i; k < j; ++k) {
      const auto value = span(k);
      const auto len = static_cast<uint32_t>(value.size());
      if (len <= duckdb::string_t::INLINE_LENGTH) {
        writer.WriteStringRef(duckdb::string_t{value.data(), len});
        continue;
      }
      std::memcpy(out, value.data(), len);
      writer.WriteStringRef(duckdb::string_t{out, len});
      out += len;
    }
  }

  std::string raw;
  std::vector<uint32_t> ends;
};

}  // namespace irs::codecs::text::blob
