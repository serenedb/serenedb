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
#include <atomic>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Codec>
class FrameInflater {
 public:
  std::string_view Inflate(const Header& h, duckdb::const_data_ptr_t base,
                           const FrameMeta& m, uint32_t f, char* dst,
                           size_t want) {
    const auto* data = reinterpret_cast<const char*>(base + h.off_data);
    if (!_decoder) {
      _decoder.emplace();
    }
    if (!_ready) {
      Prepare(h, base, data);
      _ready = true;
    }
    if (h.flags == FrameDictionary::FirstFrame && f == 0) {
      return _decoder->Dictionary();
    }
    return _decoder->Inflate(data + m.comp_off, m.comp_len, dst, want,
                             m.raw_len);
  }

  uint64_t Loaded() const noexcept { return _loaded; }

 private:
  void Prepare(const Header& h, duckdb::const_data_ptr_t base,
               const char* data) {
    if (h.flags == FrameDictionary::Local) {
      SDB_ENSURE(
        _decoder->Load(
          {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size},
          h.dictionary_size),
        "col codec: corrupted dictionary");
      _loaded += h.dictionary_size;
    } else if (h.flags == FrameDictionary::FirstFrame) {
      const auto f0 = duckdb::Load<FrameMeta>(base + h.off_frames);
      SDB_ENSURE(
        static_cast<uint64_t>(f0.comp_off) + f0.comp_len <= h.data_size,
        "col codec: corrupted frame table");
      SDB_ENSURE(
        _decoder->LoadFrame(data + f0.comp_off, f0.comp_len, f0.raw_len),
        "col codec: corrupted frame");
      _loaded += f0.raw_len;
    }
  }

  std::optional<typename Codec::Decompressor> _decoder;
  bool _ready = false;
  uint64_t _loaded = 0;
};

template<typename Codec>
struct BlobScan {
  template<typename Frames>
  BlobScan(const ColumnSegment&, const Frames&) {}

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto got = inflater.Inflate(s.header, s.base, m, f, out, m.raw_len);
    SDB_ENSURE(got.size() == m.raw_len, "col codec: corrupted frame");
    if (got.data() != out) {
      std::memcpy(out, got.data(), m.raw_len);
    }
    Entries(s, f, [&](uint32_t e, uint64_t off, uint32_t len) {
      s.values[e] = duckdb::string_t{out + off, len};
    });
  }

  template<typename Frames, typename Visit>
  static void Entries(Frames& s, uint32_t f, Visit&& visit) {
    const auto& m = s.frames[f];
    const auto end = s.FrameEnd(f);
    uint64_t off = 0;
    for (uint32_t e = m.first_entry; e < end; ++e) {
      const auto len = s.length_reader.At(e);
      SDB_ENSURE(off + len <= m.raw_len, "col codec: corrupted entry lengths");
      visit(e, off, len);
      off += len;
    }
    SDB_ENSURE(off == m.raw_len, "col codec: corrupted entry lengths");
  }

  template<typename Frames, typename Entry, typename Writer>
  void EmitRows(Frames& s, uint32_t f, duckdb::idx_t i, duckdb::idx_t j,
                Entry entry_at, duckdb::ArenaAllocator& arena, Writer& writer,
                FramePrefix& scratch) {
    auto& [raw, ends] = scratch;
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

  uint64_t HeapBytes() const noexcept { return 0; }

  FrameInflater<Codec> inflater;
};

template<typename Codec>
struct BlobFetch {
  void Reset(const Header&, duckdb::data_ptr_t) noexcept {
    frame = kNoFrame;
    decoded_total = 0;
  }

  uint64_t Walked() const noexcept { return 0; }

  uint64_t DecodedBytes() const noexcept {
    return decoded_total + inflater.Loaded();
  }

  IRS_FORCE_INLINE void Emit(const ColumnSegment&, const Header& h,
                             duckdb::data_ptr_t base, PackedReader& lengths,
                             uint32_t lo, const FrameMeta& f, uint32_t entry,
                             duckdb::Vector& result, duckdb::idx_t result_idx,
                             bool batched) {
    if (frame != lo) {
      Open(lo, f, batched);
    }
    ++hits;
    const auto k = entry - first_entry;
    if (known < k + 2) {
      Extend(lengths, f, k + 2);
    }
    const auto offset = offsets[k];
    const auto len = offsets[k + 1] - offset;
    const size_t want = offset + len;
    if (decoded < want) {
      Inflate(h, base, lo, f, want, batched);
    }
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, frame_data + offset, len);
  }

  void Open(uint32_t lo, const FrameMeta& f, bool batched) {
    if (raw.size() < f.raw_len) {
      raw.resize_and_overwrite(f.raw_len, [](char*, size_t n) { return n; });
    }
    frame = lo;
    first_entry = f.first_entry;
    known = 0;
    dense = !batched && hits > 1;
    hits = 0;
    decoded = 0;
  }

  void Extend(PackedReader& lengths, const FrameMeta& f, uint32_t need) {
    if (offsets.size() < need) {
      offsets.resize(need);
    }
    auto k = known;
    if (k == 0) {
      offsets[0] = 0;
      k = 1;
    }
    uint64_t offset = offsets[k - 1];
    for (; k < need; ++k) {
      offset += lengths.At(first_entry + k - 1);
      offsets[k] = static_cast<uint32_t>(offset);
    }
    SDB_ENSURE(offset <= f.raw_len, "col codec: corrupted entry lengths");
    known = need;
  }

  void Inflate(const Header& h, duckdb::const_data_ptr_t base, uint32_t lo,
               const FrameMeta& f, size_t want, bool batched) {
    if (!batched && (hits > 1 || dense)) {
      want = f.raw_len;
    }
    SDB_ENSURE(static_cast<uint64_t>(f.comp_off) + f.comp_len <= h.data_size,
               "col codec: corrupted frame table");
    const auto got = inflater.Inflate(h, base, f, lo, raw.data(), want);
    SDB_ENSURE(got.size() >= want, "col codec: corrupted frame");
    frame_data = got.data();
    decoded = std::min<size_t>(got.size(), f.raw_len);
    decoded_total += decoded;
  }

  uint32_t frame = kNoFrame;
  uint32_t first_entry = 0;
  uint32_t known = 0;
  uint32_t hits = 0;
  bool dense = false;
  size_t decoded = 0;
  std::string raw;
  std::vector<uint32_t> offsets;
  const char* frame_data = nullptr;
  uint64_t decoded_total = 0;
  FrameInflater<Codec> inflater;
};

template<typename Codec>
struct BlobLeaf {
  static constexpr ByteCodec kCodec = Codec::kCodec;
  using Scan = BlobScan<Codec>;
  using Fetch = BlobFetch<Codec>;
};

}  // namespace irs::codecs::text
