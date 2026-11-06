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
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Codec>
struct BlobScan {
  template<typename Frames>
  BlobScan(duckdb::ColumnSegment&, const Frames&) {}

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    std::string_view dictionary;
    bool windowed = f >= Codec::kFirstWindowed;
    if (s.header.flags == FrameDictionary::Local) {
      dictionary = LocalDictionary(s);
      windowed = Codec::kFirstWindowed != kNeverWindowed;
    } else if (f != 0 && s.header.flags == FrameDictionary::FirstFrame) {
      if (!s.decoded[0]) {
        s.DecodeFrame(0);
      }
      dictionary = s.FirstFrame();
    }
    Inflate(s, f, out, dictionary, windowed);
    Entries(s, f, [&](uint32_t e, uint64_t off, uint32_t len) {
      s.values[e] = duckdb::string_t{out + off, len};
    });
  }

  template<typename Frames>
  void Inflate(const Frames& s, uint32_t f, char* out,
               std::string_view dictionary, bool windowed) {
    char* staging = out;
    if (windowed && !dictionary.empty()) {
      staging = Window(s, dictionary);
      dictionary = {staging - dictionary.size(), dictionary.size()};
    }
    Decompress(s, f, out, staging, dictionary);
  }

  template<typename Frames>
  void Decompress(const Frames& s, uint32_t f, char* out, char* staging,
                  const auto& dictionary) {
    const auto& m = s.frames[f];
    SDB_ENSURE(decoder.Decompress(s.Data() + m.comp_off, m.comp_len, staging,
                                  m.raw_len, dictionary),
               "col codec: corrupted frame");
    if (staging != out) {
      std::memcpy(out, staging, m.raw_len);
    }
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

  template<typename Frames>
  char* Window(const Frames& s, std::string_view dictionary) {
    if (!window) {
      Allocate(s, dictionary.size());
      std::memcpy(window.get(), dictionary.data(), dictionary.size());
    }
    return window.get() + dictionary.size();
  }

  template<typename Frames>
  std::string_view LocalDictionary(const Frames& s) {
    const auto& h = s.header;
    if (!window) {
      Allocate(s, h.dictionary_size);
      SDB_ENSURE(Lz4::Decompressor::Decompress(
                   reinterpret_cast<const char*>(s.base + h.off_symtab),
                   h.symtab_size, window.get(), h.dictionary_size),
                 "col codec: corrupted dictionary");
    }
    return {window.get(), h.dictionary_size};
  }

  template<typename Frames>
  void Allocate(const Frames& s, size_t dictionary_size) {
    uint32_t largest = 0;
    for (const auto& m : s.frames) {
      largest = std::max(largest, m.raw_len);
    }
    window = duckdb::make_unsafe_uniq_array_uninitialized<char>(
      dictionary_size + largest);
  }

  uint64_t HeapBytes() const noexcept { return 0; }

  typename Codec::Decompressor decoder;
  duckdb::unsafe_unique_array<char> window;
};

template<typename Codec>
struct BlobFetch {
  void Reset(const Header&, duckdb::data_ptr_t) noexcept {
    frame = kNoFrame;
    dictionary_ready = false;
    decoded_total = 0;
  }

  uint64_t Walked() const noexcept { return 0; }

  uint64_t DecodedBytes() const noexcept { return decoded_total; }

  IRS_FORCE_INLINE void Emit(duckdb::ColumnSegment&, const Header& h,
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
    if (capacity < f.raw_len) {
      capacity = std::max<size_t>(f.raw_len, 1);
      raw = duckdb::make_unsafe_uniq_array_uninitialized<char>(capacity);
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
    const bool with_dictionary =
      h.flags == FrameDictionary::Local ||
      (h.flags == FrameDictionary::FirstFrame && lo != 0);
    SDB_ENSURE(static_cast<uint64_t>(f.comp_off) + f.comp_len <= h.data_size,
               "col codec: corrupted frame table");
    if (!decoder) {
      decoder.emplace();
    }
    auto& d = *decoder;
    if (with_dictionary && !dictionary_ready) {
      if (h.flags == FrameDictionary::Local) {
        Reserve(static_cast<size_t>(h.dictionary_size) + f.raw_len);
        SDB_ENSURE(Lz4::Decompressor::Decompress(
                     reinterpret_cast<const char*>(base + h.off_symtab),
                     h.symtab_size, dictionary.get(), h.dictionary_size),
                   "col codec: corrupted dictionary");
        dictionary_size = h.dictionary_size;
        decoded_total += h.dictionary_size;
      } else {
        const auto f0 = duckdb::Load<FrameMeta>(base + h.off_frames);
        SDB_ENSURE(
          static_cast<uint64_t>(f0.comp_off) + f0.comp_len <= h.data_size,
          "col codec: corrupted frame table");
        Reserve(static_cast<size_t>(f0.raw_len) + f.raw_len);
        SDB_ENSURE(
          d.Decompress(
            reinterpret_cast<const char*>(base + h.off_data) + f0.comp_off,
            f0.comp_len, dictionary.get(), f0.raw_len),
          "col codec: corrupted frame");
        dictionary_size = f0.raw_len;
        decoded_total += f0.raw_len;
      }
      dictionary_ready = true;
    }
    if (with_dictionary && dictionary_capacity < dictionary_size + f.raw_len) {
      dictionary_capacity = dictionary_size + f.raw_len;
      auto grown =
        duckdb::make_unsafe_uniq_array_uninitialized<char>(dictionary_capacity);
      std::memcpy(grown.get(), dictionary.get(), dictionary_size);
      dictionary = std::move(grown);
    }
    auto* dst =
      with_dictionary ? dictionary.get() + dictionary_size : raw.get();
    frame_data = dst;
    const auto* src =
      reinterpret_cast<const char*>(base + h.off_data) + f.comp_off;
    const std::string_view prefix =
      with_dictionary ? std::string_view{dictionary.get(), dictionary_size}
                      : std::string_view{};
    const size_t got =
      want == f.raw_len
        ? (d.Decompress(src, f.comp_len, dst, f.raw_len, prefix) ? f.raw_len
                                                                 : 0)
        : d.DecompressPrefix(src, f.comp_len, dst, want, f.raw_len, prefix);
    SDB_ENSURE(got >= want, "col codec: corrupted frame");
    decoded = std::min<size_t>(got, f.raw_len);
    decoded_total += decoded;
  }

  void Reserve(size_t need) {
    if (dictionary_capacity < need) {
      dictionary_capacity = std::max<size_t>(need, 1);
      dictionary =
        duckdb::make_unsafe_uniq_array_uninitialized<char>(dictionary_capacity);
    }
  }

  uint32_t frame = kNoFrame;
  uint32_t first_entry = 0;
  uint32_t known = 0;
  uint32_t hits = 0;
  bool dense = false;
  size_t decoded = 0;
  size_t capacity = 0;
  duckdb::unsafe_unique_array<char> raw;
  std::vector<uint32_t> offsets;
  bool dictionary_ready = false;
  size_t dictionary_capacity = 0;
  uint32_t dictionary_size = 0;
  duckdb::unsafe_unique_array<char> dictionary;
  const char* frame_data = nullptr;
  uint64_t decoded_total = 0;
  std::optional<typename Codec::Decompressor> decoder;
};

template<typename Codec>
struct BlobLeaf {
  static constexpr ByteCodec kCodec = Codec::kCodec;
  using Scan = BlobScan<Codec>;
  using Fetch = BlobFetch<Codec>;
};

}  // namespace irs::codecs::text
