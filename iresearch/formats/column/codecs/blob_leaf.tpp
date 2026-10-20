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
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Codec>
inline constexpr bool kTakesTrained =
  requires(typename Codec::Decompressor& d, const TrainedDictionary& t) {
    d.SetTrained(t);
  };

template<typename Codec>
struct BlobScan {
  template<typename Frames>
  BlobScan(duckdb::ColumnSegment& segment, const Frames& s)
    : trained{Trainable(Codec::kCodec) ? TrainedOf(segment, s.header)
                                       : nullptr} {}

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    if (f != 0 && s.header.flags == kFrameDictionary && !s.decoded[0]) {
      s.DecodeFrame(0);
    }
    if constexpr (Trainable(Codec::kCodec)) {
      if (trained) {
        DecodeTrained(s, f, out);
        return;
      }
    }
    const auto dictionary = DictionaryFor(s, f);
    char* staging = out;
    if (f >= Codec::kFirstWindowed && !dictionary.empty()) {
      staging = Window(s, dictionary);
    }
    decoder.SetDictionary(
      staging == out
        ? dictionary
        : std::string_view{staging - dictionary.size(), dictionary.size()});
    Decode(s, f, out, staging);
  }

  template<typename Frames>
  void DecodeTrained(Frames& s, uint32_t f, char* out) {
    if constexpr (kTakesTrained<Codec>) {
      decoder.SetTrained(*trained);
      Decode(s, f, out, out);
    } else {
      const auto bytes = trained->Bytes();
      char* staging = Window(s, bytes);
      decoder.SetDictionary({staging - bytes.size(), bytes.size()});
      Decode(s, f, out, staging);
    }
  }

  template<typename Frames>
  void Decode(Frames& s, uint32_t f, char* out, char* staging) {
    const auto& m = s.frames[f];
    SDB_ENSURE(
      decoder.Decompress(s.Data() + m.comp_off, m.comp_len, staging, m.raw_len),
      "col codec: corrupted frame");
    if (staging != out) {
      std::memcpy(out, staging, m.raw_len);
    }
    const auto end = s.FrameEnd(f);
    uint64_t off = 0;
    for (uint32_t e = m.first_entry; e < end; ++e) {
      const auto len = s.length_reader.At(e);
      SDB_ENSURE(off + len <= m.raw_len, "col codec: corrupted entry lengths");
      s.values[e] = duckdb::string_t{out + off, len};
      off += len;
    }
    SDB_ENSURE(off == m.raw_len, "col codec: corrupted entry lengths");
  }

  template<typename Frames>
  static std::string_view DictionaryFor(const Frames& s, uint32_t f) noexcept {
    if (f == 0 || s.header.flags != kFrameDictionary) {
      return {};
    }
    return {s.heap + s.frame_off[0], s.frames[0].raw_len};
  }

  template<typename Frames>
  char* Window(const Frames& s, std::string_view dictionary) {
    if (!window) {
      uint32_t largest = 0;
      for (const auto& m : s.frames) {
        largest = std::max(largest, m.raw_len);
      }
      window =
        duckdb::make_unsafe_uniq_array<char>(dictionary.size() + largest);
      std::memcpy(window.get(), dictionary.data(), dictionary.size());
    }
    return window.get() + dictionary.size();
  }

  uint64_t HeapBytes() const noexcept { return 0; }

  const TrainedDictionary* trained;
  typename Codec::Decompressor decoder;
  duckdb::unsafe_unique_array<char> window;
};

template<typename Codec>
struct BlobFetch {
  void Reset(const Header&, duckdb::const_data_ptr_t) noexcept {
    frame = kNoFrame;
    dictionary_ready = false;
    decoded_total = 0;
  }

  uint64_t Walked() const noexcept { return 0; }

  uint64_t DecodedBytes() const noexcept { return decoded_total; }

  IRS_FORCE_INLINE void Emit(duckdb::ColumnSegment& segment, const Header& h,
                             duckdb::data_ptr_t base, PackedReader& lengths,
                             uint32_t lo, const FrameMeta& f, uint32_t entry,
                             duckdb::Vector& result, duckdb::idx_t result_idx,
                             bool batched) {
    if (frame != lo) {
      Open(h, base, lengths, lo, f, batched);
    }
    ++hits;
    const auto k = entry - first_entry;
    const auto offset = offsets[k];
    const auto len = offsets[k + 1] - offset;
    const size_t want = offset + len;
    if (decoded < want) {
      Inflate(segment, h, base, lo, f, want, batched);
    }
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, frame_data + offset, len);
  }

  void Open(const Header& h, duckdb::data_ptr_t base, PackedReader& lengths,
            uint32_t lo, const FrameMeta& f, bool batched) {
    const auto end =
      lo + 1 < h.frame_count
        ? FrameMeta::Load(base + h.off_frames + (lo + 1) * kFrameMetaSize)
            .first_entry
        : h.entry_count;
    SDB_ENSURE(end > f.first_entry, "col codec: corrupted frame table");
    offsets.resize(end - f.first_entry + 1);
    uint64_t offset = 0;
    for (uint32_t e = f.first_entry; e < end; ++e) {
      offsets[e - f.first_entry] = static_cast<uint32_t>(offset);
      offset += lengths.At(e);
    }
    SDB_ENSURE(offset == f.raw_len, "col codec: corrupted entry lengths");
    offsets.back() = static_cast<uint32_t>(offset);
    if (capacity < f.raw_len) {
      capacity = std::max<size_t>(f.raw_len, 1);
      raw = duckdb::make_unsafe_uniq_array<char>(capacity);
    }
    frame = lo;
    first_entry = f.first_entry;
    dense = !batched && hits > 1;
    hits = 0;
    decoded = 0;
  }

  void Inflate(duckdb::ColumnSegment& segment, const Header& h,
               duckdb::const_data_ptr_t base, uint32_t lo, const FrameMeta& f,
               size_t want, bool batched) {
    const size_t target = !batched && (hits > 1 || dense) ? f.raw_len : want;
    const bool with_dictionary = h.flags == kFrameDictionary && lo != 0;
    const TrainedDictionary* trained = nullptr;
    if constexpr (Trainable(Codec::kCodec)) {
      trained = TrainedOf(segment, h);
    }
    Decode(f, h, base, target, with_dictionary, trained);
  }

  void Decode(const FrameMeta& f, const Header& h,
              duckdb::const_data_ptr_t base, size_t want, bool with_dictionary,
              const TrainedDictionary* trained) {
    SDB_ENSURE(static_cast<uint64_t>(f.comp_off) + f.comp_len <= h.data_size,
               "col codec: corrupted frame table");
    if (!decoder) {
      decoder.emplace();
    }
    auto& d = *decoder;
    if (trained && !kTakesTrained<Codec>) {
      if (trained_loaded != trained->Id()) {
        const auto bytes = trained->Bytes();
        const size_t need = bytes.size() + f.raw_len;
        if (dictionary_capacity < need) {
          dictionary_capacity = need;
          dictionary =
            duckdb::make_unsafe_uniq_array<char>(dictionary_capacity);
        }
        std::memcpy(dictionary.get(), bytes.data(), bytes.size());
        dictionary_size = static_cast<uint32_t>(bytes.size());
        trained_loaded = trained->Id();
      }
      with_dictionary = true;
      dictionary_ready = true;
    } else if (with_dictionary && !dictionary_ready) {
      trained_loaded = 0;
      const auto f0 = FrameMeta::Load(base + h.off_frames);
      SDB_ENSURE(
        static_cast<uint64_t>(f0.comp_off) + f0.comp_len <= h.data_size,
        "col codec: corrupted frame table");
      const size_t need = static_cast<size_t>(f0.raw_len) + f.raw_len;
      if (dictionary_capacity < need) {
        dictionary_capacity = std::max<size_t>(need, 1);
        dictionary = duckdb::make_unsafe_uniq_array<char>(dictionary_capacity);
      }
      d.SetDictionary({});
      SDB_ENSURE(d.Decompress(reinterpret_cast<const char*>(base + h.off_data) +
                                f0.comp_off,
                              f0.comp_len, dictionary.get(), f0.raw_len),
                 "col codec: corrupted frame");
      dictionary_size = f0.raw_len;
      dictionary_ready = true;
      decoded_total += f0.raw_len;
    }
    if (with_dictionary && dictionary_capacity < dictionary_size + f.raw_len) {
      dictionary_capacity = dictionary_size + f.raw_len;
      auto grown = duckdb::make_unsafe_uniq_array<char>(dictionary_capacity);
      std::memcpy(grown.get(), dictionary.get(), dictionary_size);
      dictionary = std::move(grown);
    }
    const std::string_view prefix =
      with_dictionary ? std::string_view{dictionary.get(), dictionary_size}
                      : std::string_view{};
    if constexpr (kTakesTrained<Codec>) {
      if (trained) {
        d.SetTrained(*trained);
      } else {
        d.SetDictionary(prefix);
      }
    } else {
      d.SetDictionary(prefix);
    }
    frame_data =
      with_dictionary ? dictionary.get() + dictionary_size : raw.get();
    const auto* src =
      reinterpret_cast<const char*>(base + h.off_data) + f.comp_off;
    const size_t got =
      want == f.raw_len
        ? (d.Decompress(src, f.comp_len, frame_data, f.raw_len) ? f.raw_len : 0)
        : d.DecompressPrefix(src, f.comp_len, frame_data, want, f.raw_len);
    SDB_ENSURE(got >= want, "col codec: corrupted frame");
    decoded = std::min<size_t>(got, f.raw_len);
    decoded_total += decoded;
  }

  uint32_t frame = kNoFrame;
  uint32_t first_entry = 0;
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
  uint64_t trained_loaded = 0;
  char* frame_data = nullptr;
  uint64_t decoded_total = 0;
  std::optional<typename Codec::Decompressor> decoder;
};

template<typename Codec>
struct BlobLeaf {
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static constexpr bool kSorted = false;
  static constexpr bool kSparseGroups = false;
  using Scan = BlobScan<Codec>;
  using Fetch = BlobFetch<Codec>;
};

}  // namespace irs::codecs::text
