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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/frame_spans.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text::blob {

inline constexpr size_t kProbeFrames = 1;
inline constexpr size_t kLocalMinFrames = 8;
inline constexpr uint64_t kLocalDictionaryPacking = 2;

inline uint64_t Scale(uint64_t sampled, uint64_t sampled_raw,
                      uint64_t raw) noexcept {
  return sampled_raw == 0
           ? 0
           : static_cast<uint64_t>(static_cast<double>(sampled) *
                                   static_cast<double>(raw) /
                                   static_cast<double>(sampled_raw));
}

template<typename Compressor>
void ResetCompressor(std::optional<Compressor>& codec, uint8_t& held,
                     uint8_t level) {
  if (!codec || held != level) {
    codec.emplace(level);
    held = level;
  } else {
    codec->Clear();
  }
}

template<typename Codec>
size_t CompressedSize(std::string_view raw, std::string& scratch,
                      typename Codec::Compressor& codec) {
  const auto bound = Codec::Bound(raw.size());
  size_t n = 0;
  scratch.resize_and_overwrite(bound, [&](char* buf, size_t) {
    n = codec.Compress(raw.data(), raw.size(), buf, bound);
    return n;
  });
  return n;
}

template<typename Codec, typename Rows>
class CompressionState {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static constexpr Shape kShape = Rows::kShape;
  static_assert(!Sorted(kCodec));

  CompressionState(ColumnDataCheckpointData& data, DedupScratch& dedup)
    : _rows{MakeRows<Rows>(data.type, dedup, data.tuning.strings.codes_ratio)},
      _history{data.tuning.strings.history[static_cast<size_t>(kCodec)]} {}

  void InitCompression(const Config& config, uint64_t first_row,
                       size_t distinct, Segment& out) {
    _rows.Begin(config, first_row, distinct);
    Configure(config.level, config.layout);
    _frame.clear();
    _out = &out;
    out.bytes.Truncate(0);
    _frames.clear();
  }

  IRS_FORCE_INLINE void Compress(const duckdb::string_t* value) {
    _rows.AddValue(value, [&](const duckdb::string_t& entry, uint32_t id) {
      Append(entry, id);
    });
  }

  IRS_FORCE_INLINE void Compress(
    uint32_t code, std::span<const duckdb::string_t* const> values) {
    _rows.AddCode(
      code, values,
      [&](const duckdb::string_t& entry, uint32_t id) { Append(entry, id); });
  }

  IRS_FORCE_INLINE bool Full(uint32_t target) {
    return _rows.EstimateDue(target) && _rows.Estimate() + Estimate() >= target;
  }

  void FinalizeCompress() {
    const auto rows = _rows.View();
    SDB_ASSERT(_packed + _frame.size() == rows.raw);
    const auto lengths = _rows.Lengths();
    _spans.Close();
    if (_layout == FrameDictionary::Local && _spans.size() >= kLocalMinFrames) {
      _spans.Sizes(_sizes);
      _trained = _codec->Train(_frame, _sizes);
    }
    CompressRest();
    if (_trained && !Pays()) {
      _trained = false;
      _codec->Clear();
      _out->bytes.Truncate(0);
      _frames.clear();
      CompressRest();
    }
    Count();
    absl::c_transform(rows.entries, lengths.begin(),
                      [](const duckdb::string_t& e) { return e.GetSize(); });
    const auto flags = Flags();
    const bool local = flags == FrameDictionary::Local;
    _rows.Write(
      {
        .codec = kCodec,
        .frames = _frames,
        .max_length = rows.max_len,
        .symtab = local ? _codec->Stored() : std::string_view{},
        .dictionary_size =
          local ? static_cast<uint32_t>(_codec->Dictionary().size()) : 0,
        .flags = flags,
      },
      *_out);
  }

 private:
  void Append(const duckdb::string_t& value, uint32_t entry) {
    const auto size = value.GetSize();
    if (_spans.Add(entry, size)) {
      Closed();
    }
    _frame.append(value.GetData(), size);
  }

  uint64_t Estimate() const noexcept {
    const auto raw = _rows.View().raw;
    return (_spans.size() + 1) * kFrameMetaSize + Written() +
           (_layout == FrameDictionary::Local
              ? SegmentDictionaryBytes(raw, Codec::kDictionaryCap) /
                  kLocalDictionaryPacking
              : 0) +
           static_cast<uint64_t>(static_cast<double>(raw - _packed) *
                                 Ratio(History(), 1.0));
  }

  RatioHistory& History() const noexcept {
    return _history[static_cast<size_t>(kShape)];
  }

  void Configure(uint8_t level, FrameDictionary layout) {
    _layout = layout;
    _trained = false;
    _first_frame = false;
    _spans.Reset(FrameShapeOf<Codec>(layout));
    _counted = 0;
    _packed = 0;
    ResetCompressor(_codec, _level, level);
  }

  FrameDictionary Flags() const noexcept {
    if (_trained) {
      return FrameDictionary::Local;
    }
    return _first_frame && _frames.size() > 1 ? FrameDictionary::FirstFrame
                                              : FrameDictionary::None;
  }

  void Closed() {
    if (_layout != FrameDictionary::Local) {
      CompressRest();
      Count();
      _frame.clear();
      return;
    }
    if (auto& hist = History(); hist.raw == 0) {
      const auto raw = Slice(_spans.size() - 1);
      hist.raw += raw.size();
      hist.comp += blob::CompressedSize<Codec>(raw, _scratch, *_codec);
    }
  }

  void Count() {
    auto& hist = History();
    for (; _counted < _frames.size(); ++_counted) {
      hist.raw += _frames[_counted].raw_len;
      hist.comp += _frames[_counted].comp_len;
      _packed += _frames[_counted].raw_len;
    }
  }

  std::string_view Slice(size_t i) const noexcept {
    return std::string_view{_frame}.substr(_spans[i].offset - _packed,
                                           _spans[i].raw);
  }

  uint64_t Written() const noexcept { return _out->bytes.Size(); }

  void CompressRest() {
    for (auto i = _frames.size(); i < _spans.size(); ++i) {
      const auto raw = Slice(i);
      const auto at = Written();
      const auto n = CompressFrame(raw);
      _frames.push_back({static_cast<uint32_t>(_spans[i].first),
                         static_cast<uint32_t>(raw.size()),
                         static_cast<uint32_t>(at), static_cast<uint32_t>(n)});
      if (i == 0 && !_trained && _spans.Shape().first != 0 && !raw.empty() &&
          (_spans.size() > 1 || _spans.Open())) {
        _codec->Use(raw);
        _first_frame = true;
      }
    }
  }

  bool Pays() {
    ResetCompressor(_probe, _probe_level, _level);
    const auto first = Slice(0);
    const auto first_plain =
      blob::CompressedSize<Codec>(first, _scratch, *_probe);
    _probe->Use(first);
    uint64_t sampled_raw = 0;
    uint64_t sampled_trained = 0;
    uint64_t sampled_first = 0;
    _spans.Sample(
      kProbeFrames, [](size_t) { return 0.5; },
      [&](size_t i) {
        const auto raw = Slice(i);
        sampled_raw += raw.size();
        sampled_trained += _frames[i].comp_len;
        sampled_first += blob::CompressedSize<Codec>(raw, _scratch, *_probe);
      });
    const auto rest_raw = _frame.size() - first.size();
    return _frames[0].comp_len + _codec->Stored().size() +
             Scale(sampled_trained, sampled_raw, rest_raw) <
           first_plain + Scale(sampled_first, sampled_raw, rest_raw);
  }

  size_t CompressFrame(std::string_view raw) {
    const auto bound = Codec::Bound(raw.size());
    auto* out = _out->bytes.Extend(bound);
    const auto n = _codec->Compress(raw.data(), raw.size(), out, bound);
    _out->bytes.Shrink(bound - n);
    return n;
  }

  Rows _rows;
  RatioHistory* _history;
  std::optional<typename Codec::Compressor> _codec;
  std::optional<typename Codec::Compressor> _probe;
  uint8_t _level = 0;
  uint8_t _probe_level = 0;
  FrameDictionary _layout = FrameDictionary::FirstFrame;
  bool _trained = false;
  bool _first_frame = false;
  size_t _counted = 0;
  uint64_t _packed = 0;
  std::string _frame;
  Segment* _out = nullptr;
  std::string _scratch;
  FrameSpans _spans;
  std::vector<size_t> _sizes;
  std::vector<FrameMeta> _frames;
};

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
struct Fetch {
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
struct Scan {
  static constexpr ByteCodec kCodec = Codec::kCodec;
  using Fetch = blob::Fetch<Codec>;

  template<typename Frames>
  Scan(const ColumnSegment&, const Frames&) {}

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

  template<typename Frames, typename Entry>
  void Ensure(Frames& s, duckdb::idx_t count, Entry entry_at, uint32_t,
              uint32_t) {
    s.EnsureEach(count, entry_at);
  }

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t, uint32_t) {
    s.EnsureEach(count, entry_at);
    s.WriteRefs(result, result_offset, count, entry_at);
  }

  uint64_t HeapBytes() const noexcept { return 0; }

  FrameInflater<Codec> inflater;
};

}  // namespace irs::codecs::text::blob
