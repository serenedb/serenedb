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
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text::blob {

inline constexpr size_t kSampleFrames = 8;
inline constexpr size_t kProbeFrames = 1;
inline constexpr size_t kLocalMinFrames = 8;
inline constexpr uint64_t kInflateByteCost = 6;
inline constexpr uint64_t kFrameCopyByteCost = 2;
inline constexpr uint64_t kFrameEntryCost = 72;
inline constexpr uint64_t kPrefixEntryCost = 16;
inline constexpr uint64_t kHitByteCost = 2;
inline constexpr uint64_t kHitCost = 128;

struct FrameShape {
  size_t frame;
  size_t first;
};

template<typename Codec>
constexpr FrameShape FrameShapeOf(FrameDictionary layout) noexcept {
  if (layout == FrameDictionary::None) {
    return {kFrameRawBytes, 0};
  }
  if constexpr (Codec::kCodec == ByteCodec::Zstd) {
    return {kZstdDictionaryFrameBytes, kFrameDictionaryBytes};
  } else {
    return {kDictionaryFrameBytes, kFrameDictionaryBytes};
  }
}

class FrameSpans {
 public:
  struct Span {
    uint64_t first = 0;
    uint64_t end = 0;
    uint64_t offset = 0;
    uint64_t raw = 0;
  };

  void Reset(FrameShape shape) noexcept {
    _shape = shape;
    _spans.clear();
    _open = {};
  }

  IRS_FORCE_INLINE bool Add(uint64_t entry, uint64_t size) {
    const auto limit =
      _spans.empty() && _shape.first != 0 ? _shape.first : _shape.frame;
    const bool closed = _open.end != _open.first && _open.raw + size > limit;
    if (closed) {
      _spans.push_back(_open);
      _open = {.offset = _open.offset + _open.raw};
    }
    if (_open.end == _open.first) {
      _open.first = entry;
    }
    _open.end = entry + 1;
    _open.raw += size;
    return closed;
  }

  void Close() {
    if (Open()) {
      _spans.push_back(_open);
    }
    _open = {};
  }

  bool Open() const noexcept { return _open.end != _open.first; }

  FrameShape Shape() const noexcept { return _shape; }

  size_t size() const noexcept { return _spans.size(); }

  bool empty() const noexcept { return _spans.empty(); }

  const Span& operator[](size_t i) const noexcept { return _spans[i]; }

  std::span<const Span> All() const noexcept { return _spans; }

  template<typename Offset, typename Visit>
  void Sample(size_t count, Offset&& offset, Visit&& visit) const {
    const size_t rest = _spans.size() - 1;
    if (rest <= count) {
      for (size_t i = 1; i < _spans.size(); ++i) {
        visit(i);
      }
      return;
    }
    size_t last = 0;
    for (size_t j = 0; j < count; ++j) {
      const auto i =
        1 + static_cast<size_t>((static_cast<double>(j) + offset(j)) *
                                static_cast<double>(rest) /
                                static_cast<double>(count));
      if (i != last) {
        last = i;
        visit(i);
      }
    }
  }

 private:
  FrameShape _shape{kFrameRawBytes, 0};
  Span _open;
  std::vector<Span> _spans;
};

inline uint64_t Scale(uint64_t sampled, uint64_t sampled_raw,
                      uint64_t raw) noexcept {
  return sampled_raw == 0
           ? 0
           : static_cast<uint64_t>(static_cast<double>(sampled) *
                                   static_cast<double>(raw) /
                                   static_cast<double>(sampled_raw));
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

template<typename Codec>
class SizeEstimator {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  uint64_t EstimateSize(const Profile& p, std::string_view bytes,
                        const FrameSpans& spans, uint64_t codes_bytes,
                        uint8_t level, FrameDictionary layout, uint64_t begin) {
    if (!_codec || _level != level) {
      _codec.emplace(level);
      _level = level;
    } else {
      _codec->Clear();
    }
    uint64_t data = 0;
    if (!spans.empty()) {
      const bool whole =
        layout == FrameDictionary::Local && spans.size() >= kLocalMinFrames;
      SDB_ASSERT(!whole || bytes.size() == p.raw);
      const auto frame = [&](size_t i) {
        return whole ? bytes.substr(spans[i].offset, spans[i].raw)
                     : Assemble(p, spans[i].first, spans[i].end);
      };
      bool trained = false;
      if (whole) {
        _sizes.clear();
        for (const auto& span : spans.All()) {
          _sizes.push_back(span.raw);
        }
        trained = _codec->Train(bytes, _sizes);
      }
      if (trained) {
        data += _codec->Stored().size();
      }
      const auto first = frame(0);
      data += CompressedSize<Codec>(first, _scratch, *_codec);
      if (!trained && spans.Shape().first != 0 && !first.empty() &&
          spans.size() > 1) {
        _codec->Use(first);
      }
      uint64_t sampled_raw = 0;
      uint64_t sampled = 0;
      spans.Sample(
        kSampleFrames, [&](size_t j) { return Jitter(begin, j); },
        [&](size_t i) {
          const auto raw = frame(i);
          sampled_raw += raw.size();
          sampled += CompressedSize<Codec>(raw, _scratch, *_codec);
        });
      data += Scale(sampled, sampled_raw, p.raw - spans[0].raw);
    }
    const auto entry_count =
      p.codes.size() + (p.shape == Shape::Dictionary ? 1 : 0);
    Header h;
    return LayOut(h, data, spans.size(), Packed(entry_count, p.max_len), 0,
                  codes_bytes, 0);
  }

 private:
  static double Jitter(uint64_t seed, uint64_t stratum) noexcept {
    uint64_t z = seed * 0x9E3779B97F4A7C15ULL + stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  std::string_view Assemble(const Profile& p, size_t first, size_t end) {
    _frame.clear();
    for (auto k = first; k < end; ++k) {
      const auto entry = EntryOf(p, k);
      _frame.append(entry.GetData(), entry.GetSize());
    }
    return _frame;
  }

  std::optional<typename Codec::Compressor> _codec;
  uint8_t _level = 0;
  std::string _frame;
  std::string _scratch;
  std::vector<size_t> _sizes;
};

inline constexpr uint64_t kLocalDictionaryPacking = 2;

template<typename Codec>
class CompressionState {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  CompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                   StringTuning& tuning)
    : _rows{type, dedup, tuning.codes},
      _history{tuning.history[static_cast<size_t>(kCodec)]} {}

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
      Sizes();
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
           static_cast<uint64_t>(
             static_cast<double>(raw - _packed) *
             Ratio(_history[static_cast<size_t>(_rows.SegmentShape())], 1.0));
  }

  RatioHistory& History() noexcept {
    return _history[static_cast<size_t>(_rows.SegmentShape())];
  }

  void Configure(uint8_t level, FrameDictionary layout) {
    _layout = layout;
    _trained = false;
    _first_frame = false;
    _spans.Reset(FrameShapeOf<Codec>(layout));
    _counted = 0;
    _packed = 0;
    if (!_codec || _level != level) {
      _codec.emplace(level);
      _level = level;
    } else {
      _codec->Clear();
    }
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

  void Sizes() {
    _sizes.clear();
    for (const auto& span : _spans.All()) {
      _sizes.push_back(span.raw);
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
    if (!_probe || _probe_level != _level) {
      _probe.emplace(_level);
      _probe_level = _level;
    }
    const auto first = Slice(0);
    _probe->Clear();
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

  SegmentRows _rows;
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
struct Fetch;

template<typename Codec>
struct Scan {
  static constexpr ByteCodec kCodec = Codec::kCodec;
  using Fetch = blob::Fetch<Codec>;

  template<typename Frames>
  Scan(const ColumnSegment&, const Frames&) {}

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    Inflate(s, f, out);
    Entries(s, f, [&](uint32_t e, uint64_t off, uint32_t len) {
      s.values[e] = duckdb::string_t{out + off, len};
    });
  }

  template<typename Frames>
  void Stream(Frames& s, uint32_t f, char* out, uint32_t* ends) {
    Inflate(s, f, out);
    Entries(s, f, [&](uint32_t, uint64_t off, uint32_t len) {
      *ends++ = static_cast<uint32_t>(off + len);
    });
  }

  template<typename Frames>
  void Inflate(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto got = inflater.Inflate(s.header, s.base, m, f, out, m.raw_len);
    SDB_ENSURE(got.size() == m.raw_len, "col codec: corrupted frame");
    if (got.data() != out) {
      std::memcpy(out, got.data(), m.raw_len);
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

  template<typename Frames, typename Entry>
  void Ensure(Frames& s, duckdb::idx_t count, Entry entry_at, uint32_t,
              uint32_t) {
    s.EnsureEach(count, entry_at);
  }

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    if constexpr (kCodec == ByteCodec::Lz4) {
      SelectFrames(s, result, result_offset, count, entry_at, window_first,
                   window_end);
    } else {
      s.EnsureEach(count, entry_at);
      s.WriteRefs(result, result_offset, count, entry_at);
    }
  }

  template<typename Frames, typename Entry>
  void SelectFrames(Frames& s, duckdb::Vector& result,
                    duckdb::idx_t result_offset, duckdb::idx_t count,
                    Entry entry_at, uint32_t window_first,
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

  uint64_t HeapBytes() const noexcept { return 0; }

  FrameInflater<Codec> inflater;
  [[no_unique_address]] utils::Need<Codec::kCodec == ByteCodec::Lz4,
                                    std::string> raw;
  [[no_unique_address]] utils::Need<Codec::kCodec == ByteCodec::Lz4,
                                    std::vector<uint32_t>> ends;
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

}  // namespace irs::codecs::text::blob
