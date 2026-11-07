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

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/leaf_encoder.hpp"
#include "iresearch/formats/column/codecs/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {

inline constexpr size_t kPriceFrames = 8;
inline constexpr size_t kLocalMinFrames = 8;
inline constexpr uint64_t kLocalDictionaryPacking = 2;

template<typename Codec>
class BlobEncoder {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  explicit BlobEncoder(StringTuning& tuning)
    : _tuning{tuning}, _history{tuning.history[static_cast<size_t>(kCodec)]} {}

  void Begin(Shape shape, uint8_t level, FrameLayout layout) {
    _shape = shape;
    Configure(level, layout);
    _frame.clear();
    _data.clear();
    _frames.clear();
  }

  void Append(std::string_view sv, uint32_t entry) {
    if (Add(entry, sv.size())) {
      Closed();
    }
    _frame.append(sv);
  }

  uint64_t Estimate(const SegmentEntries& rows) const noexcept {
    const auto raw = rows.raw;
    return (_spans.size() + 1) * kFrameMetaSize + _data.size() +
           (_layout == FrameLayout::Local
              ? SegmentDictionaryBytes(raw, Codec::kDictionaryCap) /
                  kLocalDictionaryPacking
              : 0) +
           static_cast<uint64_t>(
             static_cast<double>(raw - _packed) *
             Ratio(_history[static_cast<size_t>(_shape)], 1.0));
  }

  EncodedFrames Finish(const SegmentEntries& rows,
                       std::span<uint32_t> lengths) {
    SDB_ASSERT(_packed + _frame.size() == rows.raw);
    Close();
    if (_layout == FrameLayout::Local && _spans.size() >= kLocalMinFrames) {
      Sizes();
      _trained = _codec->Train(_frame, _sizes);
    }
    CompressRest();
    if (_trained && !Pays()) {
      _trained = false;
      _codec->Clear();
      _data.clear();
      _frames.clear();
      CompressRest();
    }
    Count();
    absl::c_transform(rows.entries, lengths.begin(), [](std::string_view e) {
      return static_cast<uint32_t>(e.size());
    });
    const auto flags = Flags();
    const bool local = flags == FrameDictionary::Local;
    return {
      .codec = kCodec,
      .frames = _frames,
      .max_length = rows.max_len,
      .symtab = local ? _codec->Stored() : std::string_view{},
      .dictionary_size =
        local ? static_cast<uint32_t>(_codec->Dictionary().size()) : 0,
      .flags = flags,
    };
  }

  std::string& Data() noexcept { return _data; }

  uint64_t Price(const Profile& p, uint8_t level, FrameLayout layout,
                 uint64_t seed) {
    Configure(level, layout);
    for (uint64_t i = 0; i < p.entries.size(); ++i) {
      Add(i, p.entries[i].size());
    }
    Close();
    uint64_t data = 0;
    if (!_spans.empty()) {
      const bool whole =
        _layout == FrameLayout::Local && _spans.size() >= kLocalMinFrames;
      if (whole) {
        _frame.clear();
        for (const auto entry : p.entries) {
          _frame.append(entry);
        }
        Sizes();
        _trained = _codec->Train(_frame, _sizes);
      }
      const auto frame = [&](size_t i) {
        return whole ? Slice(i) : Assemble(p.entries, _spans[i]);
      };
      if (_trained) {
        data += _codec->Stored().size();
      }
      const auto first = frame(0);
      data += CompressedSize(first);
      if (!_trained && _frame_shape.first != 0 && !first.empty() &&
          _spans.size() > 1) {
        _codec->Use(first);
      }
      uint64_t sampled_raw = 0;
      uint64_t sampled = 0;
      Sample([&](size_t j) { return Jitter(seed, j); },
             [&](size_t i) {
               const auto raw = frame(i);
               sampled_raw += raw.size();
               sampled += CompressedSize(raw);
             });
      data += Scale(sampled, sampled_raw, p.raw - _spans[0].raw);
      auto& hist = _history[static_cast<size_t>(p.shape)];
      hist.raw += p.raw;
      hist.comp += data;
    }
    const auto entry_count =
      p.entries.size() + (p.shape == Shape::Dedup ? 1 : 0);
    Header h;
    LayOut(h, _spans.size(), Packed(entry_count, p.max_len), 0,
           CodesBytes(p.shape, p.rows, p.entries.size(), p.runs, _tuning.codes),
           0);
    return h.off_data + data;
  }

 private:
  struct FrameShape {
    size_t frame;
    size_t first;
  };

  struct Span {
    uint64_t first = 0;
    uint64_t end = 0;
    uint64_t offset = 0;
    uint64_t raw = 0;
  };

  RatioHistory& History() noexcept {
    return _history[static_cast<size_t>(_shape)];
  }

  void Configure(uint8_t level, FrameLayout layout) {
    _layout = layout;
    _trained = false;
    _first_frame = false;
    _spans.clear();
    _open = {};
    _counted = 0;
    _packed = 0;
    if (!_codec || _level != level) {
      _codec.emplace(level);
      _level = level;
    } else {
      _codec->Clear();
    }
    if (layout == FrameLayout::Wide) {
      _frame_shape = {kFrameRawBytes, 0};
    } else if constexpr (kCodec == ByteCodec::Zstd) {
      _frame_shape = {kZstdDictionaryFrameBytes, kFrameDictionaryBytes};
    } else {
      _frame_shape = {kDictionaryFrameBytes, kFrameDictionaryBytes};
    }
  }

  FrameDictionary Flags() const noexcept {
    if (_trained) {
      return FrameDictionary::Local;
    }
    return _first_frame && _frames.size() > 1 ? FrameDictionary::FirstFrame
                                              : FrameDictionary::None;
  }

  bool Add(uint64_t entry, uint64_t size) {
    const auto limit = _spans.empty() && _frame_shape.first != 0
                         ? _frame_shape.first
                         : _frame_shape.frame;
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
    if (_open.end != _open.first) {
      _spans.push_back(_open);
    }
    _open = {};
  }

  void Closed() {
    if (_layout != FrameLayout::Local) {
      CompressRest();
      Count();
      _frame.clear();
      return;
    }
    if (auto& hist = History(); hist.raw == 0) {
      const auto raw = Slice(_spans.size() - 1);
      hist.raw += raw.size();
      hist.comp += CompressedSize(raw);
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
    for (const auto& span : _spans) {
      _sizes.push_back(span.raw);
    }
  }

  std::string_view Slice(size_t i) const noexcept {
    return std::string_view{_frame}.substr(_spans[i].offset - _packed,
                                           _spans[i].raw);
  }

  std::string_view Assemble(std::span<const std::string_view> entries,
                            const Span& span) {
    _frame.clear();
    for (auto k = span.first; k < span.end; ++k) {
      _frame.append(entries[k]);
    }
    return _frame;
  }

  void CompressRest() {
    for (auto i = _frames.size(); i < _spans.size(); ++i) {
      const auto raw = Slice(i);
      const auto at = _data.size();
      const auto n = Compress(raw, _data);
      _frames.push_back({static_cast<uint32_t>(_spans[i].first),
                         static_cast<uint32_t>(raw.size()),
                         static_cast<uint32_t>(at), static_cast<uint32_t>(n)});
      if (i == 0 && !_trained && _frame_shape.first != 0 && !raw.empty() &&
          (_spans.size() > 1 || _open.end != _open.first)) {
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
    const auto first_plain = CompressedSize(first, *_probe);
    _probe->Use(first);
    uint64_t sampled_raw = 0;
    uint64_t sampled_trained = 0;
    uint64_t sampled_first = 0;
    Sample([](size_t) { return 0.0; },
           [&](size_t i) {
             const auto raw = Slice(i);
             sampled_raw += raw.size();
             sampled_trained += _frames[i].comp_len;
             sampled_first += CompressedSize(raw, *_probe);
           });
    const auto rest_raw = _frame.size() - first.size();
    return _frames[0].comp_len + _codec->Stored().size() +
             Scale(sampled_trained, sampled_raw, rest_raw) <
           first_plain + Scale(sampled_first, sampled_raw, rest_raw);
  }

  template<typename Offset, typename Visit>
  void Sample(Offset&& offset, Visit&& visit) const {
    const size_t rest = _spans.size() - 1;
    if (rest <= kPriceFrames) {
      for (size_t i = 1; i < _spans.size(); ++i) {
        visit(i);
      }
      return;
    }
    size_t last = 0;
    for (size_t j = 0; j < kPriceFrames; ++j) {
      const auto i =
        1 + static_cast<size_t>((static_cast<double>(j) + offset(j)) *
                                static_cast<double>(rest) /
                                static_cast<double>(kPriceFrames));
      if (i != last) {
        last = i;
        visit(i);
      }
    }
  }

  static double Jitter(uint64_t seed, uint64_t stratum) noexcept {
    uint64_t z = seed * 0x9E3779B97F4A7C15ULL + stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  static uint64_t Scale(uint64_t sampled, uint64_t sampled_raw,
                        uint64_t raw) noexcept {
    return sampled_raw == 0
             ? 0
             : static_cast<uint64_t>(static_cast<double>(sampled) *
                                     static_cast<double>(raw) /
                                     static_cast<double>(sampled_raw));
  }

  uint64_t CompressedSize(std::string_view raw) {
    return CompressedSize(raw, *_codec);
  }

  uint64_t CompressedSize(std::string_view raw,
                          typename Codec::Compressor& codec) {
    _out.clear();
    return Compress(raw, _out, codec);
  }

  size_t Compress(std::string_view raw, std::string& out) {
    return Compress(raw, out, *_codec);
  }

  static size_t Compress(std::string_view raw, std::string& out,
                         typename Codec::Compressor& codec) {
    const auto bound = Codec::Bound(raw.size());
    const auto at = out.size();
    size_t n = 0;
    out.resize_and_overwrite(at + bound, [&](char* buf, size_t) {
      n = codec.Compress(raw.data(), raw.size(), buf + at, bound);
      return at + n;
    });
    return n;
  }

  const StringTuning& _tuning;
  RatioHistory* _history;
  std::optional<typename Codec::Compressor> _codec;
  std::optional<typename Codec::Compressor> _probe;
  uint8_t _level = 0;
  uint8_t _probe_level = 0;
  Shape _shape = Shape::Dedup;
  FrameLayout _layout = FrameLayout::FirstFrame;
  FrameShape _frame_shape{kFrameRawBytes, 0};
  bool _trained = false;
  bool _first_frame = false;
  size_t _counted = 0;
  uint64_t _packed = 0;
  std::string _frame;
  std::string _data;
  std::string _out;
  Span _open;
  std::vector<Span> _spans;
  std::vector<size_t> _sizes;
  std::vector<FrameMeta> _frames;
};

}  // namespace irs::codecs::text
