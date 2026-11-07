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
#include "iresearch/formats/column/codecs/segment_builder.hpp"
#include "iresearch/formats/column/codecs/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"

namespace irs::codecs::text {

inline constexpr size_t kPriceFrames = 8;
inline constexpr size_t kLocalMinFrames = 8;
inline constexpr uint64_t kLocalDictionaryPacking = 2;

template<typename Codec>
class BlobEncoder {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;

  explicit BlobEncoder(StringTuning& tuning)
    : _tuning{tuning}, _history{tuning.history[static_cast<size_t>(kCodec)]} {}

  void Begin(Shape shape, uint8_t level, FrameLayout layout) {
    _shape = shape;
    _layout = Configure(level, layout);
    _local = layout == FrameLayout::Local;
    _data.clear();
    _frames.clear();
  }

  void Append(std::string_view sv, uint32_t entry) {
    if (_frame_entries != 0 && _frame.size() - _frame_begin + sv.size() >
                                 Limit(_layout, _frames.empty())) {
      CloseFrame();
    }
    if (_frame_entries == 0) {
      _frame_first = entry;
    }
    _frame.append(sv);
    ++_frame_entries;
  }

  uint64_t Estimate(const SegmentBuilder&) const noexcept {
    return (_frames.size() + 1) * kFrameMetaSize + _data.size() +
           (_local
              ? SegmentDictionaryBytes(_frame.size(), Codec::kDictionaryCap) /
                  kLocalDictionaryPacking
              : 0) +
           static_cast<uint64_t>(
             static_cast<double>(_frame.size()) *
             Ratio(_history[static_cast<size_t>(_shape)], 1.0));
  }

  EncodedFrames Finish(const SegmentBuilder& rows,
                       std::span<uint32_t> lengths) {
    if (_frame_entries != 0) {
      CloseFrame();
    }
    if (_local) {
      CompressHeld();
    }
    absl::c_transform(rows.Entries(), lengths.begin(), [](std::string_view e) {
      return static_cast<uint32_t>(e.size());
    });
    const auto flags = Flags();
    const bool local = flags == FrameDictionary::Local;
    return {
      .codec = kCodec,
      .frames = _frames,
      .max_length = rows.MaxLen(),
      .symtab = local ? _codec->Stored() : std::string_view{},
      .dictionary_size =
        local ? static_cast<uint32_t>(_codec->Dictionary().size()) : 0,
      .flags = flags,
    };
  }

  std::string& Data() noexcept { return _data; }

  uint64_t Price(const Profile& p, uint8_t level, FrameLayout frame_layout,
                 uint64_t seed) {
    const auto frame_shape = Configure(level, frame_layout);
    SplitFrames(p, frame_shape);
    uint64_t data = 0;
    if (!_spans.empty()) {
      const bool trained = frame_layout == FrameLayout::Local &&
                           _spans.size() >= kLocalMinFrames && Train(p);
      if (trained) {
        data += _codec->Stored().size();
      }
      Assemble(p, _spans[0], _price_dictionary);
      data += CompressFrame(_price_dictionary);
      if (!trained && frame_shape.dictionary != 0 &&
          !_price_dictionary.empty() && _spans.size() > 1) {
        _codec->Use(_price_dictionary);
      }
      const size_t rest = _spans.size() - 1;
      if (rest <= kPriceFrames) {
        for (size_t i = 1; i < _spans.size(); ++i) {
          Assemble(p, _spans[i], _price_frame);
          data += CompressFrame(_price_frame);
        }
      } else {
        uint64_t sampled_raw = 0;
        uint64_t sampled = 0;
        size_t count = 0;
        size_t last = 0;
        for (size_t j = 0; j < kPriceFrames; ++j) {
          const auto i =
            1 + static_cast<size_t>((static_cast<double>(j) + Jitter(seed, j)) *
                                    static_cast<double>(rest) /
                                    static_cast<double>(kPriceFrames));
          if (i == last) {
            continue;
          }
          last = i;
          Assemble(p, _spans[i], _price_frame);
          sampled += CompressFrame(_price_frame);
          sampled_raw += _spans[i].raw;
          ++count;
        }
        const auto rest_raw = p.raw - _spans[0].raw;
        data += sampled_raw == 0
                  ? sampled * rest / count
                  : static_cast<uint64_t>(static_cast<double>(sampled) *
                                          static_cast<double>(rest_raw) /
                                          static_cast<double>(sampled_raw));
      }
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
    size_t dictionary;
  };

  struct Span {
    uint64_t first;
    uint64_t end;
    uint64_t raw;
  };

  static constexpr FrameShape DictionaryShape() noexcept {
    if constexpr (kCodec == ByteCodec::Zstd) {
      return {kZstdDictionaryFrameBytes, kFrameDictionaryBytes};
    } else {
      return {kDictionaryFrameBytes, kFrameDictionaryBytes};
    }
  }

  static double Jitter(uint64_t seed, uint64_t stratum) noexcept {
    uint64_t z = seed * 0x9E3779B97F4A7C15ULL + stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  FrameShape Configure(uint8_t level, FrameLayout layout) {
    _trained = false;
    _first_frame = false;
    if (!_codec || _level != level) {
      _codec.emplace(level);
      _level = level;
    } else {
      _codec->Clear();
    }
    if (layout == FrameLayout::Wide) {
      return {kFrameRawBytes, 0};
    }
    return DictionaryShape();
  }

  FrameDictionary Flags() const noexcept {
    if (_trained) {
      return FrameDictionary::Local;
    }
    return _first_frame && _frames.size() > 1 ? FrameDictionary::FirstFrame
                                              : FrameDictionary::None;
  }

  void CloseFrame() {
    if (_local) {
      auto& hist = _history[static_cast<size_t>(_shape)];
      if (hist.raw == 0 && _frame.size() != _frame_begin) {
        const std::string_view raw{_frame.data() + _frame_begin,
                                   _frame.size() - _frame_begin};
        hist.raw += raw.size();
        hist.comp += CompressFrame(raw);
      }
      _frames.push_back(
        FrameMeta{_frame_first,
                  static_cast<uint32_t>(_frame.size() - _frame_begin), 0, 0});
      _frame_begin = _frame.size();
      _frame_entries = 0;
      return;
    }
    _frames.push_back(Compress(_frame_first, _frame));
    if (_frames.size() == 1 && _layout.dictionary != 0 && !_frame.empty()) {
      _codec->Use(_frame);
      _first_frame = true;
    }
    _frame.clear();
    _frame_entries = 0;
  }

  void CompressHeld() {
    if (_frames.size() >= kLocalMinFrames) {
      _sizes.clear();
      for (const auto& m : _frames) {
        _sizes.push_back(m.raw_len);
      }
      _trained = _codec->Train(_frame, _sizes);
    }
    CompressFrames();
    if (_trained && !Pays()) {
      _trained = false;
      _codec->Clear();
      _data.clear();
      CompressFrames();
    }
    _frame.clear();
    _frame_begin = 0;
  }

  void CompressFrames() {
    std::string_view rest = _frame;
    for (size_t i = 0; i < _frames.size(); ++i) {
      auto& m = _frames[i];
      const auto raw = rest.substr(0, m.raw_len);
      rest.remove_prefix(m.raw_len);
      m = Compress(m.first_entry, raw);
      if (i == 0 && !_trained && !raw.empty() && _frames.size() > 1) {
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
    const std::string_view first{_frame.data(), _frames[0].raw_len};
    _probe->Clear();
    const auto first_plain = Probe(first);
    _probe->Use(first);
    const size_t rest = _frames.size() - 1;
    uint64_t sampled_raw = 0;
    uint64_t sampled_trained = 0;
    uint64_t sampled_first = 0;
    size_t f = 1;
    size_t off = first.size();
    size_t last = 0;
    for (size_t j = 0; j < kPriceFrames; ++j) {
      const size_t i = 1 + j * rest / kPriceFrames;
      if (i == last) {
        continue;
      }
      last = i;
      for (; f < i; ++f) {
        off += _frames[f].raw_len;
      }
      const std::string_view raw{_frame.data() + off, _frames[i].raw_len};
      sampled_raw += raw.size();
      sampled_trained += _frames[i].comp_len;
      sampled_first += Probe(raw);
    }
    const double scale = sampled_raw == 0
                           ? 0.0
                           : static_cast<double>(_frame.size() - first.size()) /
                               static_cast<double>(sampled_raw);
    const double trained =
      static_cast<double>(_frames[0].comp_len + _codec->Stored().size()) +
      static_cast<double>(sampled_trained) * scale;
    const double first_frame = static_cast<double>(first_plain) +
                               static_cast<double>(sampled_first) * scale;
    return trained < first_frame;
  }

  uint64_t Probe(std::string_view raw) {
    const auto bound = Codec::Bound(raw.size());
    size_t n = 0;
    _price_out.resize_and_overwrite(bound, [&](char* buf, size_t) {
      n = _probe->Compress(raw.data(), raw.size(), buf, bound);
      return n;
    });
    return n;
  }

  bool Train(const Profile& p) {
    _price_samples.clear();
    _price_samples.reserve(p.raw);
    for (const auto entry : p.entries) {
      _price_samples.append(entry);
    }
    _sizes.clear();
    for (const auto& span : _spans) {
      _sizes.push_back(span.raw);
    }
    return _codec->Train(_price_samples, _sizes);
  }

  FrameMeta Compress(uint32_t first, std::string_view raw) {
    const auto comp_off = _data.size();
    const auto n = AppendCompressed(raw, _data);
    auto& hist = _history[static_cast<size_t>(_shape)];
    hist.raw += raw.size();
    hist.comp += n;
    return {first, static_cast<uint32_t>(raw.size()),
            static_cast<uint32_t>(comp_off), static_cast<uint32_t>(n)};
  }

  static size_t Limit(const FrameShape& shape, bool first) noexcept {
    return first && shape.dictionary != 0 ? shape.dictionary : shape.frame;
  }

  void SplitFrames(const Profile& p, const FrameShape& frame_shape) {
    _spans.clear();
    uint64_t first = 0;
    uint64_t raw = 0;
    for (uint64_t i = 0; i < p.entries.size(); ++i) {
      const auto limit = Limit(frame_shape, _spans.empty());
      const auto len = p.entries[i].size();
      if (i != first && raw + len > limit) {
        _spans.push_back(Span{first, i, raw});
        first = i;
        raw = 0;
      }
      raw += len;
    }
    if (first < p.entries.size()) {
      _spans.push_back(Span{first, p.entries.size(), raw});
    }
  }

  static void Assemble(const Profile& p, const Span& span, std::string& out) {
    out.clear();
    out.reserve(span.raw);
    for (auto k = span.first; k < span.end; ++k) {
      out.append(p.entries[k]);
    }
  }

  uint64_t CompressFrame(std::string_view frame) {
    _price_out.clear();
    return AppendCompressed(frame, _price_out);
  }

  size_t AppendCompressed(std::string_view frame, std::string& out) {
    const auto bound = Codec::Bound(frame.size());
    const auto at = out.size();
    size_t n = 0;
    out.resize_and_overwrite(at + bound, [&](char* buf, size_t) {
      n = _codec->Compress(frame.data(), frame.size(), buf + at, bound);
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
  FrameShape _layout{kFrameRawBytes, 0};
  bool _local = false;
  bool _trained = false;
  bool _first_frame = false;
  std::string _frame;
  size_t _frame_begin = 0;
  uint32_t _frame_first = 0;
  uint32_t _frame_entries = 0;
  std::vector<FrameMeta> _frames;
  std::string _data;
  std::vector<size_t> _sizes;
  std::vector<Span> _spans;
  std::string _price_dictionary;
  std::string _price_frame;
  std::string _price_out;
  std::string _price_samples;
};

}  // namespace irs::codecs::text
