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

#include <cstdint>
#include <duckdb/common/bitpacking.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/segment_builder.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"

namespace irs::codecs::text {

inline constexpr size_t kPriceFrames = 8;

template<typename Codec>
class BlobEncoder {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static constexpr bool kSorted = false;

  explicit BlobEncoder(StringTuning& tuning, uint32_t)
    : _tuning{tuning}, _history{tuning.history[static_cast<size_t>(kCodec)]} {}

  void Begin(Shape shape, uint8_t level, FrameLayout layout) {
    _shape = shape;
    _layout = Configure(level, layout);
    _dictionary.clear();
    _data.clear();
    _frames.clear();
  }

  void Append(std::string_view sv, uint32_t entry) {
    if (_frame_entries != 0 && _frame.size() + sv.size() > FrameLimit()) {
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
           static_cast<uint64_t>(static_cast<double>(_frame.size()) * Ratio());
  }

  EncodedFrames Finish(const SegmentBuilder& rows,
                       std::span<uint32_t> lengths) {
    if (_frame_entries != 0) {
      CloseFrame();
    }
    const auto entries = rows.Entries();
    for (size_t i = 0; i < entries.size(); ++i) {
      lengths[i] = static_cast<uint32_t>(entries[i].size());
    }
    return {
      .codec = kCodec,
      .frames = _frames,
      .max_length = rows.MaxLen(),
      .flags = Flags(),
      .dictionary = _trained ? _tuning.dictionary_id : uint16_t{0},
    };
  }

  std::string& Data() noexcept { return _data; }

  uint64_t Price(const Profile& p, uint8_t level, FrameLayout frame_layout,
                 uint64_t seed) {
    const auto frame_shape = Configure(level, frame_layout);
    SplitFrames(p, frame_shape);
    uint64_t data = 0;
    if (!_spans.empty()) {
      Assemble(p, _spans[0], _price_dictionary);
      data += CompressFrame(_price_dictionary);
      if (frame_shape.dictionary != 0 && !_price_dictionary.empty() &&
          _spans.size() > 1) {
        _codec.LoadDictionary(_price_dictionary);
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
    Header h{};
    LayOut(
      h, _spans.size(),
      duckdb::BitpackingPrimitives::GetRequiredSize(
        entry_count,
        duckdb::BitpackingPrimitives::MinimumBitWidth<uint32_t>(p.max_len)),
      0, PlanCodes(p.shape, p.rows, p.entries.size(), p.runs), 0);
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
    _codec.SetLevel(level);
    _trained = nullptr;
    if constexpr (Trainable(kCodec)) {
      if (layout == FrameLayout::Dictionary && _tuning.dictionary) {
        _trained = _tuning.dictionary.get();
        _codec.LoadTrained(*_trained);
        return {kTrainedFrameBytes, 0};
      }
    }
    if (layout == FrameLayout::Wide) {
      return {kFrameRawBytes, 0};
    }
    return DictionaryShape();
  }

  uint8_t Flags() const noexcept {
    if (_trained) {
      return kTrainedDictionary;
    }
    return !_dictionary.empty() && _frames.size() > 1 ? kFrameDictionary : 0;
  }

  double Ratio() const noexcept {
    const auto& hist = _history[static_cast<size_t>(_shape)];
    if (hist.raw != 0) {
      return static_cast<double>(hist.comp) / static_cast<double>(hist.raw);
    }
    return 1.0;
  }

  void CloseFrame() {
    const auto bound = Codec::Bound(_frame.size());
    const auto comp_off = _data.size();
    size_t n = 0;
    _data.resize_and_overwrite(comp_off + bound, [&](char* buf, size_t) {
      n = _codec.Compress(_frame.data(), _frame.size(), buf + comp_off, bound);
      return comp_off + n;
    });
    _frames.push_back(
      FrameMeta{_frame_first, static_cast<uint32_t>(_frame.size()),
                static_cast<uint32_t>(comp_off), static_cast<uint32_t>(n)});
    auto& hist = _history[static_cast<size_t>(_shape)];
    hist.raw += _frame.size();
    hist.comp += n;
    if (_frames.size() == 1 && _layout.dictionary != 0 && !_frame.empty()) {
      _dictionary.swap(_frame);
      _codec.LoadDictionary(_dictionary);
    }
    _frame.clear();
    _frame_entries = 0;
  }

  size_t FrameLimit() const noexcept {
    return _frames.empty() && _layout.dictionary != 0 ? _layout.dictionary
                                                      : _layout.frame;
  }

  void SplitFrames(const Profile& p, const FrameShape& frame_shape) {
    _spans.clear();
    uint64_t first = 0;
    uint64_t raw = 0;
    for (uint64_t i = 0; i < p.entries.size(); ++i) {
      const auto limit = _spans.empty() && frame_shape.dictionary != 0
                           ? frame_shape.dictionary
                           : frame_shape.frame;
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

  uint64_t CompressFrame(const std::string& frame) {
    const auto bound = Codec::Bound(frame.size());
    size_t n = 0;
    _price_out.resize_and_overwrite(bound, [&](char* buf, size_t) {
      n = _codec.Compress(frame.data(), frame.size(), buf, bound);
      return n;
    });
    return n;
  }

  const StringTuning& _tuning;
  RatioHistory* _history;
  typename Codec::Compressor _codec;
  const TrainedDictionary* _trained = nullptr;
  Shape _shape = Shape::Dedup;
  FrameShape _layout{kFrameRawBytes, 0};
  std::string _frame;
  std::string _dictionary;
  uint32_t _frame_first = 0;
  uint32_t _frame_entries = 0;
  std::vector<FrameMeta> _frames;
  std::string _data;
  std::vector<Span> _spans;
  std::string _price_dictionary;
  std::string _price_frame;
  std::string _price_out;
};

}  // namespace irs::codecs::text
