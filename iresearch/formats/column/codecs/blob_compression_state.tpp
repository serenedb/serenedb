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

#include "iresearch/formats/column/codecs/blob_frames.hpp"
#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {

inline constexpr uint64_t kLocalDictionaryPacking = 2;

template<typename Codec>
class BlobCompressionState {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  BlobCompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                       EstimatorState& estimators)
    : _rows{type, dedup, estimators.codes},
      _history{estimators.history[static_cast<size_t>(kCodec)]} {}

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
    if (_layout == FrameLayout::Local && _spans.size() >= kLocalMinFrames) {
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
           (_layout == FrameLayout::Local
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

  void Configure(uint8_t level, FrameLayout layout) {
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
    if (_layout != FrameLayout::Local) {
      CompressRest();
      Count();
      _frame.clear();
      return;
    }
    if (auto& hist = History(); hist.raw == 0) {
      const auto raw = Slice(_spans.size() - 1);
      hist.raw += raw.size();
      hist.comp += text::CompressedSize<Codec>(raw, _scratch, *_codec);
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
      text::CompressedSize<Codec>(first, _scratch, *_probe);
    _probe->Use(first);
    uint64_t sampled_raw = 0;
    uint64_t sampled_trained = 0;
    uint64_t sampled_first = 0;
    _spans.Sample([](size_t) { return 0.0; },
                  [&](size_t i) {
                    const auto raw = Slice(i);
                    sampled_raw += raw.size();
                    sampled_trained += _frames[i].comp_len;
                    sampled_first +=
                      text::CompressedSize<Codec>(raw, _scratch, *_probe);
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
  FrameLayout _layout = FrameLayout::FirstFrame;
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

}  // namespace irs::codecs::text
