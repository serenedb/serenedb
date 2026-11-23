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
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/frame_spans.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/text_function.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
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
    _sorted = config.sorted;
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
    if constexpr (kShape == Shape::Dictionary) {
      _rows.AddCode(code, values, [](const duckdb::string_t&, uint32_t) {});
    } else {
      _rows.AddCode(
        code, values,
        [&](const duckdb::string_t& entry, uint32_t id) { Append(entry, id); });
    }
  }

  IRS_FORCE_INLINE bool Full(uint32_t target) {
    return _rows.EstimateDue(target) && _rows.Estimate() + Estimate() >= target;
  }

  void FinalizeCompress() {
    if constexpr (kShape == Shape::Dictionary) {
      AppendEntries();
    }
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

  void AppendEntries() {
    if (_sorted) {
      _rows.Sort(_sort);
    }
    const auto rows = _rows.View();
    for (size_t i = 0; i < rows.entries.size(); ++i) {
      Append(rows.entries[i], rows.first_entry + static_cast<uint32_t>(i));
    }
  }

  void Seed(const SegmentEntries& rows) {
    auto& hist = History();
    const auto frame = _spans.Shape().frame;
    if (hist.raw != 0 || rows.raw < frame) {
      return;
    }
    _frame.clear();
    for (size_t i = 0; i < rows.entries.size() && _frame.size() < frame; ++i) {
      _frame.append(rows.entries[i].GetData(), rows.entries[i].GetSize());
    }
    AddSample(hist, _frame);
    _frame.clear();
  }

  void AddSample(RatioHistory& hist, std::string_view raw) {
    hist.raw += raw.size();
    hist.comp += blob::CompressedSize<Codec>(raw, _scratch, *_codec);
  }

  uint64_t Estimate() {
    const auto rows = _rows.View();
    const auto dictionary =
      _layout == FrameDictionary::Local
        ? SegmentDictionaryBytes(rows.raw, Codec::kDictionaryCap) /
            kLocalDictionaryPacking
        : 0;
    if constexpr (kShape == Shape::Dictionary) {
      Seed(rows);
      return (rows.raw / _spans.Shape().frame + 2) * kFrameMetaSize +
             dictionary +
             static_cast<uint64_t>(static_cast<double>(rows.raw) *
                                   Ratio(History(), 1.0));
    } else {
      return (_spans.size() + 1) * kFrameMetaSize + Written() + dictionary +
             static_cast<uint64_t>(static_cast<double>(rows.raw - _packed) *
                                   Ratio(History(), 1.0));
    }
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
    SDB_ASSERT(!_first_frame || _frames.size() > 1);
    return _first_frame ? FrameDictionary::FirstFrame : FrameDictionary::None;
  }

  void Closed() {
    if (_layout != FrameDictionary::Local) {
      CompressRest();
      Count();
      _frame.clear();
      return;
    }
    if (auto& hist = History(); hist.raw == 0) {
      AddSample(hist, Slice(_spans.size() - 1));
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
  SortScratch _sort;
  RatioHistory* _history;
  std::optional<typename Codec::Compressor> _codec;
  std::optional<typename Codec::Compressor> _probe;
  uint8_t _level = 0;
  uint8_t _probe_level = 0;
  FrameDictionary _layout = FrameDictionary::FirstFrame;
  bool _sorted = false;
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

inline constexpr size_t kSampleFrames = 8;
inline constexpr uint64_t kTrialRawBytes = 512 * 1024;
inline constexpr double kEstimateSlack = 0.02;
inline constexpr double kSortedGain = 0.06;

inline void ConcatEntries(const Profile& p, size_t first, size_t end,
                          std::string& out) {
  for (auto k = first; k < end; ++k) {
    const auto entry = EntryOf(p, k);
    out.append(entry.GetData(), entry.GetSize());
  }
}

template<typename Codec>
class SizeEstimator {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  uint64_t FramesSize(const Profile& p, std::string_view bytes,
                      const FrameSpans& spans, uint8_t level,
                      FrameDictionary layout) {
    ResetCompressor(_codec, _level, level);
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
        spans.Sizes(_sizes);
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
        kSampleFrames, [](size_t j) { return Jitter(j); },
        [&](size_t i) {
          const auto raw = frame(i);
          sampled_raw += raw.size();
          sampled += CompressedSize<Codec>(raw, _scratch, *_codec);
        });
      data += Scale(sampled, sampled_raw, p.raw - spans[0].raw);
    }
    return data;
  }

  static uint64_t SegmentSize(const Profile& p, uint64_t frames_bytes,
                              size_t frames, uint64_t codes_bytes) {
    const auto entry_count =
      p.codes.size() + (p.shape == Shape::Dictionary ? 1 : 0);
    Header h;
    return LayOut(h, frames_bytes, frames, Packed(entry_count, p.max_len), 0,
                  codes_bytes, 0);
  }

 private:
  static double Jitter(uint64_t stratum) noexcept {
    uint64_t z = stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  std::string_view Assemble(const Profile& p, size_t first, size_t end) {
    _frame.clear();
    ConcatEntries(p, first, end, _frame);
    return _frame;
  }

  std::optional<typename Codec::Compressor> _codec;
  uint8_t _level = 0;
  std::string _frame;
  std::string _scratch;
  std::vector<size_t> _sizes;
};

consteval StateId SizesState(ByteCodec codec) noexcept {
  switch (codec) {
    case ByteCodec::Zstd:
      return StateId::TextZstdSizes;
    case ByteCodec::Zxc:
      return StateId::TextZxcSizes;
    default:
      return StateId::TextLz4Sizes;
  }
}

template<typename Codec>
class TrialSizes final : public State {
 public:
  static constexpr StateId kId = SizesState(Codec::kCodec);

  template<typename Rows>
  using Candidate = CodecState<CompressionState<Codec, Rows>>;

  explicit TrialSizes(ColumnDataCheckpointData& data)
    : _values{data.Get<Values>()},
      _window{data.Get<Window>()},
      _params{data.params} {}

  template<typename Rows>
  uint64_t Size(Candidate<Rows>& state, const Config& config) {
    constexpr auto kShape = Rows::kShape;
    if (const auto* trial = Find(state, config)) {
      return trial->Size();
    }
    if (Codec::HighCompression(config.level)) {
      return Estimate(kShape, config);
    }
    SDB_ASSERT(config.shape == kShape);
    const auto raw =
      kShape == Shape::Dictionary
        ? std::min<uint64_t>(_window.input, TrialEntries() * _window.max_len)
        : _window.input;
    if (raw <= kTrialRawBytes) {
      return Trial(state, config, _values, _window, _params.segment_target)
        .Size();
    }
    return Estimate(kShape, config);
  }

  uint64_t Estimate(Shape shape, const Config& config) {
    auto& rows = TrialRows(shape);
    const auto count = Split(rows, FrameShapeOf<Codec>(config.layout)).size();
    if (!config.sorted) {
      return SizeEstimator<Codec>::SegmentSize(
        rows.profile, FramesSize(rows, config), count, CodesSize(rows));
    }
    auto& sorted = SortedRows();
    auto usual = config;
    usual.layout = NamedConfig(shape, Codec::kCodec, _params).layout;
    auto frames = FramesSize(rows, usual);
    if (rows.profile.raw > kFrameDictionaryBytes) {
      frames -=
        static_cast<uint64_t>(static_cast<double>(frames) * kSortedGain);
    }
    if (config.layout != usual.layout) {
      frames =
        Scale(FramesSize(sorted, config), FramesSize(sorted, usual), frames);
    }
    return SizeEstimator<Codec>::SegmentSize(rows.profile, frames, count,
                                             CodesSize(sorted));
  }

 private:
  struct Spans {
    FrameShape frame;
    FrameSpans spans;
  };

  struct FramesBytes {
    uint8_t level;
    FrameDictionary layout;
    uint64_t bytes;
  };

  struct ShapeRows {
    bool done = false;
    bool assembled = false;
    bool coded = false;
    Profile profile;
    std::vector<uint32_t> codes;
    ScratchVector<uint32_t> locals;
    uint64_t codes_bytes = 0;
    std::string bytes;
    std::vector<Spans> spans;
    std::vector<FramesBytes> frames;
  };

  ShapeRows& TrialRows(Shape shape) {
    SDB_ASSERT(_window.rows != 0);
    auto& rows = _rows[std::to_underlying(shape)];
    if (rows.done) {
      return rows;
    }
    rows.done = true;
    auto& p = rows.profile;
    p.shape = shape;
    const auto window = _window.rows;
    p.rows = window;
    const auto& distinct = _values.distinct;
    p.values = distinct;
    const auto add = [&](uint32_t code) {
      const auto size = static_cast<uint32_t>(distinct[code - 1]->GetSize());
      p.raw += size;
      p.max_len = std::max(p.max_len, size);
    };
    if (shape == Shape::Plain) {
      p.codes = std::span{_values.codes}.subspan(0, window);
      for (const auto code : p.codes) {
        if (code != 0) {
          add(code);
        }
      }
      return rows;
    }
    _dedup.Begin(distinct.size());
    rows.locals.resize(window);
    for (uint64_t row = 0; row < window; ++row) {
      const auto code = _values.codes[row];
      uint32_t local = 0;
      if (code != 0) {
        const auto mapped =
          _dedup.Local(code, static_cast<uint32_t>(rows.codes.size()) + 1);
        local = mapped.local;
        if (mapped.fresh) {
          rows.codes.push_back(code);
          add(code);
        }
      }
      rows.locals[row] = local;
    }
    p.codes = rows.codes;
    return rows;
  }

  ShapeRows& SortedRows() {
    auto& rows = _sorted;
    if (rows.done) {
      return rows;
    }
    rows.done = true;
    const auto& arrival = TrialRows(Shape::Dictionary);
    std::vector<duckdb::string_t> entries;
    entries.reserve(arrival.codes.size());
    for (const auto code : arrival.codes) {
      entries.emplace_back(*_values.distinct[code - 1]);
    }
    SortScratch scratch;
    SortEntries(entries, scratch);
    rows.codes.resize(arrival.codes.size());
    for (size_t at = 0; at < rows.codes.size(); ++at) {
      rows.codes[at] = arrival.codes[scratch.order[at].entry];
    }
    rows.locals.resize(arrival.locals.size());
    for (size_t row = 0; row < rows.locals.size(); ++row) {
      rows.locals[row] = scratch.remap[arrival.locals[row]];
    }
    rows.profile = arrival.profile;
    rows.profile.codes = rows.codes;
    return rows;
  }

  uint64_t TrialEntries() { return TrialRows(Shape::Dictionary).codes.size(); }

  uint64_t CodesSize(ShapeRows& rows) {
    if (rows.profile.shape != Shape::Dictionary) {
      return 0;
    }
    if (!rows.coded) {
      rows.coded = true;
      double ratio = 0;
      CompressCodes(rows.locals, ratio, [&](CodecType, std::string_view bytes) {
        rows.codes_bytes = bytes.size();
      });
    }
    return rows.codes_bytes;
  }

  static std::string_view Bytes(ShapeRows& rows) {
    if (!rows.assembled) {
      rows.assembled = true;
      const auto& p = rows.profile;
      rows.bytes.reserve(p.raw);
      ConcatEntries(p, 0, p.codes.size(), rows.bytes);
    }
    return rows.bytes;
  }

  static const FrameSpans& Split(ShapeRows& rows, FrameShape frame) {
    static_assert(kFrameRawBytes != kDictionaryFrameBytes &&
                  kFrameRawBytes != kZstdDictionaryFrameBytes);
    const auto it = absl::c_find_if(rows.spans, [&](const Spans& s) {
      SDB_ASSERT(s.frame.frame != frame.frame || s.frame.first == frame.first);
      return s.frame.frame == frame.frame;
    });
    if (it != rows.spans.end()) {
      return it->spans;
    }
    auto& split = rows.spans.emplace_back(frame).spans;
    split.Reset(frame);
    for (size_t i = 0; i < rows.profile.codes.size(); ++i) {
      split.Add(i, EntryOf(rows.profile, i).GetSize());
    }
    split.Close();
    return split;
  }

  uint64_t FramesSize(ShapeRows& rows, const Config& config) {
    const auto it = absl::c_find_if(rows.frames, [&](const FramesBytes& f) {
      return f.level == config.level && f.layout == config.layout;
    });
    if (it != rows.frames.end()) {
      return it->bytes;
    }
    const auto whole = config.layout == FrameDictionary::Local
                         ? Bytes(rows)
                         : std::string_view{};
    const auto bytes =
      _estimator[std::to_underlying(rows.profile.shape)].FramesSize(
        rows.profile, whole, Split(rows, FrameShapeOf<Codec>(config.layout)),
        config.level, config.layout);
    rows.frames.emplace_back(config.level, config.layout, bytes);
    return bytes;
  }

  const Values& _values;
  Window& _window;
  const ColCodecParams& _params;
  DedupScratch _dedup;
  ShapeRows _rows[kTextShapeCount];
  ShapeRows _sorted;
  SizeEstimator<Codec> _estimator[kTextShapeCount];
};

template<typename Codec, typename Rows>
Config FrameConfig(const ColumnDataCheckpointData& data) noexcept {
  auto config = NamedConfig(Rows::kShape, Codec::kCodec, data.params);
  if constexpr (Rows::kShape == Shape::Dictionary) {
    config.sorted = (data.option & kSorted) != 0;
  }
  if ((data.option & kWide) != 0) {
    config.layout = FrameDictionary::None;
  }
  return config;
}

template<typename Rows>
bool Declines(ColumnDataCheckpointData& data) {
  const auto& values = data.Get<Values>();
  return data.contested &&
         (!values.Repeats() || !Contends(values, data.disabled, Rows::kShape));
}

template<typename Codec, typename Rows>
double EstimateFrames(ColumnDataCheckpointData& data) {
  constexpr auto kShape = Rows::kShape;
  if (Declines<Rows>(data)) {
    return kNoScore;
  }
  const auto& values = data.Get<Values>();
  auto& window = data.Get<Window>();
  const auto config = FrameConfig<Codec, Rows>(data);
  if (window.rows == 0) {
    Trial(data.Get<CodecState<CompressionState<Codec, Rows>>>(), config, values,
          window, data.params.segment_target);
  }
  return Score(values, window, kShape, data.price,
               data.Get<TrialSizes<Codec>>().Estimate(kShape, config));
}

template<typename Codec, typename Rows>
double AnalyzeFrames(ColumnDataCheckpointData& data, double bound) {
  constexpr auto kShape = Rows::kShape;
  if (Declines<Rows>(data)) {
    return kNoScore;
  }
  const auto& values = data.Get<Values>();
  auto& state = data.Get<CodecState<CompressionState<Codec, Rows>>>();
  auto& window = data.Get<Window>();
  const auto config = FrameConfig<Codec, Rows>(data);
  const auto target = data.params.segment_target;
  if (window.rows == 0) {
    Trial(state, config, values, window, target);
  }
  const auto priced = [&](uint64_t bytes) {
    return Score(values, window, kShape, data.price, bytes);
  };
  if (!data.contested) {
    return priced(Trial(state, config, values, window, target).Size());
  }
  auto bytes = data.Get<TrialSizes<Codec>>().Size(state, config);
  if (!Find(state, config)) {
    const auto estimate = priced(bytes);
    if (Codec::HighCompression(config.level)
          ? estimate < bound
          : estimate <= bound * (1.0 + kEstimateSlack)) {
      bytes = Trial(state, config, values, window, target).Size();
    }
  }
  return priced(bytes);
}

template<typename Codec, typename Rows>
bool CompressFrames(const codecs::Codec& codec,
                    ColumnDataCheckpointData& data) {
  using Compression = CompressionState<Codec, Rows>;
  const auto config = FrameConfig<Codec, Rows>(data);
  if (data.contested) {
    return CompressContested<Compression>(config, codec, data);
  }
  CompressAs<Compression>(config, codec, data);
  return true;
}

template<typename Codec, typename Rows>
inline constexpr CompressionFunction kFrameFunction{
  .analyze = AnalyzeFrames<Codec, Rows>,
  .compress = CompressFrames<Codec, Rows>,
  .estimate = EstimateFrames<Codec, Rows>,
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
      if (!_decoder->Load(
            {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size},
            h.dictionary_size)) [[unlikely]] {
        ThrowCorrupted("text", "dictionary is inconsistent");
      }
      _loaded += h.dictionary_size;
    } else if (h.flags == FrameDictionary::FirstFrame) {
      const auto f0 = duckdb::Load<FrameMeta>(base + h.off_frames);
      if (static_cast<uint64_t>(f0.comp_off) + f0.comp_len > h.data_size)
        [[unlikely]] {
        ThrowCorrupted("text", "frame table is inconsistent");
      }
      if (!_decoder->LoadFrame(data + f0.comp_off, f0.comp_len, f0.raw_len))
        [[unlikely]] {
        ThrowCorrupted("text", "frame is inconsistent");
      }
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
    if (offset > f.raw_len) [[unlikely]] {
      ThrowCorrupted("text", "entry lengths are inconsistent");
    }
    known = need;
  }

  void Inflate(const Header& h, duckdb::const_data_ptr_t base, uint32_t lo,
               const FrameMeta& f, size_t want, bool batched) {
    if (!batched && (hits > 1 || dense)) {
      want = f.raw_len;
    }
    const auto got = inflater.Inflate(h, base, f, lo, raw.data(), want);
    if (got.size() < want) [[unlikely]] {
      ThrowCorrupted("text", "frame is inconsistent");
    }
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

inline constexpr uint64_t kInflateByteCost = 6;
inline constexpr uint64_t kFrameCopyByteCost = 2;
inline constexpr uint64_t kFrameEntryCost = 72;
inline constexpr uint64_t kPrefixEntryCost = 16;
inline constexpr uint64_t kHitByteCost = 2;
inline constexpr uint64_t kHitCost = 128;

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
    if (got.size() != m.raw_len) [[unlikely]] {
      ThrowCorrupted("text", "frame is inconsistent");
    }
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
      if (off + len > m.raw_len) [[unlikely]] {
        ThrowCorrupted("text", "entry lengths are inconsistent");
      }
      visit(e, off, len);
      off += len;
    }
    if (off != m.raw_len) [[unlikely]] {
      ThrowCorrupted("text", "entry lengths are inconsistent");
    }
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
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count,
                                                               result_offset);
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    bool referenced = false;
    for (duckdb::idx_t i = 0; i < count;) {
      auto e = static_cast<uint32_t>(entry_at(i));
      const auto f = s.FrameOf(e);
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        SDB_ASSERT(e < next);
        if (next >= end) {
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
        const uint64_t inflated = Codec::kInflatesPrefix ? prefix : rows;
        const bool done =
          end <= window_end ||
          count * uint64_t{end - window_end} < window_end - window_first;
        if (done &&
            inflated * entry_bytes * kInflateByteCost +
                prefix * kPrefixEntryCost +
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
    if (end > m.raw_len) [[unlikely]] {
      ThrowCorrupted("text", "entry lengths are inconsistent");
    }
    if (raw.size() < m.raw_len) {
      raw.resize_and_overwrite(m.raw_len, [](char*, size_t n) { return n; });
    }
    const auto got = inflater.Inflate(s.header, s.base, m, f, raw.data(), end);
    if (got.size() < end) [[unlikely]] {
      ThrowCorrupted("text", "frame is inconsistent");
    }
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
  std::string raw;
  std::vector<uint32_t> ends;
};

}  // namespace irs::codecs::text::blob
