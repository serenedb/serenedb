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

#include "iresearch/formats/column/codecs/text/lz4.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/text_function.hpp"

/*
lz4 frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary. An entry longer than a frame holds is a
frame of its own.
*/

namespace irs::codecs {
namespace text::blob {

Lz4Tuner::ShapeRows& Lz4Tuner::TrialRows(Shape shape) {
  auto& rows = _rows[std::to_underlying(shape)];
  if (rows.done) {
    return rows;
  }
  rows.done = true;
  auto& p = rows.profile;
  p.shape = shape;
  const auto window = _contest.trial.rows;
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

Lz4Tuner::ShapeRows& Lz4Tuner::SortedRows() {
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

uint64_t Lz4Tuner::CodesSize(ShapeRows& rows) {
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

std::string_view Lz4Tuner::Bytes(ShapeRows& rows) {
  if (!rows.assembled) {
    rows.assembled = true;
    const auto& p = rows.profile;
    rows.bytes.reserve(p.raw);
    for (size_t i = 0; i < p.codes.size(); ++i) {
      const auto entry = EntryOf(p, i);
      rows.bytes.append(entry.GetData(), entry.GetSize());
    }
  }
  return rows.bytes;
}

const FrameSpans& Lz4Tuner::Split(ShapeRows& rows, FrameShape frame) {
  const auto it = absl::c_find_if(rows.spans, [&](const Spans& s) {
    return s.frame.frame == frame.frame && s.frame.first == frame.first;
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

uint64_t Lz4Tuner::Estimate(Shape shape, const Config& config) {
  auto& rows = config.sorted ? SortedRows() : TrialRows(shape);
  const auto whole =
    config.layout == FrameDictionary::Local ? Bytes(rows) : std::string_view{};
  return _estimator[std::to_underlying(shape)].EstimateSize(
    rows.profile, whole, Split(rows, FrameShapeOf<Lz4>(config.layout)),
    CodesSize(rows), config.level, config.layout);
}

}  // namespace text::blob
namespace text {
namespace {

constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kShapeSlack = 0.08;

constexpr Config Fast(Shape shape) noexcept {
  return {.shape = shape, .leaf = ByteCodec::Lz4, .level = blob::kLz4Fast};
}

bool Contested(const CodecSet& disabled, Shape shape) noexcept {
  return !disabled.test(std::to_underlying(CodecOf(shape, ByteCodec::Fsst))) ||
         !disabled.test(std::to_underlying(CodecOf(shape, ByteCodec::Lz4)));
}

template<typename Rows>
auto& Lz4Of(ColumnDataCheckpointData& data) {
  return data.Get<CodecState<blob::CompressionState<Lz4, Rows>>>();
}

template<typename Rows>
double FastSize(ColumnDataCheckpointData& data) {
  auto config = Fast(Rows::kShape);
  config.sorted =
    Rows::kShape == Shape::Dictionary &&
    data.tuning.strings.order[std::to_underlying(ByteCodec::Lz4)].sorted;
  return static_cast<double>(
    data.Get<blob::Lz4Tuner>().Estimated(Lz4Of<Rows>(data), config));
}

void MeasureShapes(ColumnDataCheckpointData& data, const Values& values,
                   Contest& contest) {
  auto& dictionary = contest.contends[std::to_underlying(Shape::Dictionary)];
  auto& plain = contest.contends[std::to_underlying(Shape::Plain)];
  dictionary = Contested(data.disabled, Shape::Dictionary);
  plain = Contested(data.disabled, Shape::Plain);
  if (!dictionary || !plain || !values.Repeats()) {
    return;
  }
  if (contest.trial.rows == 0) {
    Trial(Lz4Of<DictionaryRows>(data), Fast(Shape::Dictionary), values, contest,
          data.params.segment_target);
  }
  const auto& trial = contest.trial;
  const auto least = ShapeCost(
    Shape::Plain, true,
    static_cast<double>(kHeaderSize + Packed(trial.rows, trial.max_len) +
                        trial.input / kLz4MaxRatio));
  const auto out_of_reach = [&](double dictionary_cost) {
    return least >= dictionary_cost * (1.0 + kShapeSlack);
  };
  const auto built = contest.built[std::to_underlying(Shape::Dictionary)];
  if (built != 0 && out_of_reach(static_cast<double>(built))) {
    plain = false;
    return;
  }
  const auto dictionary_cost = FastSize<DictionaryRows>(data);
  if (out_of_reach(dictionary_cost)) {
    plain = false;
    return;
  }
  const auto plain_cost =
    ShapeCost(Shape::Plain, true, FastSize<PlainRows>(data));
  plain = plain_cost < dictionary_cost * (1.0 + kShapeSlack);
  dictionary = dictionary_cost * (1.0 - kShapeSlack) <= plain_cost;
}

}  // namespace

bool Contends(ColumnDataCheckpointData& data, Shape shape) {
  auto& contest = data.Get<Contest>();
  if (!contest.shapes_measured) {
    contest.shapes_measured = true;
    MeasureShapes(data, data.Get<Values>(), contest);
  }
  return contest.contends[std::to_underlying(shape)];
}

}  // namespace text
namespace {

constexpr double kReadPrice = 1.05;

template<typename Rows>
using Lz4Builder = text::blob::CompressionState<Lz4, Rows>;

template<typename Rows>
double AnalyzeLz4(ColumnDataCheckpointData& data, double bound) {
  return text::AnalyzeHinted<Lz4Builder<Rows>>(
    data, bound,
    [&](text::CodecState<Lz4Builder<Rows>>& state, const text::Values& values,
        bool moved) {
      if (!values.Repeats() || !text::Contests(state, data, moved)) {
        return kNoScore;
      }
      const auto& contest = data.Get<text::Contest>();
      const auto price = data.params.tier == WriteTier::Merge &&
                             Rows::kShape == Shape::Dictionary
                           ? 1.0
                           : kReadPrice;
      const auto priced = [&](uint64_t bytes) {
        return text::Score(values, contest, Rows::kShape, price, bytes);
      };
      const auto measured =
        data.Get<text::blob::Lz4Tuner>().Measure(state, bound, priced);
      state.config = measured.config;
      return priced(measured.bytes);
    });
}

template<typename Rows>
constexpr CompressionFunction kFunction{
  .analyze = AnalyzeLz4<Rows>,
  .compress = text::blob::CompressFrames<Lz4Builder<Rows>>,
};

}  // namespace

constinit const Codec kLz4Codec =
  text::StringCodec<text::PlainScan<text::blob::Lz4Scan>,
                    Lz4Builder<text::PlainRows>, kFunction<text::PlainRows>>();

constinit const Codec kDictLz4Codec =
  text::StringCodec<text::DictionaryScan<text::blob::Lz4Scan>,
                    Lz4Builder<text::DictionaryRows>,
                    kFunction<text::DictionaryRows>>();

}  // namespace irs::codecs
