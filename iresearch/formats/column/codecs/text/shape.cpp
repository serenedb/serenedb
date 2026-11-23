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

#include "iresearch/formats/column/codecs/text/shape.hpp"

#include <algorithm>
#include <cstdint>

#include "iresearch/formats/column/codecs/text/lz4.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/text_encoder.hpp"
#include "iresearch/formats/column/codecs/text/values.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kShapeSlack = 0.08;

bool PlainWins(bool repeats, double dictionary, double plain) noexcept {
  if (!repeats) {
    return plain <= dictionary;
  }
  return plain < dictionary * kPlainWinsBelow &&
         plain + static_cast<double>(kPlainMinSaving) <= dictionary;
}

constexpr Config Fast(Shape shape) noexcept {
  return {.shape = shape, .leaf = ByteCodec::Lz4, .level = blob::kLz4Fast};
}

bool Contested(const CodecSet& disabled, Shape shape) noexcept {
  return !disabled.test(std::to_underlying(CodecOf(shape, ByteCodec::Fsst))) ||
         !disabled.test(std::to_underlying(CodecOf(shape, ByteCodec::Lz4)));
}

Shape MeasuredShape(ColumnDataCheckpointData& data, Values& values) {
  auto& lz4 = data.Get<TextEncoder<blob::Lz4CompressionState>>();
  if (values.trial.rows == 0) {
    lz4.Trial(Fast(values.Repeats() ? Shape::Dictionary : Shape::Plain),
              values);
  }
  const bool repeats = values.TrialRepeats();
  const auto slack = [&](Shape shape) {
    return lz4.Find(Fast(shape)) ? 0.0 : kShapeSlack;
  };
  auto dictionary =
    static_cast<double>(lz4.Size(Fast(Shape::Dictionary), values));
  const auto& trial = values.trial;
  const auto least =
    static_cast<double>(kHeaderSize + Packed(trial.rows, trial.max_len) +
                        trial.input / kLz4MaxRatio);
  if (!PlainWins(repeats, dictionary * (1.0 + slack(Shape::Dictionary)),
                 least)) {
    return Shape::Dictionary;
  }
  auto plain = static_cast<double>(lz4.Size(Fast(Shape::Plain), values));
  const auto margin = std::max(slack(Shape::Dictionary), slack(Shape::Plain));
  if (PlainWins(repeats, dictionary * (1.0 - margin), plain) !=
      PlainWins(repeats, dictionary * (1.0 + margin), plain)) {
    dictionary =
      static_cast<double>(lz4.Trial(Fast(Shape::Dictionary), values).Size());
    plain = static_cast<double>(lz4.Trial(Fast(Shape::Plain), values).Size());
  }
  return PlainWins(repeats, dictionary, plain) ? Shape::Plain
                                               : Shape::Dictionary;
}

}  // namespace

Shape ContestedShape(ColumnDataCheckpointData& data) {
  auto& values = data.Get<Values>();
  if (!values.shape) {
    if (!Contested(data.disabled, Shape::Plain)) {
      values.shape = Shape::Dictionary;
    } else if (!Contested(data.disabled, Shape::Dictionary)) {
      values.shape = Shape::Plain;
    } else {
      values.shape = MeasuredShape(data, values);
    }
  }
  return *values.shape;
}

}  // namespace irs::codecs::text
