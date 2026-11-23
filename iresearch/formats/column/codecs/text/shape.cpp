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
  return data.Get<TextEncoder<blob::Lz4CompressionState<Rows>>>();
}

void MeasureShapes(ColumnDataCheckpointData& data, Values& values) {
  auto& dictionary = values.contends[std::to_underlying(Shape::Dictionary)];
  auto& plain = values.contends[std::to_underlying(Shape::Plain)];
  dictionary = Contested(data.disabled, Shape::Dictionary);
  plain = Contested(data.disabled, Shape::Plain);
  if (!dictionary || !plain || !values.Repeats()) {
    return;
  }
  if (values.trial.rows == 0) {
    Lz4Of<DictionaryRows>(data).Trial(Fast(Shape::Dictionary), values);
  }
  const auto dictionary_cost = static_cast<double>(
    Lz4Of<DictionaryRows>(data).Size(Fast(Shape::Dictionary), values));
  const auto& trial = values.trial;
  const auto least =
    static_cast<double>(kHeaderSize + Packed(trial.rows, trial.max_len) +
                        trial.input / kLz4MaxRatio);
  if (ShapeCost(Shape::Plain, true, least) >=
      dictionary_cost * (1.0 + kShapeSlack)) {
    plain = false;
    return;
  }
  const auto plain_cost =
    ShapeCost(Shape::Plain, true,
              static_cast<double>(
                Lz4Of<PlainRows>(data).Size(Fast(Shape::Plain), values)));
  plain = plain_cost < dictionary_cost * (1.0 + kShapeSlack);
  dictionary = dictionary_cost * (1.0 - kShapeSlack) <= plain_cost;
}

}  // namespace

bool Contends(ColumnDataCheckpointData& data, Shape shape) {
  auto& values = data.Get<Values>();
  if (!values.shapes_measured) {
    values.shapes_measured = true;
    MeasureShapes(data, values);
  }
  return values.contends[std::to_underlying(shape)];
}

}  // namespace irs::codecs::text
