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
#include <optional>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_tuning.hpp"
#include "iresearch/formats/column/codecs/reuse_policy.hpp"

namespace irs::codecs {

struct RatioHistory {
  uint64_t raw = 0;
  uint64_t comp = 0;
};

constexpr double Ratio(const RatioHistory& hist, double empty) noexcept {
  if (hist.raw != 0) {
    return static_cast<double>(hist.comp) / static_cast<double>(hist.raw);
  }
  return empty;
}

namespace text {

struct Config {
  Shape shape = Shape::Dictionary;
  ByteCodec leaf = ByteCodec::Lz4;
  uint8_t level = 0;
  FrameDictionary layout = FrameDictionary::FirstFrame;

  bool operator==(const Config&) const = default;
};

}  // namespace text

struct TunedParameters {
  bool done = false;
  uint8_t level[kByteCodecCount]{};
  FrameDictionary layout[kByteCodecCount]{};
};

struct EstimatorState {
  RatioHistory history[kByteCodecCount][kShapeCount]{};
  NumericTuning codes;
};

struct StringTuning {
  std::optional<text::Config> config;
  TunedParameters tuned;
  EstimatorState estimators;
  uint64_t last_distinct = 0;
};

}  // namespace irs::codecs
