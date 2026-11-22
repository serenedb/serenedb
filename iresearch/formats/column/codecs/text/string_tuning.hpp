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

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

enum class FrameDictionary : uint8_t {
  None,
  FirstFrame,
  Local,
};

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

inline constexpr double kSortedGain = 0.03;
inline constexpr uint64_t kOrderRetuneGrowth = 4;

struct Config {
  Shape shape = Shape::Dictionary;
  ByteCodec leaf = ByteCodec::Lz4;
  uint8_t level = 0;
  FrameDictionary layout = FrameDictionary::FirstFrame;
  bool sorted = false;

  bool operator==(const Config&) const = default;
};

}  // namespace text

struct FrameTuning {
  bool tuned = false;
  uint8_t level = 0;
  FrameDictionary layout = FrameDictionary::None;
  uint64_t tuned_on = 0;
};

struct OrderTuning {
  bool tuned = false;
  bool sorted = false;
  uint64_t tuned_on = 0;
};

struct StringTuning {
  FrameTuning frames[kByteCodecCount][kTextShapeCount];
  OrderTuning order[kByteCodecCount];
  RatioHistory history[kByteCodecCount][kShapeCount]{};
  double codes_ratio = 0;
  uint64_t last_distinct = 0;
};

}  // namespace irs::codecs
