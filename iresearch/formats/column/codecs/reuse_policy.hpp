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

#include <algorithm>
#include <cstdint>

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

inline constexpr double kDrift = 1.25;

struct ReusePolicy {
  static constexpr uint32_t kMaxGap = 16;

  CodecType pick = CodecType::Auto;
  uint32_t gap = 1;
  uint32_t since = 0;
  double reference = 0;
  bool calibrated = false;

  bool Due() noexcept { return !calibrated || ++since >= gap; }

  void Restart() noexcept {
    gap = 1;
    since = 0;
  }

  bool Drifted(double bytes, double input) const noexcept {
    const auto ratio = bytes / input;
    return ratio > reference * kDrift || ratio * kDrift < reference;
  }

  bool Grew(double bytes, double input) const noexcept {
    return bytes / input > reference * kDrift;
  }

  void Settle(CodecType next, double bytes, double input) noexcept {
    gap = pick == next ? std::min(gap * 2, kMaxGap) : 1;
    since = 0;
    calibrated = true;
    pick = next;
    if (input != 0) {
      reference = bytes / input;
    }
  }
};

}  // namespace irs::codecs
