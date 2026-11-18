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

struct Pick {
  CodecType codec = CodecType::Auto;
  uint8_t layout = 0;
};

struct ReusePolicy {
  static constexpr uint32_t kMaxGap = 16;

  Pick pick;
  uint32_t gap = 1;
  uint32_t since = 0;
  double reference = 0;
  bool calibrated = false;

  bool Due() noexcept { return !calibrated || ++since >= gap; }

  void Restart() noexcept {
    gap = 1;
    since = 0;
  }

  void Release() noexcept { pick = {}; }

  bool Grew(double bytes, double input) const noexcept {
    return bytes / input > reference * kDrift;
  }

  bool Undercuts(double bytes, double input) const noexcept {
    return bytes * kDrift < reference * input;
  }

  void Settle(Pick next, double bytes, double input) noexcept {
    gap = pick.codec == next.codec ? std::min(gap * 2, kMaxGap) : 1;
    since = 0;
    calibrated = true;
    pick = next;
    if (input != 0) {
      reference = bytes / input;
    }
  }
};

}  // namespace irs::codecs
