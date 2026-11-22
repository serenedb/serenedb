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
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace irs::codecs {

inline constexpr double kNoScore = std::numeric_limits<double>::infinity();
inline constexpr double kFeaturesMove = 1.1;
inline constexpr double kWorthSwitching = 1.02;
inline constexpr uint32_t kOldHint = 8;

using Features = std::array<float, 3>;

struct Hint {
  Features features = {};
  double score = kNoScore;
  uint32_t forced = 0;
};

constexpr bool Forgotten(const Hint& hint, uint32_t row_group) noexcept {
  return hint.forced == 0 || row_group - hint.forced > kOldHint;
}

constexpr double Shift(const Features& now, const Features& then) noexcept {
  double shift = 1;
  for (size_t i = 0; i < now.size(); ++i) {
    const double a = now[i];
    const double b = then[i];
    if (a == b) {
      continue;
    }
    if (a == 0 || b == 0) {
      return kNoScore;
    }
    shift = std::max(shift, a > b ? a / b : b / a);
  }
  return shift;
}

constexpr bool Measures(double remembered, double best, double shift) noexcept {
  if (remembered == kNoScore) {
    return best == kNoScore || shift >= kFeaturesMove;
  }
  return remembered * kWorthSwitching <= best * shift;
}

constexpr bool Measures(const Hint& hint, double best, const Features& now,
                        double least_shift) noexcept {
  const auto shift = Shift(now, hint.features);
  return Measures(hint.score, best,
                  hint.score == kNoScore || shift >= least_shift ? shift : 1.0);
}

}  // namespace irs::codecs
