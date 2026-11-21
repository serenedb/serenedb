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
inline constexpr double kFactsMove = 1.1;
inline constexpr double kWorthSwitching = 1.02;
inline constexpr uint8_t kOldHint = 8;

using Facts = std::array<float, 3>;

struct Hint {
  Facts facts = {};
  double score = kNoScore;
  uint8_t form = 0;
  uint8_t age = 0;
  bool known = false;
};

constexpr bool Forgotten(const Hint& hint) noexcept {
  return !hint.known || hint.age >= kOldHint;
}

constexpr double Shift(const Facts& now, const Facts& then) noexcept {
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
    return best == kNoScore || shift >= kFactsMove;
  }
  return remembered * kWorthSwitching <= best * shift;
}

template<typename Measure>
double Hinted(Hint& hint, double bound, const Facts& now, double least_shift,
              Measure&& measure) {
  const bool forgotten = Forgotten(hint);
  const auto shift = forgotten ? kNoScore : Shift(now, hint.facts);
  const auto counted =
    hint.score == kNoScore || shift >= least_shift ? shift : 1.0;
  if (!forgotten && !Measures(hint.score, bound, counted)) {
    ++hint.age;
    return kNoScore;
  }
  hint.score = measure(shift >= kFactsMove);
  hint.facts = now;
  hint.age = 0;
  hint.known = true;
  return hint.score;
}

}  // namespace irs::codecs
