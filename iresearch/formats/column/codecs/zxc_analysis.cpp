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

#include "iresearch/formats/column/codecs/zxc_analysis.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iterator>
#include <limits>

namespace irs::codecs::text {
namespace {

constexpr double kLevelTolerance = 0.02;
constexpr double kWideFramesGain = 0.03;
constexpr uint8_t kLevels[] = {1, 3};
constexpr auto kLeaf = static_cast<size_t>(ByteCodec::Zxc);

}  // namespace

Estimate AnalyzeZxc(StringWindow& window, Shape shape, bool tune,
                    StringTuning& tuning) {
  Config config{.choice = {shape, ByteCodec::Zxc}};
  if (!tune) {
    config.level = absl::c_linear_search(kLevels, tuning.level[kLeaf])
                     ? tuning.level[kLeaf]
                     : kLevels[0];
    config.layout = tuning.layout[kLeaf];
    return {config, window.Price(config)};
  }
  uint64_t prices[std::size(kLevels)];
  uint64_t lowest = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < std::size(kLevels); ++i) {
    config.level = kLevels[i];
    prices[i] = window.Price(config);
    lowest = std::min(lowest, prices[i]);
  }
  size_t rung = 0;
  while (static_cast<double>(prices[rung]) >
         static_cast<double>(lowest) * (1.0 + kLevelTolerance)) {
    ++rung;
  }
  config.level = kLevels[rung];
  Estimate best{config, prices[rung]};
  const auto prefer = [&](FrameLayout layout, double gain) {
    auto candidate = best.config;
    candidate.layout = layout;
    const auto bytes = window.Price(candidate);
    if (static_cast<double>(bytes) <
        static_cast<double>(best.bytes) * (1.0 - gain)) {
      best = {candidate, bytes};
    }
  };
  prefer(FrameLayout::Local, 0.0);
  prefer(FrameLayout::Wide, kWideFramesGain);
  tuning.level[kLeaf] = best.config.level;
  tuning.layout[kLeaf] = best.config.layout;
  return best;
}

}  // namespace irs::codecs::text
