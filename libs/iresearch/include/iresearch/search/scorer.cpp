////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "scorer.hpp"

#include <vector>

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/attribute_provider.hpp"

namespace irs {

uint8_t Scorer::compatible(WandType index, WandType query) noexcept {
  auto bin_case = [](WandType index, WandType query) noexcept -> uint8_t {
    return (static_cast<uint8_t>(index) * 8) + static_cast<uint8_t>(query);
  };
  switch (bin_case(index, query)) {
    // no needed wand data
    case bin_case(WandType::None, WandType::None):
    case bin_case(WandType::None, WandType::DivNorm):
    case bin_case(WandType::None, WandType::MaxFreq):
    case bin_case(WandType::None, WandType::MinNorm):
    case bin_case(WandType::DivNorm, WandType::None):
    case bin_case(WandType::MaxFreq, WandType::None):
    case bin_case(WandType::MinNorm, WandType::None):
      SDB_ASSERT(false);
      [[fallthrough]];
    // DivNorm very precise and is not compatible with other types
    case bin_case(WandType::DivNorm, WandType::MaxFreq):
    case bin_case(WandType::DivNorm, WandType::MinNorm):
      return 0;
    // MaxFreq suitable for any other type
    case bin_case(WandType::MaxFreq, WandType::DivNorm):
    case bin_case(WandType::MaxFreq, WandType::MinNorm):
    // MinNorm suitable for any score
    case bin_case(WandType::MinNorm, WandType::MaxFreq):
      return 1;
    case bin_case(WandType::MinNorm, WandType::DivNorm):
      return 2;
    case bin_case(WandType::DivNorm, WandType::DivNorm):
    case bin_case(WandType::MaxFreq, WandType::MaxFreq):
    case bin_case(WandType::MinNorm, WandType::MinNorm):
      return std::numeric_limits<uint8_t>::max();
  }
  return 0;
}

size_t Scorers::PushBack(const Scorer& scorer) {
  const auto [bucket_stats_size, bucket_stats_align] = scorer.stats_size();
  SDB_ASSERT(bucket_stats_align <= alignof(std::max_align_t));
  // math::IsPower2(0) returns true
  SDB_ASSERT(math::IsPower2(bucket_stats_align));

  _stats_size = memory::AlignUp(_stats_size, bucket_stats_align);
  _features |= scorer.GetIndexFeatures();
  _buckets.emplace_back(scorer, _stats_size);
  _stats_size += memory::AlignUp(bucket_stats_size, bucket_stats_align);

  return bucket_stats_align;
}

REGISTER_ATTRIBUTE(FilterBoost);

const Scorers Scorers::kUnordered;

}  // namespace irs
