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
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/attribute_provider.hpp"

namespace irs {

uint8_t Scorer::compatible(ScoreBoundType index,
                           ScoreBoundType query) noexcept {
  auto bin_case = [](ScoreBoundType index,
                     ScoreBoundType query) noexcept -> uint8_t {
    return (static_cast<uint8_t>(index) * 8) + static_cast<uint8_t>(query);
  };
  switch (bin_case(index, query)) {
    // no score bounds needed
    case bin_case(ScoreBoundType::None, ScoreBoundType::None):
    case bin_case(ScoreBoundType::None, ScoreBoundType::DivNorm):
    case bin_case(ScoreBoundType::None, ScoreBoundType::MaxFreq):
    case bin_case(ScoreBoundType::None, ScoreBoundType::MinNorm):
    case bin_case(ScoreBoundType::DivNorm, ScoreBoundType::None):
    case bin_case(ScoreBoundType::MaxFreq, ScoreBoundType::None):
    case bin_case(ScoreBoundType::MinNorm, ScoreBoundType::None):
      SDB_ASSERT(false);
      [[fallthrough]];
    // DivNorm very precise and is not compatible with other types
    case bin_case(ScoreBoundType::DivNorm, ScoreBoundType::MaxFreq):
    case bin_case(ScoreBoundType::DivNorm, ScoreBoundType::MinNorm):
      return 0;
    // MaxFreq suitable for any other type
    case bin_case(ScoreBoundType::MaxFreq, ScoreBoundType::DivNorm):
    case bin_case(ScoreBoundType::MaxFreq, ScoreBoundType::MinNorm):
    // MinNorm suitable for any score
    case bin_case(ScoreBoundType::MinNorm, ScoreBoundType::MaxFreq):
      return 1;
    case bin_case(ScoreBoundType::MinNorm, ScoreBoundType::DivNorm):
      return 2;
    case bin_case(ScoreBoundType::DivNorm, ScoreBoundType::DivNorm):
    case bin_case(ScoreBoundType::MaxFreq, ScoreBoundType::MaxFreq):
    case bin_case(ScoreBoundType::MinNorm, ScoreBoundType::MinNorm):
      return std::numeric_limits<uint8_t>::max();
  }
  return 0;
}

}  // namespace irs
