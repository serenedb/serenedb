////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "levenshtein_default_pdp.hpp"

#include "iresearch/utils/std.hpp"
#include "levenshtein_utils.hpp"

namespace irs {

const ParametricDescription& DefaultPDP(uint8_t distance,
                                        bool with_transpositions) {
  struct Builder {
    using Type = ParametricDescription;

    static Type Make(size_t idx) {
      const auto max_distance = uint8_t(idx >> 1);
      const auto with_transpositions = 0 != (idx % 2);
      return MakeParametricDescription(max_distance, with_transpositions);
    }
  };

  const size_t idx = 2 * size_t(distance) + size_t(with_transpositions);
  return irstd::StaticLazyArray<Builder, 9>::at(idx);
}

}  // namespace irs
