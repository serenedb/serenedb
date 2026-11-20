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

#include "iresearch/search/scorers/lm_similarity.hpp"

namespace irs {

double LogOfProductInChunks(size_t n,
                            absl::FunctionRef<double(size_t)> factor) noexcept {
  constexpr double kHigh = 0x1p300;
  constexpr double kLow = 0x1p-300;
  double log = 0;
  double product = 1;
  for (size_t i = 0; i != n; ++i) {
    product *= factor(i);
    if (product > kHigh || product < kLow) {
      log += std::log(product);
      product = 1;
    }
  }
  return log + std::log(product);
}

}  // namespace irs
