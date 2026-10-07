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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include <algorithm>
#include <cstdint>
#include <iresearch/utils/lower_bound.hpp>
#include <random>
#include <vector>

#include "tests_shared.hpp"

namespace {

std::vector<uint16_t> Sorted(size_t len, uint32_t seed) {
  std::mt19937 gen{seed};
  std::uniform_int_distribution<uint32_t> pick{0,
                                               4 * static_cast<uint32_t>(len)};
  std::vector<uint16_t> values(len);
  for (auto& value : values) {
    value = static_cast<uint16_t>(pick(gen));
  }
  std::sort(values.begin(), values.end());
  return values;
}

}  // namespace

TEST(lower_bound_test, runtime_length_matches_std) {
  for (size_t len = 0; len != 600; ++len) {
    const auto values = Sorted(len, static_cast<uint32_t>(len) + 1);
    const auto* begin = values.data();
    const uint32_t top = 4 * static_cast<uint32_t>(len) + 2;
    for (uint32_t target = 0; target <= top; ++target) {
      const auto expected =
        std::lower_bound(values.begin(), values.end(), target) - values.begin();
      ASSERT_EQ(expected,
                irs::BranchlessPartitionPoint(
                  begin, len, [&](uint16_t v) { return v < target; }) -
                  begin)
        << len << " " << target;
      ASSERT_EQ(expected,
                irs::PartitionPoint(begin, len,
                                    [&](uint16_t v) { return v < target; }) -
                  begin)
        << len << " " << target;
    }
  }
}

TEST(lower_bound_test, fixed_length_matches_std) {
  const auto values = Sorted(128, 7);
  for (uint32_t target = 0; target <= 4 * 128 + 2; ++target) {
    const auto expected =
      std::lower_bound(values.begin(), values.end(), target) - values.begin();
    ASSERT_EQ(expected, irs::BranchlessLowerBound<128>(values.data(), target) -
                          values.data())
      << target;
  }
}
