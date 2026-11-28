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
#include <bit>
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

TEST(lower_bound_test, partition_index_finds_the_partition_point) {
  for (const size_t lo : {size_t{0}, size_t{1}, size_t{5}}) {
    for (size_t len = 0; len != 300; ++len) {
      const size_t hi = lo + len;
      for (size_t point = lo; point <= hi; ++point) {
        size_t probes = 0;
        const auto found = irs::PartitionIndex(lo, hi, [&](size_t i) {
          EXPECT_GE(i, lo);
          EXPECT_LT(i, hi);
          ++probes;
          return i < point;
        });
        ASSERT_EQ(point, found) << lo << " " << len;
        ASSERT_LE(probes, std::bit_width(len))
          << lo << " " << len << " " << point;
        if (len >= 100) {
          continue;
        }
        for (size_t from = lo; from <= hi; ++from) {
          ASSERT_EQ(
            std::max(point, from),
            irs::GallopIndex(from, hi, [&](size_t i) { return i < point; }))
            << lo << " " << len << " " << point << " " << from;
        }
      }
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
