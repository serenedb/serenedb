////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/utils/file_utils_ext.hpp>
#include <random>
#include <vector>

#include "tests_shared.hpp"

TEST(file_utils_tests, residency_map) {
  irs::file_utils::ResidencyMap map;
  map.Reset(200);
  ASSERT_FALSE(map.Valid(7));
  ASSERT_TRUE(map.Adopt(7));
  ASSERT_TRUE(map.Valid(7));
  ASSERT_EQ(0, map.Test(0, 199));

  map.Set(60, 130);
  ASSERT_EQ(71, map.Test(60, 130));
  ASSERT_EQ(2, map.Test(63, 64));
  ASSERT_TRUE(map.Test(128));
  ASSERT_FALSE(map.Test(59));
  ASSERT_FALSE(map.Test(131));
  ASSERT_EQ(0, map.Test(59, 130));
  ASSERT_EQ(71, map.Test(60, 131));
  ASSERT_EQ(3, map.Test(128, 199));

  map.Set(199);
  ASSERT_TRUE(map.Test(199));
  ASSERT_EQ(0, map.Test(198, 199));

  ASSERT_FALSE(map.Adopt(6));
  ASSERT_TRUE(map.Valid(7));
  ASSERT_EQ(71, map.Test(60, 130));

  ASSERT_TRUE(map.Adopt(8));
  ASSERT_TRUE(map.Valid(8));
  ASSERT_FALSE(map.Valid(7));
  ASSERT_FALSE(map.Test(60));
  ASSERT_FALSE(map.Test(199));
}

TEST(file_utils_tests, residency_map_counts_leading_warm_pages) {
  constexpr size_t kPages = 320;
  std::mt19937 rng{42};
  std::vector<std::vector<bool>> patterns;
  for (const double density : {0.0, 0.5, 0.97, 1.0}) {
    std::bernoulli_distribution warm{density};
    auto& pattern = patterns.emplace_back(kPages);
    for (size_t page = 0; page != kPages; ++page) {
      pattern[page] = warm(rng);
    }
  }
  auto& edges = patterns.emplace_back(kPages, true);
  for (const size_t page : {63, 64, 127, 255, 256, 319}) {
    edges[page] = false;
  }

  irs::file_utils::ResidencyMap map;
  map.Reset(kPages);
  uint32_t epoch = 0;
  for (const auto& pattern : patterns) {
    ASSERT_TRUE(map.Adopt(++epoch));
    for (size_t page = 0; page != kPages; ++page) {
      if (pattern[page]) {
        map.Set(page);
      }
    }
    for (size_t first = 0; first != kPages; ++first) {
      size_t warm = 0;
      for (size_t last = first; last != kPages; ++last) {
        if (warm == last - first && pattern[last]) {
          ++warm;
        }
        ASSERT_EQ(warm, map.Test(first, last)) << first << ".." << last;
      }
    }
  }
}

TEST(file_utils_tests, residency_epoch) {
  const auto epoch = irs::file_utils::ResidencyEpoch();
  irs::file_utils::InvalidateResidency();
  ASSERT_NE(epoch, irs::file_utils::ResidencyEpoch());
}
