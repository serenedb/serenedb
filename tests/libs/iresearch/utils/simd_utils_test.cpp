////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include "iresearch/utils/simd_utils.hpp"
#include "tests_shared.hpp"

// `irs::AllSame` is the only function left in simd_utils.hpp after the
// HWY-based helpers (delta_encode / avg / zigzag / maxmin / maxbits) were
// removed along with the legacy columnstore. `AllSame` is still called
// from `utils/bitpack.hpp` to short-circuit FOR-encoding when every value
// in a block is identical.
TEST(simd_utils_test, all_equal) {
  // Trivially-true: one element.
  {
    const uint32_t one[1] = {7};
    EXPECT_TRUE(irs::AllSame(one, 1));
  }
  // All equal.
  {
    const uint32_t same[8] = {5, 5, 5, 5, 5, 5, 5, 5};
    EXPECT_TRUE(irs::AllSame(same, 8));
  }
  // One differs at the start, middle, end.
  {
    uint32_t differ[16];
    std::fill(std::begin(differ), std::end(differ), 9);
    differ[0] = 8;
    EXPECT_FALSE(irs::AllSame(differ, 16));

    std::fill(std::begin(differ), std::end(differ), 9);
    differ[7] = 10;
    EXPECT_FALSE(irs::AllSame(differ, 16));

    std::fill(std::begin(differ), std::end(differ), 9);
    differ[15] = 11;
    EXPECT_FALSE(irs::AllSame(differ, 16));
  }
  // Different scalar widths exercise the type-generic template.
  {
    const uint8_t u8[4] = {1, 1, 1, 1};
    EXPECT_TRUE(irs::AllSame(u8, 4));

    const uint64_t u64[4] = {0xdeadbeefULL, 0xdeadbeefULL, 0xdeadbeefULL,
                             0xdeadbeefULL};
    EXPECT_TRUE(irs::AllSame(u64, 4));

    const int32_t mixed[3] = {-1, -1, 0};
    EXPECT_FALSE(irs::AllSame(mixed, 3));
  }
}
