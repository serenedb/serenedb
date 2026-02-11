////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <basics/math_utils.hpp>

#include "tests_shared.hpp"

namespace math = irs::math;

TEST(math_utils_test, IsPower2) {
  using namespace irs::math;

  // static_assert(!IsPower2(0), "Invalid answer"); // 0 gives false negative
  static_assert(IsPower2(1), "Invalid answer");
  static_assert(IsPower2(2), "Invalid answer");
  static_assert(!IsPower2(3), "Invalid answer");
  static_assert(IsPower2(4), "Invalid answer");
  static_assert(!IsPower2(999), "Invalid answer");
  static_assert(IsPower2(1024), "Invalid answer");
  static_assert(IsPower2(UINT64_C(1) << 63), "Invalid answer");
  static_assert(!IsPower2(std::numeric_limits<size_t>::max()),
                "Invalid answer");
}

TEST(math_utils_test, roundup_power2) {
  ASSERT_EQ(0, math::RoundupPower2(0));
  ASSERT_EQ(1, math::RoundupPower2(1));
  ASSERT_EQ(2, math::RoundupPower2(2));
  ASSERT_EQ(4, math::RoundupPower2(3));
  ASSERT_EQ(4, math::RoundupPower2(4));
  ASSERT_EQ(8, math::RoundupPower2(5));
  ASSERT_EQ(16, math::RoundupPower2(11));
  ASSERT_EQ(1024, math::RoundupPower2(900));
  ASSERT_EQ(std::numeric_limits<size_t>::max(),
            math::RoundupPower2(std::numeric_limits<size_t>::max()) -
              1);  // round to the max possible value
  ASSERT_EQ(0, math::RoundupPower2(std::numeric_limits<size_t>::max()));
}

TEST(math_utils_test, Log2Floor32) {
  ASSERT_EQ(0, math::Log2Floor32(1));
  ASSERT_EQ(1, math::Log2Floor32(2));
  ASSERT_EQ(1, math::Log2Floor32(3));
  ASSERT_EQ(2, math::Log2Floor32(4));
  ASSERT_EQ(2, math::Log2Floor32(6));
  ASSERT_EQ(3, math::Log2Floor32(8));
  ASSERT_EQ(9, math::Log2Floor32(999));
  ASSERT_EQ(10, math::Log2Floor32(1024));
  ASSERT_EQ(10, math::Log2Floor32(1025));
  ASSERT_EQ(31, math::Log2Floor32(std::numeric_limits<uint32_t>::max()));
}

TEST(math_utils_test, Log2Floor64) {
  ASSERT_EQ(0, math::Log2Floor64(UINT64_C(1)));
  ASSERT_EQ(1, math::Log2Floor64(UINT64_C(2)));
  ASSERT_EQ(1, math::Log2Floor64(UINT64_C(3)));
  ASSERT_EQ(2, math::Log2Floor64(UINT64_C(4)));
  ASSERT_EQ(2, math::Log2Floor64(UINT64_C(6)));
  ASSERT_EQ(3, math::Log2Floor64(UINT64_C(8)));
  ASSERT_EQ(9, math::Log2Floor64(UINT64_C(999)));
  ASSERT_EQ(10, math::Log2Floor64(UINT64_C(1024)));
  ASSERT_EQ(10, math::Log2Floor64(UINT64_C(1025)));
  ASSERT_EQ(31, math::Log2Floor64(std::numeric_limits<uint32_t>::max()));
  ASSERT_EQ(32, math::Log2Floor64(UINT64_C(1) << 32));
  ASSERT_EQ(32, math::Log2Floor64(UINT64_C(1) + (UINT64_C(1) << 32)));
  ASSERT_EQ(63, math::Log2Floor64(std::numeric_limits<uint64_t>::max()));
}

TEST(math_utils_test, log) {
  ASSERT_EQ(0, math::Log(1, 2));
  ASSERT_EQ(1, math::Log(235, 235));
  ASSERT_EQ(2, math::Log(49, 7));
  ASSERT_EQ(3, math::Log(8, 2));
  ASSERT_EQ(4, math::Log(12341, 7));
  ASSERT_EQ(6, math::Log(34563456, 17));
  ASSERT_EQ(31, math::Log(std::numeric_limits<uint32_t>::max(), 2));
}

TEST(math_utils_test, log2_32) {
  ASSERT_EQ(0, math::Log232(1));
  ASSERT_EQ(1, math::Log232(2));
  ASSERT_EQ(1, math::Log232(3));
  ASSERT_EQ(2, math::Log232(4));
  ASSERT_EQ(7, math::Log232(128));
  ASSERT_EQ(10, math::Log232(1025));
  ASSERT_EQ(31, math::Log232(std::numeric_limits<uint32_t>::max()));
}

TEST(math_utils_test, log2_64) {
  ASSERT_EQ(0, math::Log264(1));
  ASSERT_EQ(1, math::Log264(2));
  ASSERT_EQ(1, math::Log264(3));
  ASSERT_EQ(2, math::Log264(4));
  ASSERT_EQ(7, math::Log264(128));
  ASSERT_EQ(10, math::Log264(1025));
  ASSERT_EQ(31, math::Log264(std::numeric_limits<uint32_t>::max()));
  ASSERT_EQ(63, math::Log264(std::numeric_limits<uint64_t>::max()));
}

TEST(math_utils, DivCeil32) {
  ASSERT_EQ(7, math::DivCeil32(49, 7));
  ASSERT_EQ(7, math::DivCeil32(48, 7));
  ASSERT_EQ(6, math::DivCeil32(41, 7));
  ASSERT_EQ(6, math::DivCeil32(42, 7));
  ASSERT_EQ(1, math::DivCeil32(2, 2));
  ASSERT_EQ(2, math::DivCeil32(3, 2));
  ASSERT_EQ(1, math::DivCeil32(5, 7));
  ASSERT_EQ(0, math::DivCeil32(0, 7));
  ASSERT_EQ(0, math::DivCeil32(0, 1));
  ASSERT_EQ(7, math::DivCeil32(7, 1));
  ASSERT_EQ(1, math::DivCeil32(std::numeric_limits<uint32_t>::max() / 2,
                               std::numeric_limits<uint32_t>::max() / 2));
  ASSERT_EQ(1, math::DivCeil32(std::numeric_limits<uint32_t>::max() / 2,
                               std::numeric_limits<uint32_t>::max() / 2));
  ASSERT_EQ(1, math::DivCeil32(-1 + std::numeric_limits<uint32_t>::max() / 2,
                               1 + std::numeric_limits<uint32_t>::max() / 2));
}

TEST(math_utils, DivCeil64) {
  ASSERT_EQ(7, math::DivCeil64(49, 7));
  ASSERT_EQ(7, math::DivCeil64(48, 7));
  ASSERT_EQ(6, math::DivCeil64(41, 7));
  ASSERT_EQ(6, math::DivCeil64(42, 7));
  ASSERT_EQ(1, math::DivCeil64(2, 2));
  ASSERT_EQ(2, math::DivCeil64(3, 2));
  ASSERT_EQ(1, math::DivCeil64(5, 7));
  ASSERT_EQ(0, math::DivCeil64(0, 7));
  ASSERT_EQ(0, math::DivCeil64(0, 1));
  ASSERT_EQ(7, math::DivCeil64(7, 1));
  ASSERT_EQ(1, math::DivCeil64(std::numeric_limits<uint64_t>::max() / 2,
                               std::numeric_limits<uint64_t>::max() / 2));
  ASSERT_EQ(1, math::DivCeil64(-1 + std::numeric_limits<uint64_t>::max() / 2,
                               1 + std::numeric_limits<uint64_t>::max() / 2));
}

TEST(math_utils, ceil32) {
  ASSERT_EQ(41, math::Ceil32(41, 1));
  ASSERT_EQ(0, math::Ceil32(0, 7));
  ASSERT_EQ(42, math::Ceil32(41, 7));
  ASSERT_EQ(42, math::Ceil32(42, 7));
  ASSERT_EQ(49, math::Ceil32(43, 7));
  ASSERT_EQ(uint32_t(std::numeric_limits<uint32_t>::max() - 3),
            math::Ceil32(std::numeric_limits<uint32_t>::max() - 3, 2));
  ASSERT_EQ(uint32_t(std::numeric_limits<uint32_t>::max() - 3),
            math::Ceil32(std::numeric_limits<uint32_t>::max() - 4, 2));
}

TEST(math_utils, ceil64) {
  ASSERT_EQ(41, math::Ceil64(41, 1));
  ASSERT_EQ(0, math::Ceil64(0, 7));
  ASSERT_EQ(42, math::Ceil64(41, 7));
  ASSERT_EQ(42, math::Ceil64(42, 7));
  ASSERT_EQ(49, math::Ceil64(43, 7));
  ASSERT_EQ(uint64_t(std::numeric_limits<uint64_t>::max() - 3),
            math::Ceil64(std::numeric_limits<uint64_t>::max() - 3, 2));
  ASSERT_EQ(uint64_t(std::numeric_limits<uint64_t>::max() - 3),
            math::Ceil64(std::numeric_limits<uint64_t>::max() - 4, 2));
}
