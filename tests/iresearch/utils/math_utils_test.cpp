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

#include <iresearch/utils/math_utils.hpp>

#include "tests_shared.hpp"

namespace math = irs::math;

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
