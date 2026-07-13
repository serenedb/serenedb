////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <cstdint>

#include "basics/random/random_generator.h"
#include "gtest/gtest.h"

using namespace sdb;

TEST(RandomGeneratorTest, test_RandomGeneratorTest_interval_uint32_brute) {
  random::Reset();
  random::Ensure();

  constexpr uint32_t kBounds[] = {1,    2,     4,     1023,
                                  1024, 65535, 65536, 4294967295ULL};
  for (uint32_t bound : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      uint32_t value = random::Interval(bound);
      ASSERT_LE(value, bound);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_interval_uint32) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  uint32_t value;
  value = random::Interval(uint32_t(0));
  EXPECT_EQ(0U, value);

  value = random::Interval(uint32_t(UINT32_MAX));
  EXPECT_EQ(3104012925U, value);

  value = random::Interval(uint32_t(5));
  EXPECT_EQ(4U, value);

  value = random::Interval(uint32_t(5));
  EXPECT_EQ(4U, value);

  value = random::Interval(uint32_t(5));
  EXPECT_EQ(1U, value);
}
