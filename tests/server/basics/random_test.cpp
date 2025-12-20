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

template<typename T, T MinValue = 0,
         T MaxValue = std::numeric_limits<std::make_signed_t<T>>::max()>
struct UniformRandomGenerator {
  using result_type = T;
  using internal_type = std::make_signed_t<T>;

  static_assert(std::is_unsigned_v<T>);
  static_assert(MaxValue <= std::numeric_limits<internal_type>::max());

  static constexpr result_type min() noexcept { return MinValue; }
  static constexpr result_type max() noexcept { return MaxValue; }

  constexpr result_type operator()() {
    return random::Interval(static_cast<internal_type>(min()),
                            static_cast<internal_type>(max()));
  }
};

TEST(RandomGeneratorTest, test_RandomGeneratorTest_random_uint32) {
  random::Reset();
  random::Ensure();
  ASSERT_EQ(random::Interval(INT32_MAX, 5), INT32_MAX);
  ASSERT_LE(random::Interval(1000, INT32_MAX), INT32_MAX);
  ASSERT_GE(random::Interval(1000, INT32_MAX), 1000);
}

TEST(RandomGeneratorTest,
     test_RandomGeneratorTest_UniformRandomGenerator_uint16) {
  UniformRandomGenerator<uint16_t> uniform_random_generator;
  ASSERT_EQ(uniform_random_generator.min(), 0);
  ASSERT_EQ(uniform_random_generator.max(), INT16_MAX);
  for (int i = 0; i < 10000; ++i) {
    uint16_t value = uniform_random_generator();
    ASSERT_LE(value, uniform_random_generator.max());
  }
}

TEST(RandomGeneratorTest,
     test_RandomGeneratorTest_UniformRandomGenerator_uint32) {
  UniformRandomGenerator<uint32_t> uniform_random_generator;
  ASSERT_EQ(uniform_random_generator.min(), 0);
  ASSERT_EQ(uniform_random_generator.max(), INT32_MAX);
  for (int i = 0; i < 10000; ++i) {
    uint32_t value = uniform_random_generator();
    ASSERT_LE(value, uniform_random_generator.max());
  }
}

TEST(RandomGeneratorTest,
     test_RandomGeneratorTest_UniformRandomGenerator_uint64) {
  UniformRandomGenerator<uint64_t> uniform_random_generator;
  ASSERT_EQ(uniform_random_generator.min(), 0);
  ASSERT_EQ(uniform_random_generator.max(), INT64_MAX);
  for (int i = 0; i < 10000; ++i) {
    uint64_t value = uniform_random_generator();
    ASSERT_LE(value, uniform_random_generator.max());
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorTest_interval_uint16_brute) {
  random::Reset();
  random::Ensure();

  constexpr uint16_t kBounds[] = {1, 2, 4, 1023, 1024, 65535};
  for (uint16_t bound : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      uint16_t value = random::Interval(bound);
      ASSERT_LE(value, bound);
    }
  }
}

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

TEST(RandomGeneratorTest, test_RandomGeneratorTest_interval_uint64_brute) {
  random::Reset();
  random::Ensure();

  constexpr uint64_t kBounds[] = {1,
                                  2,
                                  4,
                                  1023,
                                  1024,
                                  65535,
                                  65536,
                                  4294967295ULL,
                                  4294967296ULL,
                                  18446744073709551615ULL};
  for (uint64_t bound : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      uint64_t value = random::Interval(bound);
      ASSERT_LE(value, bound);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorTest_ranges_int16_brute) {
  random::Reset();
  random::Ensure();

  constexpr std::pair<int16_t, int16_t> kBounds[] = {
    {0, 0},         {0, 1},         {0, 2},         {0, 1023},
    {0, 1024},      {0, 32760},     {0, 32767},     {1, 1},
    {1, 2},         {1, 1023},      {1, 1024},      {1, 32766},
    {1, 32767},     {10, 10},       {10, 11},       {10, 32766},
    {10, 32767},    {1024, 32760},  {1024, 32766},  {1024, 32767},
    {32760, 32761}, {32760, 32765}, {32760, 32766}, {32760, 32767},
    {32766, 32766}, {32766, 32767}, {32767, 32767},
  };
  for (auto [lower, upper] : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      int16_t value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
      value = random::Interval(upper, lower);
      ASSERT_EQ(value, upper);
      value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorTest_ranges_int32_brute) {
  random::Reset();
  random::Ensure();

  constexpr std::pair<int32_t, int32_t> kBounds[] = {
    {0, 0},
    {0, 1},
    {0, 2},
    {0, 1023},
    {0, 1024},
    {0, 65534},
    {0, 65535},
    {0, 2147483647},
    {1, 1},
    {1, 2},
    {1, 1023},
    {1, 1024},
    {1, 65534},
    {1, 65535},
    {1, 2147483647},
    {10, 10},
    {10, 11},
    {10, 65534},
    {10, 65535},
    {10, 2147483647},
    {1024, 65534},
    {1024, 65535},
    {1024, 2147483647},
    {65530, 65534},
    {65530, 65535},
    {65530, 2147483647},
    {2147483640, 2147483640},
    {2147483640, 2147483641},
    {2147483640, 2147483642},
    {2147483640, 2147483646},
    {2147483640, 2147483647},
    {2147483646, 2147483646},
    {2147483646, 2147483647},
    {2147483647, 2147483647},
  };
  for (auto [lower, upper] : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      int32_t value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
      value = random::Interval(upper, lower);
      ASSERT_EQ(value, upper);
      value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorTest_ranges_int64_brute) {
  random::Reset();
  random::Ensure();

  constexpr std::pair<int64_t, int64_t> kBounds[] = {
    {0, 0},
    {0, 1},
    {0, 2},
    {0, 1023},
    {0, 1024},
    {0, 65534},
    {0, 65535},
    {0, 2147483647},
    {0, 9223372036854775807LL},
    {1, 1},
    {1, 2},
    {1, 1023},
    {1, 1024},
    {1, 65534},
    {1, 65535},
    {1, 2147483647},
    {1, 9223372036854775807LL},
    {10, 10},
    {10, 11},
    {10, 65534},
    {10, 65535},
    {10, 9223372036854775807LL},
    {2147483640, 2147483640},
    {2147483640, 2147483641},
    {2147483640, 2147483642},
    {2147483640, 2147483646},
    {2147483640, 2147483647},
    {2147483640, 9223372036854775807LL},
    {9223372036854775800LL, 9223372036854775800LL},
    {9223372036854775800LL, 9223372036854775806LL},
    {9223372036854775800LL, 9223372036854775807LL},
    {9223372036854775806LL, 9223372036854775806LL},
    {9223372036854775806LL, 9223372036854775807LL},
    {9223372036854775807LL, 9223372036854775807LL},
  };
  for (auto [lower, upper] : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      int64_t value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
      value = random::Interval(upper, lower);
      ASSERT_EQ(value, upper);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorTest_random_int32_brute) {
  random::Reset();
  random::Ensure();

  constexpr std::pair<int32_t, int32_t> kBounds[] = {
    {0, 0},
    {0, 1},
    {0, 2},
    {0, 1023},
    {0, 1024},
    {0, 65534},
    {0, 65535},
    {0, 2147483646},
    {0, 2147483647},
    {1, 1},
    {1, 2},
    {1, 1023},
    {1, 1024},
    {1, 65534},
    {1, 65535},
    {1, 2147483646},
    {1, 2147483647},
    {10, 10},
    {10, 11},
    {10, 65534},
    {10, 65535},
    {10, 2147483647},
    {1024, 65534},
    {1024, 65535},
    {1024, 2147483647},
    {65530, 65534},
    {65530, 65535},
    {65530, 2000000000},
    {65530, 2147483600},
    {65535, 2147483600},
    {65536, 2147483600},
    {2000000000, 2147483640},
    {2000000000, 2147483641},
    {2000000000, 2147483642},
    {2000000000, 2147483646},
    {2000000000, 2147483647},
    {2147483640, 2147483640},
    {2147483640, 2147483641},
    {2147483640, 2147483642},
    {2147483640, 2147483646},
    {2147483640, 2147483647},
    {2147483646, 2147483646},
    {2147483646, 2147483647},
    {2147483647, 2147483647},
  };
  for (auto [lower, upper] : kBounds) {
    for (int i = 0; i < 10000; ++i) {
      int32_t value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
      value = random::Interval(upper, lower);
      ASSERT_EQ(value, upper);
      value = random::Interval(lower, upper);
      ASSERT_GE(value, lower);
      ASSERT_LE(value, upper);
    }
  }
}

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_ranges_int64) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  int64_t value;
  value = random::Interval(int64_t(INT64_MIN), int64_t(0));
  EXPECT_EQ(-5115110072778412328, value);

  value = random::Interval(int64_t(INT64_MIN), int64_t(-20));
  EXPECT_EQ(-4189371921854575647, value);

  value = random::Interval(int64_t(INT64_MIN), int64_t(19));
  EXPECT_EQ(-6664312351376758686, value);

  value = random::Interval(int64_t(0), int64_t(INT64_MAX));
  EXPECT_EQ(7199328709248114754, value);

  value = random::Interval(int64_t(INT64_MIN), int64_t(INT64_MAX));
  EXPECT_EQ(7812659160807914514, value);

  value = random::Interval(int64_t(0), int64_t(0));
  EXPECT_EQ(0, value);

  value = random::Interval(int64_t(INT64_MIN), int64_t(INT64_MIN));
  EXPECT_EQ(INT64_MIN, value);

  value = random::Interval(int64_t(INT64_MAX), int64_t(INT64_MAX));
  EXPECT_EQ(INT64_MAX, value);

  value = random::Interval(int64_t(5), int64_t(15));
  EXPECT_EQ(12, value);

  value = random::Interval(int64_t(5), int64_t(15));
  EXPECT_EQ(6, value);

  value = random::Interval(int64_t(5), int64_t(15));
  EXPECT_EQ(13, value);

  value = random::Interval(int64_t(INT64_MIN), int64_t(-49874753588));
  EXPECT_EQ(-6563046468452533532, value);
}

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_ranges_int32) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  int32_t value;
  value = random::Interval(int32_t(INT32_MIN), int32_t(0));
  EXPECT_EQ(-1190954371, value);

  value = random::Interval(int32_t(0), int32_t(INT32_MAX));
  EXPECT_EQ(1694838488, value);

  value = random::Interval(int32_t(INT32_MIN), int32_t(INT32_MAX));
  EXPECT_EQ(-975414162, value);

  value = random::Interval(int32_t(0), int32_t(0));
  EXPECT_EQ(0, value);

  value = random::Interval(int32_t(INT32_MIN), int32_t(INT32_MIN));
  EXPECT_EQ(INT32_MIN, value);

  value = random::Interval(int32_t(INT32_MAX), int32_t(INT32_MAX));
  EXPECT_EQ(INT32_MAX, value);

  value = random::Interval(int32_t(5), int32_t(15));
  EXPECT_EQ(9, value);

  value = random::Interval(int32_t(5), int32_t(15));
  EXPECT_EQ(6, value);

  value = random::Interval(int32_t(5), int32_t(15));
  EXPECT_EQ(11, value);

  value = random::Interval(int32_t(INT32_MIN), int32_t(-485774));
  EXPECT_EQ(-1208965022, value);
}

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_ranges_int16) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  int16_t value;
  value = random::Interval(int16_t(INT16_MIN), int16_t(0));
  EXPECT_EQ(-30601, value);

  value = random::Interval(int16_t(0), int16_t(INT16_MAX));
  EXPECT_EQ(11992, value);

  value = random::Interval(int16_t(INT16_MIN), int16_t(INT16_MAX));
  EXPECT_EQ(-9106, value);

  value = random::Interval(int16_t(0), int16_t(0));
  EXPECT_EQ(0, value);

  value = random::Interval(int16_t(INT16_MIN), int16_t(INT16_MIN));
  EXPECT_EQ(INT16_MIN, value);

  value = random::Interval(int16_t(INT16_MAX), int16_t(INT16_MAX));
  EXPECT_EQ(INT16_MAX, value);

  value = random::Interval(int16_t(5), int16_t(15));
  EXPECT_EQ(9, value);

  value = random::Interval(int16_t(5), int16_t(15));
  EXPECT_EQ(6, value);

  value = random::Interval(int16_t(5), int16_t(15));
  EXPECT_EQ(11, value);

  value = random::Interval(int16_t(INT16_MIN), int16_t(-4854));
  EXPECT_EQ(-16442, value);
}

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_interval_uint64) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  uint64_t value;
  value = random::Interval(uint64_t(0));
  EXPECT_EQ(0U, value);

  value = random::Interval(uint64_t(UINT64_MAX));
  EXPECT_EQ(13331634000931139288U, value);

  value = random::Interval(uint64_t(5));
  EXPECT_EQ(4U, value);

  value = random::Interval(uint64_t(5));
  EXPECT_EQ(1U, value);

  value = random::Interval(uint64_t(5));
  EXPECT_EQ(0U, value);
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

TEST(RandomGeneratorTest, test_RandomGeneratorMersenne_interval_uint16) {
  GTEST_SKIP();
  random::Reset();
  random::Ensure();

  uint16_t value;
  value = random::Interval(uint16_t(0));
  EXPECT_EQ(0U, value);

  value = random::Interval(uint16_t(UINT16_MAX));
  EXPECT_EQ(31357U, value);

  value = random::Interval(uint16_t(5));
  EXPECT_EQ(4U, value);

  value = random::Interval(uint16_t(5));
  EXPECT_EQ(4U, value);

  value = random::Interval(uint16_t(5));
  EXPECT_EQ(1U, value);
}
