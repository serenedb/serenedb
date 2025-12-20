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

#include "basics/common.h"
#include "basics/number_utils.h"
#include "gtest/gtest.h"

using namespace sdb;

template<typename T>
static void TestImpl(bool should_be_valid, const std::string& value) {
  bool valid;
  T result = sdb::number_utils::Atoi<T>(value.c_str(),
                                        value.c_str() + value.size(), valid);
  ASSERT_EQ(should_be_valid, valid);
  if (should_be_valid) {
    ASSERT_EQ(value, std::to_string(result));
    T result2 = sdb::number_utils::AtoiUnchecked<T>(
      value.c_str(), value.c_str() + value.size());
    ASSERT_EQ(value, std::to_string(result2));
  }
}

template<typename T>
static void TestImpl(T expected, const std::string& value) {
  bool valid;
  T result = sdb::number_utils::Atoi<T>(value.c_str(),
                                        value.c_str() + value.size(), valid);
  ASSERT_TRUE(valid);
  ASSERT_EQ(expected, result);

  T result2 = sdb::number_utils::AtoiUnchecked<T>(value.c_str(),
                                                  value.c_str() + value.size());
  ASSERT_EQ(expected, result2);
}

TEST(NumberUtilsTest, testStrangeNumbers) {
  TestImpl<int64_t>(int64_t(0), "00");
  TestImpl<int64_t>(int64_t(0), "00000000000000000000000000000");
  TestImpl<int64_t>(int64_t(1), "01");
  TestImpl<int64_t>(int64_t(0), "-0");
  TestImpl<int64_t>(int64_t(-1), "-01");
  TestImpl<int64_t>(int64_t(-10), "-010");
  TestImpl<int64_t>(int64_t(0), "-00000");
  TestImpl<int64_t>(int64_t(-2), "-000002");
  TestImpl<int64_t>(int64_t(0), "+0");
  TestImpl<int64_t>(int64_t(0), "+00");
  TestImpl<int64_t>(int64_t(10), "+010");
  TestImpl<int64_t>(int64_t(0), "+00000000");
  TestImpl<int64_t>(int64_t(2), "+000000002");
  TestImpl<int64_t>(int64_t(0), "+0000000000000000000000000000000000000000");
  TestImpl<int64_t>(int64_t(22), "+000000000000000000000000000000000000000022");
}

TEST(NumberUtilsTest, testPredefinedConstants) {
  TestImpl<int16_t>(static_cast<int16_t>(INT16_MIN), std::to_string(INT16_MIN));
  TestImpl<int16_t>(INT16_MAX, std::to_string(INT16_MAX));

  TestImpl<int32_t>(INT32_MIN, std::to_string(INT32_MIN));
  TestImpl<int32_t>(INT32_MAX, std::to_string(INT32_MAX));

  TestImpl<int64_t>(INT64_MIN, std::to_string(INT64_MIN));
  TestImpl<int64_t>(INT64_MAX, std::to_string(INT64_MAX));

  TestImpl<uint8_t>(UINT8_MAX, std::to_string(UINT8_MAX));
  TestImpl<uint16_t>(UINT16_MAX, std::to_string(UINT16_MAX));
  TestImpl<uint32_t>(UINT32_MAX, std::to_string(UINT32_MAX));
  TestImpl<uint64_t>(UINT64_MAX, std::to_string(UINT64_MAX));

  TestImpl<size_t>(SIZE_MAX, std::to_string(SIZE_MAX));
}

TEST(NumberUtilsTest, testInvalidChars) {
  TestImpl<int64_t>(false, "");
  TestImpl<int64_t>(false, " ");
  TestImpl<int64_t>(false, "  ");
  TestImpl<int64_t>(false, "1a");
  TestImpl<int64_t>(false, "11234b");
  TestImpl<int64_t>(false, "1 ");
  TestImpl<int64_t>(false, "1234 ");
  TestImpl<int64_t>(false, "-");
  TestImpl<int64_t>(false, "+");
  TestImpl<int64_t>(false, "- ");
  TestImpl<int64_t>(false, "+ ");
  TestImpl<int64_t>(false, "-11234a");
  TestImpl<int64_t>(false, "-11234 ");
  TestImpl<int64_t>(false, "o");
  TestImpl<int64_t>(false, "ooooo");
  TestImpl<int64_t>(false, "1A2B3C");
  TestImpl<int64_t>(false, "aaaaa14453");
  TestImpl<int64_t>(false, "02a");
}

TEST(NumberUtilsTest, testInt64OutOfBoundsLow) {
  // out of bounds
  TestImpl<int64_t>(false,
                    "-1111111111111111111111111111111111111111111111111111111");
  TestImpl<int64_t>(false, "-111111111111111111111111111111111111111");
  TestImpl<int64_t>(false, "-9223372036854775810943");
  TestImpl<int64_t>(false, "-9223372036854775810");
  TestImpl<int64_t>(false, "-9223372036854775809");
}

TEST(NumberUtilsTest, testInt64InBounds) {
  // in bounds
  TestImpl<int64_t>(true, "-9223372036854775808");
  TestImpl<int64_t>(true, "-9223372036854775807");
  TestImpl<int64_t>(true, "-9223372036854775801");
  TestImpl<int64_t>(true, "-9223372036854775800");
  TestImpl<int64_t>(true, "-9223372036854775799");
  TestImpl<int64_t>(true, "-123456789012");
  TestImpl<int64_t>(true, "-999999999");
  TestImpl<int64_t>(true, "-98765543");
  TestImpl<int64_t>(true, "-10000");
  TestImpl<int64_t>(true, "-100");
  TestImpl<int64_t>(true, "-99");
  TestImpl<int64_t>(true, "-9");
  TestImpl<int64_t>(true, "-2");
  TestImpl<int64_t>(true, "-1");
  TestImpl<int64_t>(true, "0");
  TestImpl<int64_t>(true, "1");
  TestImpl<int64_t>(true, "10");
  TestImpl<int64_t>(true, "10000");
  TestImpl<int64_t>(true, "1234567890");
  TestImpl<int64_t>(true, "1844674407370955161");
  TestImpl<int64_t>(true, "9223372036854775799");
  TestImpl<int64_t>(true, "9223372036854775800");
  TestImpl<int64_t>(true, "9223372036854775806");
  TestImpl<int64_t>(true, "9223372036854775807");
}

TEST(NumberUtilsTest, testInt64OutOfBoundsHigh) {
  // out of bounds
  TestImpl<int64_t>(false, "9223372036854775808");
  TestImpl<int64_t>(false, "9223372036854775809");
  TestImpl<int64_t>(false, "18446744073709551610");
  TestImpl<int64_t>(false, "18446744073709551614");
  TestImpl<int64_t>(false, "18446744073709551615");
  TestImpl<int64_t>(false, "18446744073709551616");
  TestImpl<int64_t>(false, "118446744073709551612");
  TestImpl<int64_t>(false, "111111111111111111111111111111");
  TestImpl<int64_t>(
    false, "11111111111111111111111111111111111111111111111111111111111111111");
}

TEST(NumberUtilsTest, testUint64OutOfBoundsNegative) {
  // out of bounds
  TestImpl<uint64_t>(
    false, "-1111111111111111111111111111111111111111111111111111111111111");
  TestImpl<uint64_t>(false, "-1111111111111111111111111111111111111");
  TestImpl<uint64_t>(false, "-9223372036854775809");
  TestImpl<uint64_t>(false, "-9223372036854775808");
  TestImpl<uint64_t>(false, "-9223372036854775807");
  TestImpl<uint64_t>(false, "-10000");
  TestImpl<uint64_t>(false, "-10000");
  TestImpl<uint64_t>(false, "-1");
  TestImpl<uint64_t>(false, "-0");
}

TEST(NumberUtilsTest, testUint64InBounds) {
  // in bounds
  TestImpl<uint64_t>(true, "0");
  TestImpl<uint64_t>(true, "1");
  TestImpl<uint64_t>(true, "10");
  TestImpl<uint64_t>(true, "10000");
  TestImpl<uint64_t>(true, "1234567890");
  TestImpl<uint64_t>(true, "9223372036854775807");
  TestImpl<uint64_t>(true, "9223372036854775808");
  TestImpl<uint64_t>(true, "1844674407370955161");
  TestImpl<uint64_t>(true, "18446744073709551610");
  TestImpl<uint64_t>(true, "18446744073709551614");
  TestImpl<uint64_t>(true, "18446744073709551615");
}

TEST(NumberUtilsTest, testUint64OutOfBoundsHigh) {
  // out of bounds
  TestImpl<uint64_t>(false, "18446744073709551616");
  TestImpl<uint64_t>(false, "118446744073709551612");
  TestImpl<uint64_t>(false, "1111111111111111111111111111111111111");
  TestImpl<uint64_t>(
    false, "1111111111111111111111111111111111111111111111111111111111111");
}
