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

#include <gtest/gtest.h>

#include <cstring>

#include "basics/common.h"
#include "basics/strings.h"

using namespace sdb;

#define CHECK_CONVERSION_UINT32_HEX(value, expectedValue, buffer)         \
  actualLength = StringUInt32HexInPlace((uint32_t)value, (char*)&buffer); \
  EXPECT_EQ(actualLength, strlen(expectedValue));                         \
  EXPECT_EQ(std::string(buffer), std::string(expectedValue));

#define CHECK_CONVERSION_UINT64_HEX(value, expectedValue, buffer)         \
  actualLength = StringUInt64HexInPlace((uint64_t)value, (char*)&buffer); \
  EXPECT_EQ(actualLength, strlen(expectedValue));                         \
  EXPECT_EQ(std::string(buffer), std::string(expectedValue));

TEST(CConversionsTest, tst_uint32_hex) {
  char buffer[128];
  size_t actualLength;

  CHECK_CONVERSION_UINT32_HEX(0UL, "0", buffer)
  CHECK_CONVERSION_UINT32_HEX(1UL, "1", buffer)
  CHECK_CONVERSION_UINT32_HEX(9UL, "9", buffer)
  CHECK_CONVERSION_UINT32_HEX(10UL, "A", buffer)
  CHECK_CONVERSION_UINT32_HEX(128UL, "80", buffer)
  CHECK_CONVERSION_UINT32_HEX(3254UL, "CB6", buffer)
  CHECK_CONVERSION_UINT32_HEX(65535UL, "FFFF", buffer)
  CHECK_CONVERSION_UINT32_HEX(65536UL, "10000", buffer)
  CHECK_CONVERSION_UINT32_HEX(68863UL, "10CFF", buffer)
  CHECK_CONVERSION_UINT32_HEX(465765536ULL, "1BC304A0", buffer)
  CHECK_CONVERSION_UINT32_HEX(UINT32_MAX, "FFFFFFFF", buffer)
}

TEST(CConversionsTest, tst_uint64_hex) {
  char buffer[128];
  size_t actualLength;

  CHECK_CONVERSION_UINT64_HEX(0ULL, "0", buffer)
  CHECK_CONVERSION_UINT64_HEX(1ULL, "1", buffer)
  CHECK_CONVERSION_UINT64_HEX(9ULL, "9", buffer)
  CHECK_CONVERSION_UINT64_HEX(10ULL, "A", buffer)
  CHECK_CONVERSION_UINT64_HEX(128ULL, "80", buffer)
  CHECK_CONVERSION_UINT64_HEX(3254ULL, "CB6", buffer)
  CHECK_CONVERSION_UINT64_HEX(65535ULL, "FFFF", buffer)
  CHECK_CONVERSION_UINT64_HEX(65536ULL, "10000", buffer)
  CHECK_CONVERSION_UINT64_HEX(68863ULL, "10CFF", buffer)
  CHECK_CONVERSION_UINT64_HEX(465765536ULL, "1BC304A0", buffer)
  CHECK_CONVERSION_UINT64_HEX(47634665765536ULL, "2B52CF54FAA0", buffer)
  CHECK_CONVERSION_UINT64_HEX(8668398959769325ULL, "1ECBDCE8C4B6ED", buffer)
  CHECK_CONVERSION_UINT64_HEX(UINT64_MAX, "FFFFFFFFFFFFFFFF", buffer)
}
