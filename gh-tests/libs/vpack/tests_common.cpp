////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "tests-common.h"

inline uint64_t ReadUInt64(const uint8_t* start) noexcept {
  return ReadIntegerFixed<uint64_t, 8>(start);
}

inline void StoreUInt64(uint8_t* start, uint64_t value) noexcept {
  if constexpr (std::endian::native != std::endian::little) {
    value = HostToLittle(value);
  }
  memcpy(start, &value, sizeof(value));
}

static uint8_t gLocalBuffer[128];

TEST(CommonTest, StoreUInt64Zero) {
  const uint64_t value = 0;
  StoreUInt64(&gLocalBuffer[0], value);

  ASSERT_EQ(0UL, gLocalBuffer[0]);
  ASSERT_EQ(0UL, gLocalBuffer[1]);
  ASSERT_EQ(0UL, gLocalBuffer[2]);
  ASSERT_EQ(0UL, gLocalBuffer[3]);
  ASSERT_EQ(0UL, gLocalBuffer[4]);
  ASSERT_EQ(0UL, gLocalBuffer[5]);
  ASSERT_EQ(0UL, gLocalBuffer[6]);
  ASSERT_EQ(0UL, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}

TEST(CommonTest, StoreUInt64One) {
  const uint64_t value = 1;
  StoreUInt64(&gLocalBuffer[0], value);
  ASSERT_EQ(0x01, gLocalBuffer[0]);
  ASSERT_EQ(0x00, gLocalBuffer[1]);
  ASSERT_EQ(0x00, gLocalBuffer[2]);
  ASSERT_EQ(0x00, gLocalBuffer[3]);
  ASSERT_EQ(0x00, gLocalBuffer[4]);
  ASSERT_EQ(0x00, gLocalBuffer[5]);
  ASSERT_EQ(0x00, gLocalBuffer[6]);
  ASSERT_EQ(0x00, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}

TEST(CommonTest, StoreUInt64Something) {
  const uint64_t value = 259;
  StoreUInt64(&gLocalBuffer[0], value);
  ASSERT_EQ(0x03, gLocalBuffer[0]);
  ASSERT_EQ(0x01, gLocalBuffer[1]);
  ASSERT_EQ(0x00, gLocalBuffer[2]);
  ASSERT_EQ(0x00, gLocalBuffer[3]);
  ASSERT_EQ(0x00, gLocalBuffer[4]);
  ASSERT_EQ(0x00, gLocalBuffer[5]);
  ASSERT_EQ(0x00, gLocalBuffer[6]);
  ASSERT_EQ(0x00, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}

TEST(CommonTest, StoreUInt64SomethingElse) {
  const uint64_t value = 0xab12760039;
  StoreUInt64(&gLocalBuffer[0], value);
  ASSERT_EQ(0x39, gLocalBuffer[0]);
  ASSERT_EQ(0x00, gLocalBuffer[1]);
  ASSERT_EQ(0x76, gLocalBuffer[2]);
  ASSERT_EQ(0x12, gLocalBuffer[3]);
  ASSERT_EQ(0xab, gLocalBuffer[4]);
  ASSERT_EQ(0x00, gLocalBuffer[5]);
  ASSERT_EQ(0x00, gLocalBuffer[6]);
  ASSERT_EQ(0x00, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}

TEST(CommonTest, StoreUInt64SomethingMore) {
  const uint64_t value = 0x7f9831ab12761339;

  StoreUInt64(&gLocalBuffer[0], value);
  ASSERT_EQ(0x39, gLocalBuffer[0]);
  ASSERT_EQ(0x13, gLocalBuffer[1]);
  ASSERT_EQ(0x76, gLocalBuffer[2]);
  ASSERT_EQ(0x12, gLocalBuffer[3]);
  ASSERT_EQ(0xab, gLocalBuffer[4]);
  ASSERT_EQ(0x31, gLocalBuffer[5]);
  ASSERT_EQ(0x98, gLocalBuffer[6]);
  ASSERT_EQ(0x7f, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}

TEST(CommonTest, StoreUInt64Max) {
  const uint64_t value = 0xffffffffffffffffULL;

  StoreUInt64(&gLocalBuffer[0], value);
  ASSERT_EQ(0xff, gLocalBuffer[0]);
  ASSERT_EQ(0xff, gLocalBuffer[1]);
  ASSERT_EQ(0xff, gLocalBuffer[2]);
  ASSERT_EQ(0xff, gLocalBuffer[3]);
  ASSERT_EQ(0xff, gLocalBuffer[4]);
  ASSERT_EQ(0xff, gLocalBuffer[5]);
  ASSERT_EQ(0xff, gLocalBuffer[6]);
  ASSERT_EQ(0xff, gLocalBuffer[7]);

  ASSERT_EQ(value, ReadUInt64(&gLocalBuffer[0]));
}
