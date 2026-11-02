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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <cstdint>
#include <iresearch/utils/huge_page_allocator.hpp>

#include "tests_shared.hpp"

namespace {

constexpr size_t kHugePage = size_t{2} << 20;

TEST(HugePageAllocatorTest, GrowthAcrossHugePageKeepsContents) {
  irs::HugePageVector<uint32_t> v;
  for (uint32_t i = 0; i < kHugePage; ++i) {
    v.push_back(i);
  }
  for (uint32_t i = 0; i < kHugePage; ++i) {
    ASSERT_EQ(i, v[i]);
  }
  v.resize(7);
  v.shrink_to_fit();
  for (uint32_t i = 0; i < 7; ++i) {
    ASSERT_EQ(i, v[i]);
  }
}

TEST(HugePageAllocatorTest, ValuesGivenAreConstructed) {
  irs::HugePageVector<uint32_t> v(kHugePage, 5);
  EXPECT_EQ(5, v.front());
  EXPECT_EQ(5, v.back());
  v.assign(3, 9);
  EXPECT_EQ((irs::HugePageVector<uint32_t>{9, 9, 9}), v);
  v.resize(kHugePage, 4);
  EXPECT_EQ(9, v[2]);
  EXPECT_EQ(4, v[3]);
  EXPECT_EQ(4, v.back());
}

#ifdef __linux__
TEST(HugePageAllocatorTest, LargeArraysStartOnHugePage) {
  irs::HugePageVector<uint8_t> large(kHugePage, 1);
  EXPECT_EQ(0, reinterpret_cast<uintptr_t>(large.data()) % kHugePage);
  EXPECT_EQ(1, large.back());
  irs::HugePageVector<uint8_t> small(kHugePage - 1, 2);
  EXPECT_EQ(2, small.front());
  EXPECT_EQ(2, small.back());
}
#endif

}  // namespace
