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

#include <climits>
#include <iresearch/types.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <random>
#include <vector>

#include "tests_shared.hpp"

TEST(bit_utils_test, set_check_bit) {
  using namespace irs;

  unsigned v = 3422;

  for (unsigned i = 0; i < sizeof(v) * 8; ++i) {
    EXPECT_EQ(static_cast<bool>((v >> i) & 1), CheckBit(v, i));

    UnsetBit(v, i);
    EXPECT_FALSE(CheckBit(v, i));
  }
  EXPECT_EQ(0, v);

  byte_type b = 76;

  EXPECT_FALSE(CheckBit(b, 0));
  EXPECT_FALSE(CheckBit(b, 1));
  EXPECT_TRUE(CheckBit(b, 2));
  EXPECT_TRUE(CheckBit(b, 3));
  EXPECT_FALSE(CheckBit(b, 4));
  EXPECT_FALSE(CheckBit(b, 5));
  EXPECT_TRUE(CheckBit(b, 6));
  EXPECT_FALSE(CheckBit(b, 7));

  byte_type b_orig = b;
  SetBit(b, 0, !CheckBit(b, 0));
  SetBit(b, 0, !CheckBit(b, 0));
  SetBit(b, 1, !CheckBit(b, 1));
  SetBit(b, 1, !CheckBit(b, 1));
  SetBit(b, 2, !CheckBit(b, 2));
  SetBit(b, 2, !CheckBit(b, 2));
  SetBit(b, 3, !CheckBit(b, 3));
  SetBit(b, 3, !CheckBit(b, 3));
  SetBit(b, 4, !CheckBit(b, 4));
  SetBit(b, 4, !CheckBit(b, 4));
  SetBit(b, 5, !CheckBit(b, 5));
  SetBit(b, 5, !CheckBit(b, 5));
  SetBit(b, 6, !CheckBit(b, 6));
  SetBit(b, 6, !CheckBit(b, 6));
  SetBit(b, 7, !CheckBit(b, 7));
  SetBit(b, 7, !CheckBit(b, 7));
  EXPECT_EQ(b_orig, b);
}

TEST(bit_utils_test, test_unset_bit) {
  // test compile time
  {
    irs::byte_type b = 0x6c;

    EXPECT_FALSE(irs::CheckBit(b, 0));
    EXPECT_FALSE(irs::CheckBit(b, 1));
    EXPECT_TRUE(irs::CheckBit(b, 2));
    EXPECT_TRUE(irs::CheckBit(b, 3));
    EXPECT_FALSE(irs::CheckBit(b, 4));
    EXPECT_TRUE(irs::CheckBit(b, 5));
    EXPECT_TRUE(irs::CheckBit(b, 6));
    EXPECT_FALSE(irs::CheckBit(b, 7));

    irs::UnsetBit(b, 0, true);
    irs::UnsetBit(b, 1, false);
    irs::UnsetBit(b, 2, true);
    irs::UnsetBit(b, 3, false);
    irs::UnsetBit(b, 4, true);
    irs::UnsetBit(b, 5, false);
    irs::UnsetBit(b, 6, true);
    irs::UnsetBit(b, 7, false);

    EXPECT_FALSE(irs::CheckBit(b, 0));
    EXPECT_FALSE(irs::CheckBit(b, 1));
    EXPECT_FALSE(irs::CheckBit(b, 2));
    EXPECT_TRUE(irs::CheckBit(b, 3));
    EXPECT_FALSE(irs::CheckBit(b, 4));
    EXPECT_TRUE(irs::CheckBit(b, 5));
    EXPECT_FALSE(irs::CheckBit(b, 6));
    EXPECT_FALSE(irs::CheckBit(b, 7));
  }

  // test runtime
  {
    irs::byte_type b = 0x6c;

    EXPECT_FALSE(irs::CheckBit(b, 0));
    EXPECT_FALSE(irs::CheckBit(b, 1));
    EXPECT_TRUE(irs::CheckBit(b, 2));
    EXPECT_TRUE(irs::CheckBit(b, 3));
    EXPECT_FALSE(irs::CheckBit(b, 4));
    EXPECT_TRUE(irs::CheckBit(b, 5));
    EXPECT_TRUE(irs::CheckBit(b, 6));
    EXPECT_FALSE(irs::CheckBit(b, 7));

    irs::UnsetBit(b, 0, true);
    irs::UnsetBit(b, 1, false);
    irs::UnsetBit(b, 2, true);
    irs::UnsetBit(b, 3, false);
    irs::UnsetBit(b, 4, true);
    irs::UnsetBit(b, 5, false);
    irs::UnsetBit(b, 6, true);
    irs::UnsetBit(b, 7, false);

    EXPECT_FALSE(irs::CheckBit(b, 0));
    EXPECT_FALSE(irs::CheckBit(b, 1));
    EXPECT_FALSE(irs::CheckBit(b, 2));
    EXPECT_TRUE(irs::CheckBit(b, 3));
    EXPECT_FALSE(irs::CheckBit(b, 4));
    EXPECT_TRUE(irs::CheckBit(b, 5));
    EXPECT_FALSE(irs::CheckBit(b, 6));
    EXPECT_FALSE(irs::CheckBit(b, 7));
  }
}

TEST(bit_utils_test, select_in_word) {
  std::vector<uint64_t> words{
    ~uint64_t{0},          uint64_t{1},           uint64_t{1} << 63,
    0x8000000100000001ULL, 0xAAAAAAAAAAAAAAAAULL, 0x00000000FFFF0000ULL,
  };
  std::mt19937_64 rng{7};
  for (size_t i = 0; i != 10000; ++i) {
    const uint64_t a = rng();
    const uint64_t b = rng();
    words.push_back(a);
    words.push_back(a & b);
    words.push_back(a & b & rng());
    words.push_back(a | b);
  }
  for (const uint64_t word : words) {
    uint64_t rank = 0;
    for (uint64_t bit = 0; bit != 64; ++bit) {
      if (irs::CheckBit(word, bit)) {
        ASSERT_EQ(bit, irs::SelectInWord(word, rank)) << word << ' ' << rank;
        ++rank;
      }
    }
  }
}
