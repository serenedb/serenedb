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

#include <gtest/gtest.h>

#include <iresearch/search/detail/pattern_cache.hpp>
#include <string_view>

namespace {

irs::bytes_view B(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

class PatternCacheTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _capacity = Cache().Capacity();
    Cache().Clear();
    Cache().SetCapacity(irs::PatternCache::kDefaultCapacity);
  }

  void TearDown() final {
    Cache().Clear();
    Cache().SetCapacity(_capacity);
  }

  static irs::PatternCache& Cache() { return irs::PatternCache::Instance(); }

 private:
  size_t _capacity{};
};

TEST_F(PatternCacheTest, compiles_once) {
  const auto first = Cache().Get(B("a.*b"), irs::PatternKind::RegexpPerl);
  ASSERT_NE(nullptr, first);
  EXPECT_TRUE(first->ok());
  EXPECT_EQ(first, Cache().Get(B("a.*b"), irs::PatternKind::RegexpPerl));
  EXPECT_EQ(1, Cache().Size());
  EXPECT_LT(0, Cache().Bytes());
}

TEST_F(PatternCacheTest, kind_is_part_of_the_key) {
  const auto regexp = Cache().Get(B("a.c"), irs::PatternKind::RegexpPerl);
  const auto posix = Cache().Get(B("a.c"), irs::PatternKind::RegexpPosixEre);
  const auto wildcard = Cache().Get(B("a.c"), irs::PatternKind::Wildcard);
  EXPECT_NE(regexp, posix);
  EXPECT_NE(regexp, wildcard);
  EXPECT_EQ(3, Cache().Size());
  EXPECT_TRUE(regexp->Matches(B("abc")));
  EXPECT_FALSE(wildcard->Matches(B("abc")));
  EXPECT_TRUE(wildcard->Matches(B("a.c")));
}

TEST_F(PatternCacheTest, zero_capacity_disables) {
  Cache().SetCapacity(0);
  const auto first = Cache().Get(B("a.*b"), irs::PatternKind::RegexpPerl);
  const auto second = Cache().Get(B("a.*b"), irs::PatternKind::RegexpPerl);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_NE(first, second);
  EXPECT_EQ(0, Cache().Size());
  EXPECT_EQ(0, Cache().Bytes());
}

TEST_F(PatternCacheTest, evicts_least_recently_used) {
  const auto a = Cache().Get(B("a.*"), irs::PatternKind::RegexpPerl);
  const auto b = Cache().Get(B("b.*"), irs::PatternKind::RegexpPerl);
  EXPECT_EQ(a, Cache().Get(B("a.*"), irs::PatternKind::RegexpPerl));
  Cache().SetCapacity(Cache().Bytes() - 1);
  EXPECT_EQ(1, Cache().Size());
  EXPECT_EQ(a, Cache().Get(B("a.*"), irs::PatternKind::RegexpPerl));
  EXPECT_NE(b, Cache().Get(B("b.*"), irs::PatternKind::RegexpPerl));
}

TEST_F(PatternCacheTest, evicted_entry_stays_alive_for_its_users) {
  const auto held = Cache().Get(B("x[0-9]+"), irs::PatternKind::RegexpPerl);
  Cache().SetCapacity(0);
  EXPECT_EQ(0, Cache().Size());
  ASSERT_NE(nullptr, held);
  EXPECT_TRUE(held->Matches(B("x42")));
  EXPECT_FALSE(held->Matches(B("x")));
}

}  // namespace
