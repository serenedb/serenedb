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

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/analysis/stopword_set.hpp"
#include "iresearch/analysis/text/dict/string_table.hpp"

namespace {

using irs::analysis::StopwordSet;

using StringSet = irs::analysis::dict::StringSet<std::string>;

template<typename Mapped>
using StringMap = irs::analysis::dict::StringMap<std::string_view, Mapped>;

constexpr std::string_view kInlineMax = "abcdefghijkl";
constexpr std::string_view kLong = "abcdefghijklm";
constexpr std::string_view kLonger = "the-quick-brown-fox";
constexpr std::string_view kBeyondSso = "the-quick-brown-fox-jumps-over";

duckdb::string_t Term(std::string_view value) {
  return {value.data(), static_cast<uint32_t>(value.size())};
}

template<typename Table>
std::string HashOf(const Table& table) {
  char hex[duckdb::MD5Context::MD5_HASH_LENGTH_TEXT];
  table.Hash(hex);
  return {hex, sizeof(hex)};
}

TEST(string_set_tests, empty) {
  const StringSet set;
  EXPECT_TRUE(set.Empty());
  EXPECT_FALSE(set.Contains(std::string_view{}));
  EXPECT_FALSE(set.Contains(std::string_view{"a"}));
  EXPECT_FALSE(set.Contains(kLong));
  EXPECT_FALSE(set.Contains(Term("a")));
  EXPECT_FALSE(set.Contains(Term(kLong)));
}

TEST(string_set_tests, inline_words) {
  StringSet set;
  set.Insert("a");
  set.Insert("the");
  set.Insert(std::string{kInlineMax});
  set.Insert("");

  for (const std::string_view word :
       {std::string_view{"a"}, std::string_view{"the"}, kInlineMax,
        std::string_view{""}}) {
    EXPECT_TRUE(set.Contains(word)) << word;
    EXPECT_TRUE(set.Contains(Term(word))) << word;
  }

  EXPECT_FALSE(set.Contains(std::string_view{"b"}));
  EXPECT_FALSE(set.Contains(std::string_view{"th"}));
  EXPECT_FALSE(set.Contains(std::string_view{"thee"}));
  EXPECT_FALSE(set.Contains(kLong));
  EXPECT_FALSE(set.Contains(Term(kLong)));
}

TEST(string_set_tests, long_words_own_their_bytes) {
  StringSet set;
  {
    const std::string transient{kLonger};
    set.Insert(transient);
    set.Insert(transient);
  }
  {
    const std::string transient{kBeyondSso};
    set.Insert(transient);
  }
  set.Insert(std::string{kLong});
  set.Insert(std::string{kLonger.substr(0, 12)});

  EXPECT_TRUE(set.Contains(kLonger));
  EXPECT_TRUE(set.Contains(Term(kLonger)));
  EXPECT_TRUE(set.Contains(kBeyondSso));
  EXPECT_TRUE(set.Contains(Term(kBeyondSso)));
  EXPECT_TRUE(set.Contains(kLong));
  EXPECT_TRUE(set.Contains(kLonger.substr(0, 12)));

  EXPECT_FALSE(set.Contains(kLong.substr(0, 12)));
  EXPECT_FALSE(set.Contains(kLonger.substr(0, 13)));
  EXPECT_FALSE(set.Contains(kBeyondSso.substr(0, 29)));
}

TEST(string_set_tests, reserve_prevents_rehash) {
  StringSet set;
  set.Reserve(64, 1);
  const auto reserved = set.MemoryBytes();
  ASSERT_GT(reserved, 0u);
  for (size_t i = 0; i < 64; ++i) {
    set.Insert(std::string{reinterpret_cast<const char*>(&i), sizeof(i)});
  }
  set.Insert(std::string{kLong});
  EXPECT_EQ(reserved + kLong.size(), set.MemoryBytes());
}

TEST(string_set_tests, equality) {
  StringSet a;
  a.Insert("the");
  a.Insert(std::string{kLong});
  a.Insert("the");

  StringSet b;
  b.Insert(std::string{kLong});
  b.Insert("the");

  EXPECT_TRUE(a == b);

  StringSet c;
  c.Insert("the");
  EXPECT_FALSE(a == c);

  EXPECT_GT(a.MemoryBytes(), 0u);
}

TEST(string_table_tests, hash_is_content_keyed) {
  StringSet a;
  a.Insert("the");
  a.Insert(std::string{kLonger});

  StringSet b;
  b.Reserve(32, 4);
  b.Insert(std::string{kLonger});
  b.Insert("the");
  b.Insert("the");

  EXPECT_EQ(HashOf(a), HashOf(b));

  StringSet ab_c;
  ab_c.Insert("ab");
  ab_c.Insert("c");
  StringSet a_bc;
  a_bc.Insert("a");
  a_bc.Insert("bc");
  EXPECT_NE(HashOf(ab_c), HashOf(a_bc));

  StringMap<int> x;
  x["the"] = 1;
  x[kLonger] = 2;
  StringMap<int> y;
  y[kLonger] = 7;
  y["the"] = 8;
  EXPECT_EQ(HashOf(x), HashOf(y));
  EXPECT_EQ(HashOf(a), HashOf(x));
}

TEST(string_map_tests, get_or_insert) {
  StringMap<int> map;
  EXPECT_EQ(nullptr, map.Find(std::string_view{"one"}));

  map["one"] = 1;
  map[kLong] = 2;

  ASSERT_NE(nullptr, map.Find(std::string_view{"one"}));
  EXPECT_EQ(1, *map.Find(std::string_view{"one"}));
  EXPECT_EQ(1, *map.Find(Term("one")));
  ASSERT_NE(nullptr, map.Find(kLong));
  EXPECT_EQ(2, *map.Find(kLong));
  EXPECT_EQ(2, *map.Find(Term(kLong)));

  map["one"] = 3;
  EXPECT_EQ(3, *map.Find(std::string_view{"one"}));

  EXPECT_EQ(nullptr, map.Find(std::string_view{"two"}));
  EXPECT_EQ(nullptr, map.Find(kLonger));
  EXPECT_EQ(nullptr, map.Find(Term(kLonger)));
}

TEST(string_map_tests, fold_and_visit) {
  StringMap<std::vector<int>> map;
  map["word"].push_back(2);
  map[kLong].push_back(3);
  map["word"].push_back(1);
  map[kLong].push_back(4);

  ASSERT_NE(nullptr, map.Find(kLong));
  EXPECT_EQ((std::vector<int>{3, 4}), *map.Find(kLong));

  map.ForEachMapped(
    [](std::vector<int>& values) { std::sort(values.begin(), values.end()); });
  EXPECT_EQ((std::vector<int>{1, 2}), *map.Find(std::string_view{"word"}));

  size_t count = 0;
  std::as_const(map).ForEachMapped([&](const std::vector<int>&) { ++count; });
  EXPECT_EQ(2u, count);
}

TEST(string_map_tests, equality) {
  StringMap<int> a;
  a["one"] = 1;
  a[kLong] = 2;

  StringMap<int> b;
  b[kLong] = 2;
  b["one"] = 1;

  EXPECT_TRUE(a == b);

  b["one"] = 5;
  EXPECT_FALSE(a == b);
}

TEST(string_map_tests, size_and_erase_half) {
  StringMap<int> map;
  EXPECT_EQ(0u, map.Size());
  map.EraseHalf();
  EXPECT_EQ(0u, map.Size());

  const std::vector<std::string_view> keys{
    "one", "two",   "three",    "four",
    kLong, kLonger, kBeyondSso, "another-long-key-here"};
  for (size_t i = 0; i < keys.size(); ++i) {
    map[keys[i]] = static_cast<int>(i);
  }
  EXPECT_EQ(keys.size(), map.Size());

  map.EraseHalf();
  EXPECT_EQ(keys.size() / 2, map.Size());
  size_t found = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (const auto* value = map.Find(keys[i])) {
      EXPECT_EQ(static_cast<int>(i), *value);
      ++found;
    }
  }
  EXPECT_EQ(keys.size() / 2, found);
}

TEST(stopword_set_tests, owns_long_words) {
  std::vector<std::string> words{"a", "the", std::string{kLong},
                                 std::string{kBeyondSso}};
  const StopwordSet set{words};
  words.clear();

  EXPECT_TRUE(set.Contains(std::string_view{"a"}));
  EXPECT_TRUE(set.Contains(std::string_view{"the"}));
  EXPECT_TRUE(set.Contains(kLong));
  EXPECT_TRUE(set.Contains(kBeyondSso));
  EXPECT_TRUE(set.Contains(Term(kLong)));
  EXPECT_FALSE(set.Contains(std::string_view{"fox"}));
  EXPECT_FALSE(set.Contains(kBeyondSso.substr(0, 13)));
  EXPECT_GT(set.GetEstimatedCacheMemory().GetIndex(),
            kLong.size() + kBeyondSso.size());
}

}  // namespace
