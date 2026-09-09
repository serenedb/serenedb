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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/dict/stem_cache.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"

namespace {

using irs::analysis::dict::StemCache;
using irs::analysis::dict::StemUncached;

struct StemCacheTest : ::testing::Test {
  irs::stemmer_ptr stemmer{irs::make_stemmer_ptr("en", nullptr)};
  StemCache cache;

  std::optional<std::string> Cached(std::string_view word) {
    auto stemmed = cache.Stem(stemmer.get(), irs::MakeTermView(word));
    if (!stemmed) {
      return std::nullopt;
    }
    return std::string{*stemmed};
  }

  std::optional<std::string> Uncached(std::string_view word) {
    auto stemmed = StemUncached(stemmer.get(), word);
    if (!stemmed) {
      return std::nullopt;
    }
    return std::string{*stemmed};
  }
};

TEST_F(StemCacheTest, cached_matches_uncached_across_key_tiers) {
  const std::vector<std::string> words{
    "running",
    "jumps",
    "easily",
    "cats",
    "a",
    "internationalization",
    "characteristically",
    "antidisestablishmentarianism",
    "supercalifragilisticexpialidocious",
    "12345",
    "Résumé",
    "наледь",
  };
  for (const auto& word : words) {
    EXPECT_EQ(Uncached(word), Cached(word)) << word;
    EXPECT_EQ(Uncached(word), Cached(word)) << word;
  }
}

TEST_F(StemCacheTest, repeated_calls_are_stable) {
  const auto first = Cached("running");
  ASSERT_TRUE(first);
  EXPECT_EQ("run", *first);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(first, Cached("running"));
  }
}

TEST_F(StemCacheTest, tier_boundaries_round_trip) {
  for (const size_t len : {size_t{11}, size_t{12}, size_t{13}, size_t{22},
                           size_t{23}, size_t{40}}) {
    std::string word(len, 'q');
    word.front() = 'r';
    EXPECT_EQ(Uncached(word), Cached(word)) << "len=" << len;
    EXPECT_EQ(Uncached(word), Cached(word)) << "len=" << len;
  }
}

TEST_F(StemCacheTest, memory_grows_and_respects_caps) {
  EXPECT_EQ(0u, cache.MemoryBytes());
  ASSERT_TRUE(Cached("running"));
  EXPECT_GT(cache.MemoryBytes(), 0u);

  for (size_t i = 0; i < 300'000; ++i) {
    const std::string word = "w" + std::to_string(i);
    const std::string long_word = "wordingwording" + std::to_string(i);
    cache.Stem(stemmer.get(), irs::MakeTermView(std::string_view{word}));
    cache.Stem(stemmer.get(), irs::MakeTermView(std::string_view{long_word}));
  }
  EXPECT_LE(cache.MemoryBytes(), size_t{8} << 20);
  EXPECT_EQ(Uncached("running"), Cached("running"));
  EXPECT_EQ(Uncached("jumps"), Cached("jumps"));
}

}  // namespace
