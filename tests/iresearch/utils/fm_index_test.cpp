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

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "iresearch/utils/fm_index.hpp"

namespace irs {

TEST(FmIndexTest, SubstringsReturnDistinctTerms) {
  const std::array<std::string, 5> terms{"", "banana", "bandana",
                                         "banana banana", "mississippi"};
  const FmIndex index{terms};
  ASSERT_EQ(index.Size(), terms.size());
  ASSERT_EQ(index.Find("ana"), (std::vector<uint64_t>{1, 2, 3}));
  ASSERT_EQ(index.Find("issi"), (std::vector<uint64_t>{4}));
  ASSERT_EQ(index.Find("absent"), (std::vector<uint64_t>{}));
  ASSERT_EQ(index.Find(""), (std::vector<uint64_t>{0, 1, 2, 3, 4}));
  for (size_t i = 0; i != terms.size(); ++i) {
    ASSERT_EQ(index.Term(i), terms[i]);
  }
  ASSERT_GT(index.SizeInBytes(), 0);
}

TEST(FmIndexTest, PatternsCannotCrossTermBoundaries) {
  const std::array<std::string, 4> terms{"ab", "cd", "", "ef"};
  const FmIndex index{terms};
  ASSERT_EQ(index.Find("bc"), (std::vector<uint64_t>{}));
  ASSERT_EQ(index.Find("de"), (std::vector<uint64_t>{}));
}

TEST(FmIndexTest, EveryByteIsSearchable) {
  std::string alphabet;
  for (size_t i = 0; i != 256; ++i) {
    alphabet.push_back(static_cast<char>(i));
  }
  const std::array<std::string, 3> terms{"", alphabet, alphabet + alphabet};
  const FmIndex index{terms};
  ASSERT_EQ(index.Term(1), alphabet);
  ASSERT_EQ(index.Term(2), alphabet + alphabet);
  for (size_t i = 0; i != alphabet.size(); ++i) {
    const auto needle = alphabet.substr(i, 1);
    ASSERT_EQ(index.Find(needle), (std::vector<uint64_t>{1, 2})) << i;
  }
  ASSERT_EQ(index.Find(std::string{"\xff\0", 2}), (std::vector<uint64_t>{2}));
}

TEST(FmIndexTest, DenseResultsCanDeclineBeforeLocating) {
  const std::array<std::string, 2> terms{"aaaa", "aa"};
  const FmIndex index{terms};
  ASSERT_FALSE(index.Find("a", 5));
  ASSERT_EQ(index.Find("a", 6), (std::vector<uint64_t>{0, 1}));
  ASSERT_EQ(index.Find("z", 0), (std::vector<uint64_t>{}));
}

TEST(FmIndexTest, EmptyCollection) {
  const FmIndex index{std::span<const std::string>{}};
  ASSERT_EQ(index.Size(), 0);
  ASSERT_EQ(index.Find("a"), (std::vector<uint64_t>{}));
  ASSERT_EQ(index.Find(""), (std::vector<uint64_t>{}));
}

TEST(FmIndexTest, LargeBinaryTermsAreLossless) {
  std::string large(131072, 'a');
  const std::string marker{"\0\xffneedle", 8};
  large.replace(65530, marker.size(), marker);
  const std::array<std::string, 3> terms{"", large,
                                         "prefix" + large + "suffix"};
  const FmIndex index{terms};
  ASSERT_EQ(index.Find(marker), (std::vector<uint64_t>{1, 2}));
  for (size_t i = 0; i != terms.size(); ++i) {
    ASSERT_EQ(index.Term(i), terms[i]);
  }
}

TEST(FmIndexTest, MatchesBinaryScan) {
  std::mt19937 random{20261027};
  std::vector<std::string> terms(128);
  for (auto& term : terms) {
    term.resize(random() % 80);
    for (auto& c : term) {
      c = static_cast<char>(random() % 16);
    }
  }
  const FmIndex index{terms};
  for (size_t i = 0; i != terms.size(); ++i) {
    ASSERT_EQ(index.Term(i), terms[i]);
  }
  for (size_t query = 0; query != 200; ++query) {
    const auto& source = terms[random() % terms.size()];
    const auto needle =
      source.substr(random() % (source.size() + 1), 1 + random() % 5);
    std::vector<uint64_t> expected;
    for (size_t i = 0; i != terms.size(); ++i) {
      if (terms[i].find(needle) != std::string::npos) {
        expected.push_back(i);
      }
    }
    ASSERT_EQ(index.Find(needle), expected) << query;
  }
}

TEST(FmIndexTest, ConcurrentBuildersHaveUniqueTemporaryNames) {
  constexpr size_t kWorkers = 8;
  constexpr size_t kNames = 32768;
  std::array<std::vector<uint64_t>, kWorkers> names;
  std::barrier start{kWorkers};
  std::vector<std::jthread> workers;
  for (auto& part : names) {
    part.resize(kNames);
    workers.emplace_back([&part, &start] {
      start.arrive_and_wait();
      for (auto& name : part) {
        name = sdsl::util::id();
      }
    });
  }
  workers.clear();
  std::vector<uint64_t> all;
  all.reserve(kWorkers * kNames);
  for (const auto& part : names) {
    all.insert(all.end(), part.begin(), part.end());
  }
  absl::c_sort(all);
  ASSERT_EQ(absl::c_adjacent_find(all), all.end());
}

TEST(FmIndexTest, ConcurrentConstructionAndSearch) {
  const std::array<std::string, 4> terms{"", "banana", "bandana",
                                         "banana banana"};
  const auto before = FmIndex::LiveBytes();
  const FmIndex shared{terms};
  const auto shared_bytes = FmIndex::LiveBytes();
  std::barrier start{4};
  std::vector<std::jthread> workers;
  for (size_t i = 0; i != 4; ++i) {
    workers.emplace_back([&] {
      start.arrive_and_wait();
      const FmIndex index{terms};
      ASSERT_EQ(index.Find("ana"), (std::vector<uint64_t>{1, 2, 3}));
      for (size_t id = 0; id != terms.size(); ++id) {
        ASSERT_EQ(index.Term(id), terms[id]);
        ASSERT_EQ(shared.Term(id), terms[id]);
      }
    });
  }
  workers.clear();
  ASSERT_EQ(FmIndex::LiveBytes(), shared_bytes);
  ASSERT_GT(shared_bytes, before);
}

}  // namespace irs
