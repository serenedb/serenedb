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

#include <array>
#include <barrier>
#include <limits>
#include <random>
#include <thread>

#include "iresearch/utils/fm_index.hpp"

namespace irs {

TEST(FmIndexTest, MatchesBinaryScan) {
  std::mt19937 random{20261027};
  std::vector<std::string> terms(128);
  for (auto& term : terms) {
    term.resize(random() % 80);
    for (auto& c : term) {
      c = static_cast<char>(random() % 16);
    }
  }
  const std::string marker{"\0\xffneedle", 8};
  terms.emplace_back(131072, 'a');
  terms.back().replace(65530, marker.size(), marker);
  std::vector<size_t> groups;
  for (size_t i = 0; i < terms.size(); i += 7) {
    groups.emplace_back(i);
  }
  const FmIndex index{terms, groups};
  std::vector<std::string> needles{marker, "absent", std::string{"\xff", 1}};
  for (size_t query = 0; query != 200; ++query) {
    const auto& source = terms[random() % terms.size()];
    if (!source.empty()) {
      needles.emplace_back(
        source.substr(random() % source.size(), 1 + random() % 5));
    }
  }
  for (const auto& needle : needles) {
    std::vector<uint64_t> expected;
    for (size_t i = 0; i != terms.size(); ++i) {
      if (terms[i].find(needle) != std::string::npos &&
          (expected.empty() || expected.back() != i / 7)) {
        expected.emplace_back(i / 7);
      }
    }
    ASSERT_EQ(index.Find(needle, std::numeric_limits<size_t>::max()), expected);
  }
}

TEST(FmIndexTest, AlphabetBoundaryMatchesScan) {
  for (const size_t alphabet_size : {size_t{254}, size_t{255}, size_t{256}}) {
    std::string alphabet;
    for (size_t i = 256 - alphabet_size; i != 256; ++i) {
      alphabet.push_back(static_cast<char>(i));
    }
    const std::array<std::string, 4> terms{"", alphabet, alphabet + alphabet,
                                           std::string{"\xff\xfe\xff", 3}};
    const std::array<size_t, 3> groups{0, 1, 3};
    const FmIndex index{terms, groups};
    for (size_t i = 0; i != 256; ++i) {
      for (const auto& needle :
           {std::string(1, static_cast<char>(i)),
            std::string(1, static_cast<char>(i)) + '\xff'}) {
        std::vector<uint64_t> expected;
        for (size_t id = 0; id != terms.size(); ++id) {
          const uint64_t block = id == 0 ? 0 : id < 3 ? 1 : 2;
          if (terms[id].find(needle) != std::string::npos &&
              (expected.empty() || expected.back() != block)) {
            expected.push_back(block);
          }
        }
        ASSERT_EQ(index.Find(needle, 100), expected)
          << alphabet_size << ':' << i;
      }
    }
  }
}

TEST(FmIndexTest, GroupsRetainSeparators) {
  const std::array<std::string, 6> terms{
    "", "banana", "bandana", "banana banana", "mississippi", ""};
  const std::array<size_t, 3> groups{0, 3, 5};
  const FmIndex index{terms, groups};
  ASSERT_EQ(index.Find("ana", 7), (std::vector<uint64_t>{0, 1}));
  ASSERT_EQ(index.Find("issi", 7), (std::vector<uint64_t>{1}));
  ASSERT_EQ(index.Find("anaband", 7), (std::vector<uint64_t>{}));
  ASSERT_EQ(index.Find("amiss", 7), (std::vector<uint64_t>{}));
}

TEST(FmIndexTest, DenseResultsCanDeclineBeforeLocating) {
  const std::array<std::string, 2> terms{"aaaa", "aa"};
  const std::array<size_t, 1> groups{0};
  const FmIndex index{terms, groups};
  ASSERT_FALSE(index.Find("a", 5));
  ASSERT_EQ(index.Find("a", 6), (std::vector<uint64_t>{0}));
  ASSERT_EQ(index.Find("z", 0), (std::vector<uint64_t>{}));
}

TEST(FmIndexTest, ConcurrentConstructionAndSearch) {
  const std::array<std::string, 4> terms{"", "banana", "bandana",
                                         "banana banana"};
  const std::array<size_t, 2> groups{0, 2};
  const auto before = FmIndex::LiveBytes();
  {
    const FmIndex shared{terms, groups};
    std::barrier start{4};
    std::vector<std::jthread> workers;
    for (size_t i = 0; i != 4; ++i) {
      workers.emplace_back([&] {
        start.arrive_and_wait();
        const FmIndex index{terms, groups};
        ASSERT_EQ(index.Find("ana", 10), (std::vector<uint64_t>{0, 1}));
        ASSERT_EQ(shared.Find("ana", 10), (std::vector<uint64_t>{0, 1}));
        ASSERT_EQ(shared.Find("absent", 10), (std::vector<uint64_t>{}));
      });
    }
    workers.clear();
    ASSERT_EQ(FmIndex::LiveBytes(), before + shared.SizeInBytes());
  }
  ASSERT_EQ(FmIndex::LiveBytes(), before);
}

}  // namespace irs
