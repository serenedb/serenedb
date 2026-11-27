////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/utils/minhash_utils.hpp>

#include "tests_shared.hpp"

namespace {

size_t SignatureSize(const irs::MinHash& mh) {
  return static_cast<size_t>(std::distance(std::begin(mh), std::end(mh)));
}

}  // namespace

TEST(MinHashTest, ConstructEmpty) {
  irs::MinHash mh{0};
  ASSERT_EQ(std::begin(mh), std::end(mh));
  mh.Insert(5);
  mh.Insert(3);
  ASSERT_EQ(std::vector<uint64_t>{3},
            std::vector<uint64_t>(std::begin(mh), std::end(mh)));
}

TEST(MinHashTest, InsertClear) {
  constexpr size_t kNumHashes = 42;

  irs::MinHash mh{kNumHashes};
  ASSERT_EQ(std::begin(mh), std::end(mh));

  // Insert 100 values
  {
    constexpr size_t kMax = 100;

    for (size_t i = 0; i < kMax; ++i) {
      mh.Insert(i);
      const size_t size = SignatureSize(mh);

      // Duplicates are ignored
      mh.Insert(i);
      ASSERT_EQ(size, SignatureSize(mh));
    }

    ASSERT_EQ(kNumHashes, SignatureSize(mh));

    std::vector signature(std::begin(mh), std::end(mh));
    std::sort(std::begin(signature), std::end(signature));

    auto begin = std::begin(signature);
    for (size_t i = 0; i < kNumHashes; ++i, ++begin) {
      ASSERT_EQ(i, *begin);
    }
    ASSERT_EQ(begin, std::end(signature));
  }

  // Reset state
  mh.Clear();
  ASSERT_EQ(std::begin(mh), std::end(mh));

  // Insert 27 values
  {
    constexpr size_t kMax = 27;

    for (size_t i = 0; i < kMax; ++i) {
      mh.Insert(i);
      const size_t size = SignatureSize(mh);

      // Duplicates are ignored
      mh.Insert(i);
      ASSERT_EQ(size, SignatureSize(mh));
    }

    ASSERT_EQ(kMax, SignatureSize(mh));

    std::vector signature(std::begin(mh), std::end(mh));
    std::sort(std::begin(signature), std::end(signature));

    auto begin = std::begin(signature);
    for (size_t i = 0; i < kMax; ++i, ++begin) {
      ASSERT_EQ(i, *begin);
    }
    ASSERT_EQ(begin, std::end(signature));
  }
}
