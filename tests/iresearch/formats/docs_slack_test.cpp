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

#include <bit>
#include <cstdint>
#include <initializer_list>
#include <vector>

#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

constexpr uint32_t kCanary = 0xDEADBEEF;
constexpr size_t kProbe = 64;
constexpr uint32_t kBase = 4300;

size_t OverrunOfTail(uint64_t word) {
  const auto count = static_cast<size_t>(std::popcount(word));
  EXPECT_LE(count, irs::doc_limits::kBlockSize);

  std::vector<uint32_t> buf(irs::doc_limits::kBlockSize + kProbe, kCanary);
  auto* const begin = buf.data() + (irs::doc_limits::kBlockSize - count);
  EXPECT_EQ(count, static_cast<size_t>(
                     irs::MaterializeWord(kBase, word, begin) - begin));

  size_t end = 0;
  for (size_t i = 0; i != buf.size(); ++i) {
    if (buf[i] != kCanary) {
      end = i + 1;
    }
  }
  EXPECT_LE(end, buf.size()) << "probe area too small to bound the write";
  return end <= irs::doc_limits::kBlockSize ? 0
                                            : end - irs::doc_limits::kBlockSize;
}

uint64_t WordWithBits(std::initializer_list<uint32_t> bits) {
  uint64_t word = 0;
  for (const auto bit : bits) {
    word |= uint64_t{1} << bit;
  }
  return word;
}

TEST(DocsSlackTest, TailWithUnalignedPopcountWritesPastBlock) {
  const auto overrun = OverrunOfTail(WordWithBits({0, 3, 7, 11, 20}));

  EXPECT_NE(0, overrun) << "MaterializeWord is expected to overrun; if it no "
                           "longer does, kDocsSlack can be revisited";
  EXPECT_LE(overrun, irs::doc_limits::kDocsSlack);
}

TEST(DocsSlackTest, EveryPopcountFitsDocsSlack) {
  for (uint32_t count = 1; count <= 64; ++count) {
    uint64_t word = 0;
    for (uint32_t i = 0; i != count; ++i) {
      word |= uint64_t{1} << i;
    }
    EXPECT_LE(OverrunOfTail(word), irs::doc_limits::kDocsSlack)
      << "popcount=" << count;
  }
}

}  // namespace
