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

// `doc_limits::kDocsSlack` is the number of doc_id_t slots every buffer fed to
// `MaterializeWord` must reserve past `kBlockSize`, because both branches of
// that function write past the doc count they return:
//
//   * AVX2 ends with an unconditional 32-byte store at `out + count`, so it
//     always writes 8 slots past the end.
//   * The scalar branch pads up to the next multiple of 8, so it writes
//     `roundup8(count) - count` slots -- 0 to 7 -- past the end.
//
// The padding is not slop: `scored::WindowDisjunction` reads it back, scoring
// `out[i]` up to `first + roundup8(n - first)`.
//
// A tail block is decoded right-aligned -- `ReadTailDelta` writes at
// `out + (kBlockSize - len)` -- so the last word's padding starts at
// `kBlockSize - count` and runs to `kBlockSize - count + roundup8(count)`.
// Whenever `count % 8 != 0` that crosses the end of the block, and in
// `PostingLeaf` the very next member is `IndexInput::ptr _in`.
//
// These tests measure how far `MaterializeWord` actually writes, into a buffer
// with room to spare, and compare that against `kDocsSlack`. They fail if the
// constant is ever set below what the function needs -- on any architecture,
// which matters because the scalar branch is only compiled on non-AVX2 targets
// and CI builds linux-amd64 (`-mavx2`) by default.

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

constexpr uint32_t kCanary = 0xDEADBEEF;
constexpr size_t kProbe = 64;
constexpr uint32_t kBase = 4300;

// Slots written past kBlockSize when `word` is materialized the way a tail
// block is: right-aligned so its last doc lands on the final slot.
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
  return end <= irs::doc_limits::kBlockSize
           ? 0
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
