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

#include <algorithm>
#include <iresearch/search/detail/masked_leaf.hpp>
#include <numeric>
#include <random>
#include <vector>

#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;
using irs::block_io::FillLeaf;
using irs::detail::MaskedLeaf;

constexpr uint32_t kBlock = MaskedLeaf::kBlock;
constexpr uint32_t kBits = irs::BitsRequired<uint64_t>();
constexpr doc_id_t kPrev = 1000;
constexpr doc_id_t kPacked[kBlock]{};

struct BitsetLeaf {
  std::vector<doc_id_t> docs;
  std::vector<uint64_t> words;

  FillLeaf Fill() const {
    return {reinterpret_cast<const irs::UnalignedWord*>(words.data()),
            static_cast<uint32_t>(words.size()), docs.back(),
            FillLeaf::Kind::Bitset};
  }
};

BitsetLeaf MakeBitsetLeaf(uint32_t words, uint32_t len, std::mt19937& rng) {
  const auto span = std::uniform_int_distribution<uint32_t>{
    std::max((words - 1) * kBits + 1, len), words * kBits}(rng);
  std::vector<uint32_t> offsets(span - 1);
  std::iota(offsets.begin(), offsets.end(), 0);
  std::shuffle(offsets.begin(), offsets.end(), rng);
  offsets.resize(len - 1);
  offsets.push_back(span - 1);
  std::sort(offsets.begin(), offsets.end());
  BitsetLeaf leaf{.words = std::vector<uint64_t>(words)};
  for (const auto offset : offsets) {
    leaf.docs.push_back(kPrev + 1 + offset);
    leaf.words[offset / kBits] |= uint64_t{1} << (offset % kBits);
  }
  return leaf;
}

uint32_t Rank(const std::vector<doc_id_t>& docs, doc_id_t target) {
  const auto it = std::lower_bound(docs.begin(), docs.end(), target);
  return static_cast<uint32_t>(kBlock - docs.size() + (it - docs.begin()));
}

void ExpectFind(MaskedLeaf& leaf, const std::vector<doc_id_t>& docs,
                doc_id_t target) {
  const auto it = std::lower_bound(docs.begin(), docs.end(), target);
  ASSERT_NE(docs.end(), it);
  EXPECT_EQ(*it, leaf.Find(kPacked, kPrev, target)) << target;
  EXPECT_EQ(Rank(docs, target), leaf.Index()) << target;
}

TEST(masked_leaf_test, bitset_leaves_find_every_target) {
  std::mt19937 rng{42};
  MaskedLeaf leaf;
  for (uint32_t words = 1; words <= irs::block_codec::kMaxBitsetWords;
       ++words) {
    for (const uint32_t len : {1U, 2U, 31U, 64U, 100U, kBlock}) {
      if (len > words * kBits) {
        continue;
      }
      const auto bitset = MakeBitsetLeaf(words, len, rng);
      const auto fill = bitset.Fill();
      leaf.Reset(fill, fill.bitset, len);
      for (auto target = kPrev - 1; target <= fill.max; ++target) {
        ExpectFind(leaf, bitset.docs, target);
      }
      leaf.Reset(fill, fill.bitset, len);
      for (auto target = kPrev + 1; target <= fill.max;
           target += 1 + rng() % (2 * kBits)) {
        ExpectFind(leaf, bitset.docs, target);
      }
    }
  }
}

TEST(masked_leaf_test, run_leaves_find_every_target) {
  MaskedLeaf leaf;
  for (const uint32_t len : {1U, 2U, 63U, 64U, 65U, kBlock}) {
    std::vector<doc_id_t> docs(len);
    std::iota(docs.begin(), docs.end(), kPrev + 1);
    leaf.Reset({nullptr, 0, docs.back(), FillLeaf::Kind::Run}, nullptr, len);
    for (auto target = kPrev - 1; target <= docs.back(); ++target) {
      ExpectFind(leaf, docs, target);
    }
  }
}

TEST(masked_leaf_test, bitset_leaves_match_batches) {
  constexpr uint32_t kBatch = 32;
  std::mt19937 rng{7};
  MaskedLeaf leaf;
  for (uint32_t words = 1; words <= irs::block_codec::kMaxBitsetWords;
       ++words) {
    const auto len = std::min(kBlock, words * kBits / 2);
    const auto bitset = MakeBitsetLeaf(words, len, rng);
    const auto fill = bitset.Fill();
    std::vector<doc_id_t> targets(fill.max - kPrev);
    std::iota(targets.begin(), targets.end(), kPrev + 1);
    std::shuffle(targets.begin(), targets.end(), rng);
    targets.resize(std::min<size_t>(targets.size(), kBatch));
    std::sort(targets.begin(), targets.end());
    const auto n = static_cast<uint32_t>(targets.size());
    uint32_t hits = 0;
    uint32_t index[kBatch];
    leaf.Reset(fill, fill.bitset, len);
    const auto doc =
      leaf.Match<kBatch>(kPacked, kPrev, targets.data(), 0, n, hits, index);
    for (uint32_t i = 0; i != n; ++i) {
      const bool member =
        std::binary_search(bitset.docs.begin(), bitset.docs.end(), targets[i]);
      EXPECT_EQ(member, ((hits >> i) & 1) != 0) << targets[i];
      EXPECT_EQ(Rank(bitset.docs, targets[i]), index[i]) << targets[i];
    }
    EXPECT_EQ(
      *std::lower_bound(bitset.docs.begin(), bitset.docs.end(), targets[n - 1]),
      doc);
  }
}

}  // namespace
