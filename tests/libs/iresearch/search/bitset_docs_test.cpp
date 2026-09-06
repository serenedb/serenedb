////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <algorithm>
#include <bit>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/memory.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/search/common/lazy_bitset.hpp"
#include "iresearch/search/docs/bitset.hpp"
#include "iresearch/search/fill/bitset_docs.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/lead/bitset_docs.hpp"
#include "iresearch/search/probe/bitset_docs.hpp"
#include "tests_shared.hpp"

namespace {

constexpr auto kBits = irs::search::BitsetStorage::kBits;

// A segment of `docs_count` documents holding exactly `docs`. Bit `i` is the
// document `i`, so a document is its own index and zero -- which is not a
// document -- is never set.
irs::search::BitsetStorage MakeSet(irs::doc_id_t docs_count,
                                   const std::vector<irs::doc_id_t>& docs) {
  irs::search::BitsetStorage set{docs_count};
  auto* words = set.Words();
  for (auto doc : docs) {
    EXPECT_TRUE(irs::doc_limits::valid(doc));
    EXPECT_LE(doc, docs_count);
    irs::SetBit(words[doc / kBits], doc % kBits);
  }
  set.Trim();
  return set;
}

std::vector<irs::doc_id_t> Range(irs::doc_id_t first, irs::doc_id_t last,
                                 irs::doc_id_t step = 1) {
  std::vector<irs::doc_id_t> docs;
  for (auto doc = first; doc <= last; doc += step) {
    docs.emplace_back(doc);
  }
  return docs;
}

std::vector<irs::doc_id_t> Drain(irs::lead::BitsetDocs& it) {
  std::vector<irs::doc_id_t> docs;
  while (!irs::doc_limits::eof(it.Advance())) {
    docs.emplace_back(it.Value());
  }
  return docs;
}

// Every document the root emits, driven the way a consumer drives it.
std::vector<irs::doc_id_t> Emit(irs::docs::Root& root, uint32_t capacity) {
  std::vector<irs::doc_id_t> out(capacity + irs::doc_limits::kDocsSlack);
  std::vector<irs::doc_id_t> docs;
  for (;;) {
    const auto n = root.Run(out.data(), capacity);
    if (n == 0) {
      break;
    }
    EXPECT_LE(n, capacity);
    docs.insert(docs.end(), out.begin(), out.begin() + n);
  }
  return docs;
}

// A fill node over a set, counting the windows it was asked to open. What a
// `LazyBitset` is filled from is a clause; this one is a set that already
// knows every answer, so what the count proves is how far the questions
// reached and not what they found.
class WindowFill : public irs::fill::Node {
 public:
  explicit WindowFill(irs::search::BitsetStorage&& set) noexcept
    : _set{std::move(set)} {}

  irs::doc_id_t FillOr(irs::doc_id_t min, irs::doc_id_t max,
                       uint64_t* IRS_RESTRICT mask) final {
    ++_windows;
    return _set.FillOr(min, max, mask);
  }

  irs::doc_id_t FillAnd(irs::doc_id_t min, irs::doc_id_t max,
                        uint64_t* IRS_RESTRICT mask) final {
    ++_windows;
    return _set.FillAnd(min, max, mask);
  }

  irs::doc_id_t FillAndNot(irs::doc_id_t min, irs::doc_id_t max,
                           uint64_t* IRS_RESTRICT mask) final {
    ++_windows;
    return _set.FillAndNot(min, max, mask);
  }

  size_t windows() const noexcept { return _windows; }

 private:
  irs::fill::BitsetDocs _set;
  size_t _windows = 0;
};

}  // namespace

TEST(bitset_lead_test, advance) {
  // empty segment
  {
    auto set = MakeSet(0, {});
    ASSERT_EQ(0, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // non-empty segment holding nothing
  {
    auto set = MakeSet(13, {});
    ASSERT_EQ(0, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense
  {
    const auto expected = Range(1, 73);
    auto set = MakeSet(73, expected);
    ASSERT_EQ(73, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // sparse: every second document
  {
    const auto expected = Range(1, 175, 2);
    auto set = MakeSet(176, expected);
    ASSERT_EQ(88, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // sparse with a dense region
  {
    auto expected = Range(64, 126);
    expected.emplace_back(191);
    auto set = MakeSet(192, expected);
    ASSERT_EQ(64, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // sparse with a sparse region
  {
    const std::vector<irs::doc_id_t> expected{71,  74,  82,  86,  93,
                                              101, 103, 113, 121, 126};
    auto set = MakeSet(173, expected);
    ASSERT_EQ(10, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // one document, in the last word
  {
    const std::vector<irs::doc_id_t> expected{185};
    auto set = MakeSet(189, expected);
    ASSERT_EQ(1, irs::search::CountBits(set));
    irs::lead::BitsetDocs it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }
}

TEST(bitset_lead_test, seek) {
  // empty segment
  {
    irs::lead::BitsetDocs it{MakeSet(0, {})};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // non-empty segment holding nothing
  {
    irs::lead::BitsetDocs it{MakeSet(13, {})};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // dense, ascending targets
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    for (irs::doc_id_t expected = 1; expected <= 173; ++expected) {
      ASSERT_EQ(expected, it.Seek(expected));
      ASSERT_EQ(expected, it.Value());
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense, a target at or below where it stands is where it stays
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};

    ASSERT_EQ(100, it.Seek(100));
    for (irs::doc_id_t target = 100; target != 0; --target) {
      ASSERT_EQ(100, it.Seek(target));
      ASSERT_EQ(100, it.Value());
    }
    ASSERT_EQ(101, it.Advance());
  }

  // dense, seek past the last document
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(174)));
  }

  // dense, seek to the last document
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};
    ASSERT_EQ(173, it.Seek(173));
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
  }

  // dense, seek to 'eof'
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(irs::doc_limits::eof())));
  }

  // dense, seek before the first document
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Seek(irs::doc_limits::invalid()));
    ASSERT_EQ(1, it.Advance());
  }

  // sparse: a target on a document nobody holds lands on the next one
  {
    irs::lead::BitsetDocs it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(1, it.Seek(1));
    for (irs::doc_id_t expected = 3; expected < 176; expected += 2) {
      ASSERT_EQ(expected, it.Seek(expected - 1));
      ASSERT_EQ(expected, it.Value());
      ASSERT_EQ(expected, it.Seek(expected));
      ASSERT_EQ(expected, it.Value());
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // sparse, a target at or below where it stands is where it stays
  {
    irs::lead::BitsetDocs it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(101, it.Seek(100));
    for (irs::doc_id_t target = 101; target != 0; --target) {
      ASSERT_EQ(101, it.Seek(target));
      ASSERT_EQ(101, it.Value());
    }
    ASSERT_EQ(103, it.Advance());
  }

  // sparse with a dense region
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(64, 126))};

    const std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {64, 43},
      {64, 43},
      {64, 64},
      {68, 68},
      {78, 78},
      {irs::doc_limits::eof(), 128},
      {irs::doc_limits::eof(), irs::doc_limits::eof()}};

    for (auto& [expected, target] : seeks) {
      ASSERT_EQ(expected, it.Seek(target));
      ASSERT_EQ(expected, it.Value());
    }
  }

  // sparse with a sparse region
  {
    irs::lead::BitsetDocs it{
      MakeSet(173, {71, 74, 82, 86, 93, 101, 103, 113, 121, 126})};

    const std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {71, 70},
      {74, 72},
      {126, 125},
      {irs::doc_limits::eof(), 128},
      {irs::doc_limits::eof(), irs::doc_limits::eof()}};

    for (auto& [expected, target] : seeks) {
      ASSERT_EQ(expected, it.Seek(target));
      ASSERT_EQ(expected, it.Value());
    }
  }

  // a target past the last document of the last word
  {
    irs::lead::BitsetDocs it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  {
    irs::lead::BitsetDocs it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_EQ(186, it.Seek(186));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }

  {
    irs::lead::BitsetDocs it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_EQ(182, it.Seek(181));
    ASSERT_EQ(186, it.Seek(186));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }

  // a target that crosses two empty words
  {
    irs::lead::BitsetDocs it{MakeSet(189, {185})};
    ASSERT_EQ(185, it.Seek(2));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }
}

TEST(bitset_lead_test, seek_advance) {
  constexpr irs::doc_id_t kSteps = 5;

  // dense
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};

    for (irs::doc_id_t target = 1; target <= 173; target += kSteps + 1) {
      ASSERT_EQ(target, it.Seek(target));
      ASSERT_EQ(target, it.Value());

      for (irs::doc_id_t j = 1;
           j <= kSteps && !irs::doc_limits::eof(it.Advance()); ++j) {
        ASSERT_EQ(target + j, it.Value());
      }
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense, a target below where it stands leaves the walk where it was
  {
    irs::lead::BitsetDocs it{MakeSet(173, Range(1, 173))};

    ASSERT_EQ(50, it.Seek(50));
    for (irs::doc_id_t j = 1; j <= kSteps; ++j) {
      ASSERT_EQ(50 + j, it.Advance());
    }
    ASSERT_EQ(50 + kSteps, it.Seek(3));
    ASSERT_EQ(50 + kSteps + 1, it.Advance());
  }

  // sparse: every second document
  {
    irs::lead::BitsetDocs it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(1, it.Seek(1));
    for (irs::doc_id_t target = 3; target <= 176; target += 2 * (kSteps + 1)) {
      ASSERT_EQ(target, it.Seek(target - 1));
      ASSERT_EQ(target, it.Value());

      for (irs::doc_id_t j = 1;
           j <= kSteps && !irs::doc_limits::eof(it.Advance()); ++j) {
        ASSERT_EQ(target + 2 * j, it.Value());
      }
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // sparse with a sparse region
  {
    irs::lead::BitsetDocs it{
      MakeSet(189, {71, 74, 82, 86, 93, 101, 103, 113, 121, 126, 182, 186})};

    ASSERT_EQ(71, it.Seek(68));
    ASSERT_EQ(74, it.Advance());
    ASSERT_EQ(82, it.Advance());
    ASSERT_EQ(86, it.Advance());
    ASSERT_EQ(182, it.Seek(181));
    ASSERT_EQ(186, it.Advance());
    ASSERT_TRUE(irs::doc_limits::eof(it.Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }
}

// A probe asks whether the node holds one document rather than walking to the
// next one it does. It answers out of the target's own word: the document
// itself when it is held, otherwise the next one that word holds, and the
// first document of the next word when it holds none at all. So an answer
// above the target is a bound and not necessarily a match -- what it
// guarantees is that nothing between the two is held.
TEST(bitset_probe_test, probe) {
  // empty segment: the only word holds nothing, and there is no word past it
  {
    irs::probe::BitsetDocs it{MakeSet(0, {})};
    ASSERT_EQ(kBits, it.Probe(1));
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(kBits)));
  }

  // one document, then the words past it
  {
    irs::probe::BitsetDocs it{MakeSet(128, {7})};
    ASSERT_EQ(7, it.Probe(7));
    ASSERT_EQ(kBits, it.Probe(8));
    ASSERT_EQ(2 * kBits, it.Probe(kBits));
    ASSERT_EQ(3 * kBits, it.Probe(2 * kBits));
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(3 * kBits)));
  }

  // a miss inside the word answers with the next document that word holds
  {
    irs::probe::BitsetDocs it{MakeSet(128, {3, 7, 13, 30})};
    ASSERT_EQ(7, it.Probe(7));
    ASSERT_EQ(13, it.Probe(8));
    ASSERT_EQ(30, it.Probe(14));
    ASSERT_EQ(kBits, it.Probe(31));
  }

  // nothing is remembered: the same question has the same answer
  {
    irs::probe::BitsetDocs it{MakeSet(128, {1, 5, 17})};
    for (int i = 0; i != 8; ++i) {
      ASSERT_EQ(5, it.Probe(5));
      ASSERT_EQ(5, it.Probe(2));
      ASSERT_EQ(17, it.Probe(17));
      ASSERT_EQ(1, it.Probe(1));
    }
  }

  // bit zero of the second word: the widest shift the hit path takes
  {
    irs::probe::BitsetDocs it{MakeSet(192, {1, 64, 65, 90})};
    ASSERT_EQ(1, it.Probe(1));
    ASSERT_EQ(64, it.Probe(64));
    ASSERT_EQ(65, it.Probe(65));
    ASSERT_EQ(90, it.Probe(66));
    ASSERT_EQ(2 * kBits, it.Probe(91));
  }

  // bit sixty-three of the first word, hit and miss
  {
    irs::probe::BitsetDocs it{MakeSet(192, {1, 63, 64, 65})};
    ASSERT_EQ(63, it.Probe(63));
    ASSERT_EQ(64, it.Probe(64));
  }
  {
    irs::probe::BitsetDocs it{MakeSet(192, {1, 60, 64})};
    ASSERT_EQ(60, it.Probe(60));
    ASSERT_EQ(kBits, it.Probe(63));
    ASSERT_EQ(64, it.Probe(64));
  }

  // consecutive documents of one word
  {
    irs::probe::BitsetDocs it{MakeSet(128, {5, 12, 13, 25})};
    ASSERT_EQ(5, it.Probe(5));
    ASSERT_EQ(12, it.Probe(12));
    ASSERT_EQ(13, it.Probe(13));
    ASSERT_EQ(25, it.Probe(25));
  }

  // a miss at one document, a hit at the next
  {
    irs::probe::BitsetDocs it{MakeSet(128, {1, 5, 6, 50})};
    ASSERT_EQ(5, it.Probe(4));
    ASSERT_EQ(5, it.Probe(5));
    ASSERT_EQ(6, it.Probe(6));
    ASSERT_EQ(50, it.Probe(7));
  }

  // a word holding nothing is stepped over one word at a time
  {
    irs::probe::BitsetDocs it{MakeSet(256, {3, 200})};
    ASSERT_EQ(3, it.Probe(3));
    ASSERT_EQ(2 * kBits, it.Probe(100));
    ASSERT_EQ(3 * kBits, it.Probe(2 * kBits));
    ASSERT_EQ(200, it.Probe(3 * kBits));
  }

  // past every document, and then past every word
  {
    irs::probe::BitsetDocs it{MakeSet(128, {1, 50, 90})};
    ASSERT_EQ(90, it.Probe(90));
    ASSERT_EQ(2 * kBits, it.Probe(91));
    ASSERT_EQ(3 * kBits, it.Probe(2 * kBits));
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(3 * kBits)));
  }

  // a word holding every document
  {
    irs::probe::BitsetDocs it{MakeSet(128, Range(1, 63))};
    for (irs::doc_id_t target = 1; target != kBits; ++target) {
      ASSERT_EQ(target, it.Probe(target));
    }
  }

  // a word holding every second document
  {
    irs::probe::BitsetDocs it{MakeSet(128, Range(2, 126, 2))};
    ASSERT_EQ(2, it.Probe(2));
    ASSERT_EQ(4, it.Probe(3));
    ASSERT_EQ(4, it.Probe(4));
    ASSERT_EQ(6, it.Probe(5));
    ASSERT_EQ(64, it.Probe(64));
    ASSERT_EQ(66, it.Probe(66));
  }
}

// The two guarantees a bound has to keep, over every target of a segment:
// it never stands below the target, and it never stands above the document
// the node would have walked to.
TEST(bitset_probe_test, never_skips_a_document) {
  constexpr irs::doc_id_t kDocs = 192;
  const std::vector<irs::doc_id_t> docs{1,  5,   9,   13,  64,
                                        68, 100, 127, 130, 180};

  const auto reference = MakeSet(kDocs, docs);
  irs::probe::BitsetDocs it{MakeSet(kDocs, docs)};

  for (irs::doc_id_t target = 1; target <= kDocs; ++target) {
    const auto next = irs::search::NextBit(reference, target);
    const auto bound = it.Probe(target);

    ASSERT_GE(bound, target);
    ASSERT_LE(bound, next);
    if (std::binary_search(docs.begin(), docs.end(), target)) {
      ASSERT_EQ(target, bound);
    }
    if (std::binary_search(docs.begin(), docs.end(), bound)) {
      ASSERT_EQ(next, bound);
    }
  }
}

TEST(bitset_fill_test, fill_or) {
  irs::fill::BitsetDocs it{MakeSet(192, {3, 70, 130})};

  uint64_t mask[2]{};
  ASSERT_EQ(130, it.FillOr(1, 100, mask));
  EXPECT_TRUE(irs::CheckBit(mask[0], 3 - 1));
  EXPECT_TRUE(irs::CheckBit(mask[1], 70 - 1 - kBits));
  EXPECT_EQ(2, std::popcount(mask[0]) + std::popcount(mask[1]));

  // The window it already stands past is not opened again.
  mask[0] = 0;
  mask[1] = 0;
  ASSERT_EQ(130, it.FillOr(100, 130, mask));
  EXPECT_EQ(0, std::popcount(mask[0]) + std::popcount(mask[1]));
}

TEST(bitset_fill_test, fill_and) {
  irs::fill::BitsetDocs it{MakeSet(192, {3, 70, 130})};

  uint64_t mask[2]{~uint64_t{0}, ~uint64_t{0}};
  ASSERT_EQ(130, it.FillAnd(1, 129, mask));
  EXPECT_TRUE(irs::CheckBit(mask[0], 3 - 1));
  EXPECT_TRUE(irs::CheckBit(mask[1], 70 - 1 - kBits));
  EXPECT_EQ(2, std::popcount(mask[0]) + std::popcount(mask[1]));
}

TEST(bitset_fill_test, fill_and_not) {
  irs::fill::BitsetDocs it{MakeSet(192, {3, 70, 130})};

  uint64_t mask[2]{~uint64_t{0}, ~uint64_t{0}};
  ASSERT_EQ(130, it.FillAndNot(1, 129, mask));
  EXPECT_FALSE(irs::CheckBit(mask[0], 3 - 1));
  EXPECT_FALSE(irs::CheckBit(mask[1], 70 - 1 - kBits));
  EXPECT_EQ(2 * kBits - 2, std::popcount(mask[0]) + std::popcount(mask[1]));
}

TEST(bitset_docs_test, run) {
  // one batch, and then nothing
  {
    const std::vector<irs::doc_id_t> expected{3, 70, 130, 4095};
    irs::docs::Bitset root{MakeSet(4096, expected)};
    ASSERT_EQ(expected, Emit(root, irs::doc_limits::kMinCapacity));
  }

  // more documents than one batch holds: a word is never split across two
  {
    const auto expected = Range(1, 300);
    irs::docs::Bitset root{MakeSet(300, expected)};
    ASSERT_EQ(expected, Emit(root, irs::doc_limits::kMinCapacity));
  }

  // a segment holding nothing
  {
    irs::docs::Bitset root{MakeSet(300, {})};
    ASSERT_TRUE(Emit(root, irs::doc_limits::kMinCapacity).empty());
  }
}

// What a count's root reduces to once its buckets are folded.
TEST(bitset_count_test, count) {
  ASSERT_EQ(0, irs::search::CountBits(MakeSet(0, {})));
  ASSERT_EQ(0, irs::search::CountBits(MakeSet(13, {})));
  ASSERT_EQ(73, irs::search::CountBits(MakeSet(73, Range(1, 73))));
  ASSERT_EQ(88, irs::search::CountBits(MakeSet(176, Range(1, 175, 2))));
  ASSERT_EQ(1, irs::search::CountBits(MakeSet(189, {185})));
  ASSERT_EQ(4, irs::search::CountBits(MakeSet(256, {1, 64, 130, 255})));
}

// A holder that is interrogated rather than swept fills only as far as the
// question that needs it, and the windows the clause holds nothing in are
// never opened.
TEST(lazy_bitset_test, fills_only_as_far_as_asked) {
  constexpr irs::doc_id_t kDocs = 10000;
  const std::vector<irs::doc_id_t> docs{3, 5000, 9000};

  auto node = irs::memory::make_managed<WindowFill>(MakeSet(kDocs, docs));
  auto* fill = node.get();
  irs::search::LazyBitset set{std::move(node), kDocs, nullptr};

  ASSERT_EQ(0, fill->windows());
  ASSERT_EQ(0, set.Filled());
  ASSERT_EQ(kDocs + 1, set.End());

  ASSERT_TRUE(set.Contains(3));
  ASSERT_EQ(1, fill->windows());
  ASSERT_EQ(irs::search::kWindowDocs, set.Filled());

  // Already decided, so nothing is filled to answer it.
  ASSERT_FALSE(set.Contains(7));
  ASSERT_EQ(1, fill->windows());

  ASSERT_TRUE(set.Contains(5000));
  ASSERT_EQ(2, fill->windows());
  ASSERT_EQ(2 * irs::search::kWindowDocs, set.Filled());

  // A probe that finds nothing in what is decided fills on, and what it
  // reaches is coherent afterwards.
  ASSERT_EQ(9000, set.Probe(5001));
  ASSERT_EQ(3, fill->windows());
  ASSERT_EQ(kDocs + 1, set.Filled());

  ASSERT_TRUE(set.Contains(9000));
  ASSERT_FALSE(set.Contains(8999));
  ASSERT_EQ(3, fill->windows());
  ASSERT_TRUE(irs::doc_limits::eof(set.Probe(9001)));
}

// A window the clause holds nothing in is never opened: the fill says where
// it stands next, and the fold starts again there.
TEST(lazy_bitset_test, skips_the_windows_it_holds_nothing_in) {
  constexpr irs::doc_id_t kDocs = 10000;
  const std::vector<irs::doc_id_t> docs{3, 9000};

  auto node = irs::memory::make_managed<WindowFill>(MakeSet(kDocs, docs));
  auto* fill = node.get();
  irs::search::LazyBitset set{std::move(node), kDocs, nullptr};

  // The segment spans three windows, the middle one holds nothing, and two
  // fills answer a probe that crosses all three.
  ASSERT_EQ(9000, set.Probe(4));
  ASSERT_EQ(2, fill->windows());
  ASSERT_EQ(kDocs + 1, set.Filled());

  ASSERT_TRUE(set.Contains(3));
  ASSERT_TRUE(set.Contains(9000));
  ASSERT_FALSE(set.Contains(5000));
  ASSERT_EQ(2, fill->windows());
}
