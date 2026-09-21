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
#include <iresearch/index/document_mask.hpp>
#include <iresearch/search/detail/bitset_storage.hpp>
#include <iresearch/search/detail/lazy_bitset.hpp>
#include <iresearch/search/docs/boolean_bitset.hpp>
#include <iresearch/search/docs/boolean_window.hpp>
#include <iresearch/search/fill/bitset_docs.hpp>
#include <iresearch/search/fill/docs_mask.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/fill/walk.hpp>
#include <iresearch/search/lead/bitset_docs.hpp>
#include <iresearch/search/probe/bitset_docs.hpp>
#include <iresearch/search/probe/docs_mask.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <iresearch/utils/memory.hpp>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr auto kBits = irs::detail::BitsetStorage::kBits;
constexpr auto kMin = irs::detail::BitsetStorage::kMin;

irs::detail::BitsetStorage MakeSet(irs::doc_id_t docs_count,
                                   const std::vector<irs::doc_id_t>& docs) {
  irs::detail::BitsetStorage set{docs_count};
  auto* words = set.Words();
  for (auto doc : docs) {
    EXPECT_TRUE(irs::doc_limits::valid(doc));
    EXPECT_LE(doc, docs_count);
    const auto offset = doc - kMin;
    irs::SetBit(words[offset / kBits], offset % kBits);
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

class Cursor {
 public:
  explicit Cursor(irs::detail::BitsetStorage&& set) : _it{std::move(set)} {}

  irs::doc_id_t Value() const noexcept { return _doc; }

  irs::doc_id_t Next() { return _doc = _it.Next(); }

  irs::doc_id_t Seek(irs::doc_id_t target) { return _doc = _it.Seek(target); }

 private:
  irs::lead::BitsetDocs _it;
  irs::doc_id_t _doc = irs::doc_limits::invalid();
};

std::vector<irs::doc_id_t> Drain(Cursor& it) {
  std::vector<irs::doc_id_t> docs;
  for (auto doc = it.Next(); !irs::doc_limits::eof(doc); doc = it.Next()) {
    docs.emplace_back(doc);
  }
  return docs;
}

// Every document the root emits, driven the way the scan drives it: fixed
// windows of `capacity` rows walked from the segment start to its end.
std::vector<irs::doc_id_t> Emit(irs::docs::Root& root, uint32_t capacity,
                                irs::doc_id_t docs_count) {
  std::vector<irs::doc_id_t> out(capacity + irs::doc_limits::kDocsSlack);
  std::vector<irs::doc_id_t> docs;
  const auto end = irs::doc_limits::min() + docs_count;
  for (auto min = irs::doc_limits::min(); min < end; min += capacity) {
    const auto max = std::min<irs::doc_id_t>(min + capacity, end);
    const auto n = root.Run(min, max, out.data());
    EXPECT_LE(n, max - min);
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
  explicit WindowFill(irs::detail::BitsetStorage&& set) noexcept
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

TEST(bitset_docs_test, walk_lead_first_window) {
  const std::vector<irs::doc_id_t> expected{5, 10, 4100};
  irs::docs::BooleanWindow<irs::fill::WalkDocs<irs::lead::BitsetDocs>,
                           irs::utils::Empty, irs::utils::Empty,
                           irs::utils::Empty>
    root{std::piecewise_construct,
         std::forward_as_tuple(MakeSet(4200, expected)),
         std::forward_as_tuple(), std::forward_as_tuple(),
         std::forward_as_tuple()};
  ASSERT_EQ(expected, Emit(root, irs::doc_limits::kMinCapacity, 4200));
}

TEST(bitset_lead_test, advance) {
  // empty segment
  {
    auto set = MakeSet(0, {});
    ASSERT_EQ(0, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // non-empty segment holding nothing
  {
    auto set = MakeSet(13, {});
    ASSERT_EQ(0, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense
  {
    const auto expected = Range(1, 73);
    auto set = MakeSet(73, expected);
    ASSERT_EQ(73, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // sparse: every second document
  {
    const auto expected = Range(1, 175, 2);
    auto set = MakeSet(176, expected);
    ASSERT_EQ(88, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // sparse with a dense region
  {
    auto expected = Range(64, 126);
    expected.emplace_back(191);
    auto set = MakeSet(192, expected);
    ASSERT_EQ(64, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // sparse with a sparse region
  {
    const std::vector<irs::doc_id_t> expected{71,  74,  82,  86,  93,
                                              101, 103, 113, 121, 126};
    auto set = MakeSet(173, expected);
    ASSERT_EQ(10, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // one document, in the last word
  {
    const std::vector<irs::doc_id_t> expected{185};
    auto set = MakeSet(189, expected);
    ASSERT_EQ(1, irs::detail::CountBits(set));
    Cursor it{std::move(set)};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    ASSERT_EQ(expected, Drain(it));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }
}

TEST(bitset_lead_test, seek) {
  // empty segment
  {
    Cursor it{MakeSet(0, {})};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // non-empty segment holding nothing
  {
    Cursor it{MakeSet(13, {})};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Value());

    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // dense, ascending targets
  {
    Cursor it{MakeSet(173, Range(1, 173))};
    ASSERT_FALSE(irs::doc_limits::valid(it.Value()));

    for (irs::doc_id_t expected = 1; expected <= 173; ++expected) {
      ASSERT_EQ(expected, it.Seek(expected));
      ASSERT_EQ(expected, it.Value());
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense, a target at or below where it stands is where it stays
  {
    Cursor it{MakeSet(173, Range(1, 173))};

    ASSERT_EQ(100, it.Seek(100));
    for (irs::doc_id_t target = 100; target != 0; --target) {
      ASSERT_EQ(100, it.Seek(target));
      ASSERT_EQ(100, it.Value());
    }
    ASSERT_EQ(101, it.Next());
  }

  // dense, seek past the last document
  {
    Cursor it{MakeSet(173, Range(1, 173))};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(174)));
  }

  // dense, seek to the last document
  {
    Cursor it{MakeSet(173, Range(1, 173))};
    ASSERT_EQ(173, it.Seek(173));
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
  }

  // dense, seek to 'eof'
  {
    Cursor it{MakeSet(173, Range(1, 173))};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(irs::doc_limits::eof())));
  }

  // dense, seek before the first document
  {
    Cursor it{MakeSet(173, Range(1, 173))};
    ASSERT_EQ(irs::doc_limits::invalid(), it.Seek(irs::doc_limits::invalid()));
    ASSERT_EQ(1, it.Next());
  }

  // sparse: a target on a document nobody holds lands on the next one
  {
    Cursor it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(1, it.Seek(1));
    for (irs::doc_id_t expected = 3; expected < 176; expected += 2) {
      ASSERT_EQ(expected, it.Seek(expected - 1));
      ASSERT_EQ(expected, it.Value());
      ASSERT_EQ(expected, it.Seek(expected));
      ASSERT_EQ(expected, it.Value());
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // sparse, a target at or below where it stands is where it stays
  {
    Cursor it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(101, it.Seek(100));
    for (irs::doc_id_t target = 101; target != 0; --target) {
      ASSERT_EQ(101, it.Seek(target));
      ASSERT_EQ(101, it.Value());
    }
    ASSERT_EQ(103, it.Next());
  }

  // sparse with a dense region
  {
    Cursor it{MakeSet(173, Range(64, 126))};

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
    Cursor it{MakeSet(173, {71, 74, 82, 86, 93, 101, 103, 113, 121, 126})};

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
    Cursor it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  {
    Cursor it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_EQ(186, it.Seek(186));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }

  {
    Cursor it{MakeSet(189, {71, 121, 182, 186})};
    ASSERT_EQ(182, it.Seek(181));
    ASSERT_EQ(186, it.Seek(186));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }

  // a target that crosses two empty words
  {
    Cursor it{MakeSet(189, {185})};
    ASSERT_EQ(185, it.Seek(2));
    ASSERT_TRUE(irs::doc_limits::eof(it.Seek(187)));
  }
}

TEST(bitset_lead_test, seek_advance) {
  constexpr irs::doc_id_t kSteps = 5;

  // dense
  {
    Cursor it{MakeSet(173, Range(1, 173))};

    for (irs::doc_id_t target = 1; target <= 173; target += kSteps + 1) {
      ASSERT_EQ(target, it.Seek(target));
      ASSERT_EQ(target, it.Value());

      for (irs::doc_id_t j = 1; j <= kSteps && !irs::doc_limits::eof(it.Next());
           ++j) {
        ASSERT_EQ(target + j, it.Value());
      }
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // dense, a target below where it stands leaves the walk where it was
  {
    Cursor it{MakeSet(173, Range(1, 173))};

    ASSERT_EQ(50, it.Seek(50));
    for (irs::doc_id_t j = 1; j <= kSteps; ++j) {
      ASSERT_EQ(50 + j, it.Next());
    }
    ASSERT_EQ(50 + kSteps, it.Seek(3));
    ASSERT_EQ(50 + kSteps + 1, it.Next());
  }

  // sparse: every second document
  {
    Cursor it{MakeSet(176, Range(1, 175, 2))};

    ASSERT_EQ(1, it.Seek(1));
    for (irs::doc_id_t target = 3; target <= 176; target += 2 * (kSteps + 1)) {
      ASSERT_EQ(target, it.Seek(target - 1));
      ASSERT_EQ(target, it.Value());

      for (irs::doc_id_t j = 1; j <= kSteps && !irs::doc_limits::eof(it.Next());
           ++j) {
        ASSERT_EQ(target + 2 * j, it.Value());
      }
    }
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
    ASSERT_TRUE(irs::doc_limits::eof(it.Value()));
  }

  // sparse with a sparse region
  {
    Cursor it{
      MakeSet(189, {71, 74, 82, 86, 93, 101, 103, 113, 121, 126, 182, 186})};

    ASSERT_EQ(71, it.Seek(68));
    ASSERT_EQ(74, it.Next());
    ASSERT_EQ(82, it.Next());
    ASSERT_EQ(86, it.Next());
    ASSERT_EQ(182, it.Seek(181));
    ASSERT_EQ(186, it.Next());
    ASSERT_TRUE(irs::doc_limits::eof(it.Next()));
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
  // empty segment
  {
    irs::probe::BitsetDocs it{MakeSet(0, {})};
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(1)));
  }

  // one document, then the words past it
  {
    irs::probe::BitsetDocs it{MakeSet(128, {7})};
    ASSERT_EQ(7, it.Probe(7));
    ASSERT_EQ(kMin + kBits, it.Probe(8));
    ASSERT_EQ(kMin + 2 * kBits, it.Probe(kMin + kBits));
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(kMin + 2 * kBits)));
  }

  // a miss inside the word answers with the next document that word holds
  {
    irs::probe::BitsetDocs it{MakeSet(128, {3, 7, 13, 30})};
    ASSERT_EQ(7, it.Probe(7));
    ASSERT_EQ(13, it.Probe(8));
    ASSERT_EQ(30, it.Probe(14));
    ASSERT_EQ(kMin + kBits, it.Probe(31));
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
    ASSERT_EQ(kMin + 2 * kBits, it.Probe(91));
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
    ASSERT_EQ(kMin + 2 * kBits, it.Probe(100));
    ASSERT_EQ(kMin + 3 * kBits, it.Probe(kMin + 2 * kBits));
    ASSERT_EQ(200, it.Probe(kMin + 3 * kBits));
  }

  // past every document, and then past every word
  {
    irs::probe::BitsetDocs it{MakeSet(128, {1, 50, 90})};
    ASSERT_EQ(90, it.Probe(90));
    ASSERT_EQ(kMin + 2 * kBits, it.Probe(91));
    ASSERT_TRUE(irs::doc_limits::eof(it.Probe(kMin + 2 * kBits)));
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
    const auto next = irs::detail::NextBit(reference, target);
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
    irs::docs::BooleanBitset root{MakeSet(4096, expected)};
    ASSERT_EQ(expected, Emit(root, irs::doc_limits::kMinCapacity, 4096));
  }

  // more documents than one batch holds: a word is never split across two
  {
    const auto expected = Range(1, 300);
    irs::docs::BooleanBitset root{MakeSet(300, expected)};
    ASSERT_EQ(expected, Emit(root, irs::doc_limits::kMinCapacity, 300));
  }

  // a segment holding nothing
  {
    irs::docs::BooleanBitset root{MakeSet(300, {})};
    ASSERT_TRUE(Emit(root, irs::doc_limits::kMinCapacity, 300).empty());
  }
}

// What a count's root reduces to once its buckets are folded.
TEST(bitset_count_test, count) {
  ASSERT_EQ(0, irs::detail::CountBits(MakeSet(0, {})));
  ASSERT_EQ(0, irs::detail::CountBits(MakeSet(13, {})));
  ASSERT_EQ(73, irs::detail::CountBits(MakeSet(73, Range(1, 73))));
  ASSERT_EQ(88, irs::detail::CountBits(MakeSet(176, Range(1, 175, 2))));
  ASSERT_EQ(1, irs::detail::CountBits(MakeSet(189, {185})));
  ASSERT_EQ(4, irs::detail::CountBits(MakeSet(256, {1, 64, 130, 255})));
}

// A holder that is interrogated rather than swept fills only as far as the
// question that needs it, and the windows the clause holds nothing in are
// never opened.
TEST(lazy_bitset_test, fills_only_as_far_as_asked) {
  constexpr irs::doc_id_t kDocs = 10000;
  const std::vector<irs::doc_id_t> docs{3, 5000, 9000};

  auto node = irs::memory::make_managed<WindowFill>(MakeSet(kDocs, docs));
  auto* fill = node.get();
  irs::detail::LazyBitset set{std::move(node), kDocs, {}};

  ASSERT_EQ(0, fill->windows());
  ASSERT_EQ(kMin, set.Filled());
  ASSERT_EQ(kDocs + 1, set.End());

  ASSERT_TRUE(set.Contains(3));
  ASSERT_EQ(1, fill->windows());
  ASSERT_EQ(kMin + irs::detail::kWindowDocs, set.Filled());

  // Already decided, so nothing is filled to answer it.
  ASSERT_FALSE(set.Contains(7));
  ASSERT_EQ(1, fill->windows());

  ASSERT_TRUE(set.Contains(5000));
  ASSERT_EQ(2, fill->windows());
  ASSERT_EQ(kMin + 2 * irs::detail::kWindowDocs, set.Filled());

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

TEST(lazy_bitset_test, drops_a_masked_tail) {
  constexpr irs::doc_id_t kDocs = 10000;
  constexpr irs::doc_id_t kTail = 5000;
  const std::vector<irs::doc_id_t> docs{3, 64, 4999, kTail, 5001, 9000};

  const auto removals = [] {
    irs::DocumentMask mask;
    mask.Add(64);
    mask.Trim();
    return mask;
  }();

  auto node = irs::memory::make_managed<WindowFill>(MakeSet(kDocs, docs));
  irs::detail::LazyBitset set{std::move(node), kDocs,
                              irs::DocumentMask::Iterator{&removals, kTail}};

  ASSERT_TRUE(set.Contains(3));
  ASSERT_FALSE(set.Contains(64));
  ASSERT_TRUE(set.Contains(4999));
  ASSERT_FALSE(set.Contains(kTail));
  ASSERT_FALSE(set.Contains(5001));
  ASSERT_FALSE(set.Contains(9000));

  ASSERT_EQ(4999, set.Probe(4));
  ASSERT_TRUE(irs::doc_limits::eof(set.Probe(kTail)));
}

// A window the clause holds nothing in is never opened: the fill says where
// it stands next, and the fold starts again there.
TEST(lazy_bitset_test, skips_the_windows_it_holds_nothing_in) {
  constexpr irs::doc_id_t kDocs = 10000;
  const std::vector<irs::doc_id_t> docs{3, 9000};

  auto node = irs::memory::make_managed<WindowFill>(MakeSet(kDocs, docs));
  auto* fill = node.get();
  irs::detail::LazyBitset set{std::move(node), kDocs, {}};

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

namespace {

irs::DocumentMask MakeMask(const std::vector<irs::doc_id_t>& docs) {
  irs::DocumentMask mask;
  for (auto doc : docs) {
    mask.Add(doc);
  }
  mask.Trim();
  return mask;
}

irs::probe::DocsMask ProbeOver(const irs::DocumentMask* mask,
                               irs::doc_id_t uncommitted) {
  return irs::probe::DocsMask{mask, uncommitted};
}

}  // namespace

TEST(docs_mask_test, probe_reports_the_next_deleted_doc) {
  const auto removals = MakeMask({3, 64, 4999});
  auto probe = ProbeOver(&removals, irs::doc_limits::eof());

  ASSERT_EQ(3, probe.Probe(1));
  ASSERT_EQ(3, probe.Probe(3));
  ASSERT_EQ(64, probe.Probe(4));
  ASSERT_EQ(64, probe.Probe(64));
  ASSERT_EQ(4999, probe.Probe(65));
  ASSERT_TRUE(irs::doc_limits::eof(probe.Probe(5000)));
}

TEST(docs_mask_test, probe_without_removals_excludes_nothing) {
  auto probe = ProbeOver(nullptr, irs::doc_limits::eof());

  ASSERT_TRUE(irs::doc_limits::eof(probe.Probe(1)));
  ASSERT_TRUE(irs::doc_limits::eof(probe.Probe(10000)));
}

TEST(docs_mask_test, probe_treats_the_uncommitted_tail_as_deleted) {
  const auto removals = MakeMask({3});
  auto probe = ProbeOver(&removals, 5000);

  ASSERT_EQ(3, probe.Probe(1));
  ASSERT_EQ(5000, probe.Probe(4));
  ASSERT_EQ(6000, probe.Probe(6000));
}

TEST(docs_mask_test, tail_only_probe_starts_at_the_bound) {
  auto probe = ProbeOver(nullptr, 70);

  ASSERT_EQ(70, probe.Probe(1));
  ASSERT_EQ(99, probe.Probe(99));
}

TEST(docs_mask_test, fill_sets_deleted_bits_in_a_window) {
  const auto removals = MakeMask({1, 3, 64, 127, 128});
  irs::fill::DocsMask fill{&removals, irs::doc_limits::eof()};

  uint64_t words[3]{};
  const auto next = fill.FillOr(1, 1 + 3 * kBits, words);

  ASSERT_TRUE(irs::CheckBit(words[0], 0));
  ASSERT_FALSE(irs::CheckBit(words[0], 1));
  ASSERT_TRUE(irs::CheckBit(words[0], 2));
  ASSERT_TRUE(irs::CheckBit(words[0], 63));
  ASSERT_TRUE(irs::CheckBit(words[1], 62));
  ASSERT_TRUE(irs::CheckBit(words[1], 63));
  ASSERT_EQ(0, words[2]);
  ASSERT_TRUE(irs::doc_limits::eof(next));
}

TEST(docs_mask_test, fill_covers_the_uncommitted_tail) {
  irs::fill::DocsMask fill{nullptr, 70};

  uint64_t words[2]{};
  const auto next = fill.FillOr(1, 1 + 2 * kBits, words);

  ASSERT_FALSE(irs::CheckBit(words[0], 63));
  ASSERT_FALSE(irs::CheckBit(words[1], 4));
  ASSERT_TRUE(irs::CheckBit(words[1], 5));
  ASSERT_TRUE(irs::CheckBit(words[1], 63));
  ASSERT_EQ(1 + 2 * kBits, next);
}

TEST(docs_mask_test, fill_agrees_with_probe_across_windows) {
  std::vector<irs::doc_id_t> docs;
  for (irs::doc_id_t doc = 1; doc < 20000; ++doc) {
    if (doc % 7 == 0 || doc % 13 == 0 || (doc >= 8000 && doc < 9000)) {
      docs.push_back(doc);
    }
  }
  const auto removals = MakeMask(docs);

  irs::fill::DocsMask fill{&removals, 15000};
  auto probe = ProbeOver(&removals, 15000);

  constexpr uint32_t kWords = 64;
  constexpr irs::doc_id_t kSpan = kWords * kBits;
  for (irs::doc_id_t base = 1; base < 1 + 4 * kSpan; base += kSpan) {
    std::vector<uint64_t> words(kWords, 0);
    fill.FillOr(base, base + kSpan, words.data());

    for (uint32_t w = 0; w != kWords; ++w) {
      for (uint32_t bit = 0; bit != kBits; ++bit) {
        const auto doc = static_cast<irs::doc_id_t>(base + w * kBits + bit);
        const bool excluded = probe.Probe(doc) == doc;
        ASSERT_EQ(excluded, irs::CheckBit(words[w], bit)) << "doc " << doc;
      }
    }
  }
}

// A set that is already folded has no clause left to fill from, so a question
// past its end has to stop at the end rather than reach for one. CountAgainst
// asks exactly that of the last leaf of a bounded scan.
TEST(lazy_bitset_test, reaching_past_the_end_of_a_folded_set) {
  constexpr irs::doc_id_t kDocs = 300;
  irs::detail::LazyBitset set{MakeSet(kDocs, {3, 100, 299}), nullptr};

  ASSERT_EQ(kDocs + 1, set.End());
  ASSERT_EQ(kDocs + 1, set.Filled());

  set.Reach(kDocs + 1);
  set.Reach(kDocs + 2);
  set.Reach(irs::doc_limits::eof());
  ASSERT_EQ(kDocs + 1, set.Filled());

  ASSERT_TRUE(set.Contains(3));
  ASSERT_TRUE(set.Contains(299));
  ASSERT_FALSE(set.Contains(4));
  ASSERT_EQ(irs::doc_limits::eof(), set.Probe(kDocs + 1));
}
