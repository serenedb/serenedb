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

#include "basics/containers/bitset.hpp"
#include "basics/singleton.hpp"
#include "filter_test_case_base.hpp"
#include "iresearch/search/bitset_doc_iterator.hpp"
#include "tests_shared.hpp"

size_t Count(const irs::bitset& bs) { return bs.count(); }

size_t IteratorCount(const irs::bitset& bs) {
  irs::BitsetDocIterator it{bs.begin(), bs.end()};
  return it.count();
}

TEST(bitset_iterator_test, next) {
  {
    irs::BitsetDocIterator it(nullptr, nullptr);
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  {
    irs::bitset bs;
    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // non-empty bitset
  {
    irs::bitset bs(13);
    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.value());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }

  // dense bitset
  {
    const size_t size = 73;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    // note that bitset iterator doesn't care about
    // emitting doc_limits::invalid() if first bit is set
    for (size_t i = 0; i < size; ++i) {
      ASSERT_TRUE(it.next());
      ASSERT_EQ(i, it.value());
    }
    ASSERT_FALSE(it.next());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset
  {
    const size_t size = 176;
    irs::bitset bs(size);

    // set every second bit
    for (size_t i = 0; i < size; ++i) {
      bs.reset(i, i % 2);
    }

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size / 2, cost->estimate());

    for (size_t i = 1; i < size; i += 2) {
      ASSERT_TRUE(it.next());
      ASSERT_EQ(i, it.value());
    }
    ASSERT_FALSE(it.next());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset with dense region
  {
    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0),
      ~irs::bitset::word_t(UINT64_C(0x8000000000000000)),
      irs::bitset::word_t(UINT64_C(0x8000000000000000))};

    irs::BitsetDocIterator it(std::begin(data), std::end(data));
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    irs::doc_id_t expected_docs[64];
    std::iota(std::begin(expected_docs), std::end(expected_docs) - 1, 64);
    *(std::end(expected_docs) - 1) = 191;

    auto expected_doc = std::begin(expected_docs);
    while (it.next()) {
      ASSERT_EQ(*expected_doc, it.value());
      ++expected_doc;
    }

    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset with sparse region
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    std::vector<irs::doc_id_t> docs{71,  74,  82,  86,  93,
                                    101, 103, 113, 121, 126};

    auto begin = docs.begin();
    while (it.next()) {
      ASSERT_EQ(*begin, it.value());
      ++begin;
    }
    ASSERT_EQ(begin, docs.end());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(0),
      irs::bitset::word_t(UINT64_C(0x200000000000000))};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_TRUE(it.next());
    ASSERT_EQ(185, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}

TEST(bitset_iterator_test, seek) {
  {
    // empty bitset
    irs::bitset bs;
    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_TRUE(irs::doc_limits::eof(it.seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // non-empty bitset
  {
    irs::bitset bs(13);
    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));

    ASSERT_TRUE(irs::doc_limits::eof(it.seek(1)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // dense bitset
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    for (size_t expected_doc = 0; expected_doc < size; ++expected_doc) {
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.value());
    }
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // dense bitset, seek backwards
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(size));

    for (ptrdiff_t expected_doc = size - 1; expected_doc >= 0; --expected_doc) {
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.value());
    }
    ASSERT_EQ(irs::doc_limits::invalid(), it.value());
    ASSERT_EQ(irs::doc_limits::invalid(), it.seek(irs::doc_limits::invalid()));
  }

  // dense bitset, seek after the last document
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(size));
  }

  // dense bitset, seek to the last document
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(size - 1, it.seek(size - 1));
  }

  // dense bitset, seek to 'eof'
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(irs::doc_limits::eof()));
  }

  // dense bitset, seek before the first document
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(irs::doc_limits::invalid(), it.seek(irs::doc_limits::invalid()));
  }

  // sparse bitset
  {
    const size_t size = 176;
    irs::bitset bs(size);

    // set every second bit
    for (size_t i = 0; i < size; ++i) {
      bs.reset(i, i % 2);
    }

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size / 2, cost->estimate());

    for (size_t expected_doc = 1; expected_doc < size; expected_doc += 2) {
      ASSERT_EQ(expected_doc, it.seek(expected_doc - 1));
      ASSERT_EQ(expected_doc, it.value());
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.value());
    }
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset, seek backwards
  {
    const size_t size = 176;
    irs::bitset bs(size);

    // set every second bit
    for (size_t i = 0; i < size; ++i) {
      bs.reset(i, i % 2);
    }

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size / 2, cost->estimate());

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(size));

    for (ptrdiff_t i = size - 1; i >= 0; i -= 2) {
      ASSERT_EQ(i, it.seek(i));
      ASSERT_EQ(i, it.value());
      ASSERT_EQ(i, it.seek(i - 1));
      ASSERT_EQ(i, it.value());
    }
  }

  // sparse bitset with dense region
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0),
      ~irs::bitset::word_t(UINT64_C(0x8000000000000000)),
      irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {64, 43},
      {64, 43},
      {64, 64},
      {68, 68},
      {78, 78},
      {irs::doc_limits::eof(), 128},
      {irs::doc_limits::eof(), irs::doc_limits::eof()}};

    for (auto& seek : seeks) {
      ASSERT_EQ(seek.first, it.seek(seek.second));
      ASSERT_EQ(seek.first, it.value());
    }
  }

  // sparse bitset with sparse region
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {71, 70},
      {74, 72},
      {126, 125},
      {irs::doc_limits::eof(), 128},
      {irs::doc_limits::eof(), irs::doc_limits::eof()}};

    for (auto& seek : seeks) {
      ASSERT_EQ(seek.first, it.seek(seek.second));
      ASSERT_EQ(seek.first, it.value());
    }
  }

  // sparse bitset with sparse region
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(UINT64_C(0x4440000000000000))};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {irs::doc_limits::eof(), 187}};

    for (auto& seek : seeks) {
      ASSERT_EQ(seek.first, it.seek(seek.second));
      ASSERT_EQ(seek.first, it.value());
    }
  }

  // sparse bitset with sparse region
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(UINT64_C(0x4440000000000000))};
    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> seeks{
      {186, 186}, {irs::doc_limits::eof(), 187}};

    for (auto& seek : seeks) {
      ASSERT_EQ(seek.first, it.seek(seek.second));
      ASSERT_EQ(seek.first, it.value());
    }
  }

  // sparse bitset with sparse region
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(UINT64_C(0x4440000000000000))};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(182, it.seek(181));
    ASSERT_EQ(186, it.seek(186));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(187));
  }

  // sparse bitset
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(0),
      irs::bitset::word_t(UINT64_C(0x200000000000000))};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(185, it.seek(2));
    ASSERT_EQ(irs::doc_limits::eof(), it.seek(187));
  }
}

TEST(bitset_iterator_test, seek_next) {
  // dense bitset
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    const size_t steps = 5;
    for (size_t expected_doc = 0; expected_doc < size; ++expected_doc) {
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.value());

      for (size_t j = 1; j <= steps && it.next(); ++j) {
        ASSERT_EQ(expected_doc + j, it.value());
      }
    }
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // dense bitset, seek backwards
  {
    const size_t size = 173;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[]{~irs::bitset::word_t(0), ~irs::bitset::word_t(0),
                               ~irs::bitset::word_t(0)};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(size));

    const size_t steps = 5;
    for (ptrdiff_t expected_doc = size - 1; expected_doc >= 0; --expected_doc) {
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.value());

      for (size_t j = 1; j <= steps && it.next(); ++j) {
        ASSERT_EQ(expected_doc + j, it.value());
      }
    }
    ASSERT_EQ(steps, it.value());
    ASSERT_EQ(irs::doc_limits::invalid(), it.seek(irs::doc_limits::invalid()));
  }

  // sparse bitset, seek+next
  {
    const size_t size = 176;
    irs::bitset bs(size);

    // set every second bit
    for (size_t i = 0; i < size; ++i) {
      bs.reset(i, i % 2);
    }

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size / 2, cost->estimate());

    size_t steps = 5;
    for (size_t i = 0; i < size; i += 2) {
      const auto expected_doc = irs::doc_limits::min() + i;
      ASSERT_EQ(expected_doc, it.seek(i));
      ASSERT_EQ(expected_doc, it.value());

      for (size_t j = 1; j <= steps && it.next(); ++j) {
        ASSERT_EQ(expected_doc + 2 * j, it.value());
      }
    }
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // sparse bitset, seek backwards+next
  {
    const size_t size = 176;
    irs::bitset bs(size);

    // set every second bit
    for (size_t i = 0; i < size; ++i) {
      bs.reset(i, i % 2);
    }

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size / 2, cost->estimate());

    ASSERT_EQ(irs::doc_limits::eof(), it.seek(size));

    size_t steps = 5;
    for (ptrdiff_t i = size - 1; i >= 0; i -= 2) {
      ASSERT_EQ(i, it.seek(i));
      ASSERT_EQ(i, it.value());

      for (size_t j = 1; j <= steps && it.next(); ++j) {
        ASSERT_EQ(i + 2 * j, it.value());
      }
    }
    ASSERT_EQ(2 * steps + 1, it.value());
  }

  // sparse bitset with sparse region
  {
    const size_t size = 189;
    irs::bitset bs(size);

    // set bits
    irs::bitset::word_t data[]{
      irs::bitset::word_t(0), irs::bitset::word_t(UINT64_C(0x420200A020440480)),
      irs::bitset::word_t(UINT64_C(0x4440000000000000))};

    bs.memset(data);

    EXPECT_EQ(IteratorCount(bs), Count(bs));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_TRUE(!irs::doc_limits::valid(it.value()));

    ASSERT_EQ(71, it.seek(68));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(74, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(82, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(86, it.value());
    ASSERT_EQ(182, it.seek(181));
    ASSERT_TRUE(it.next());
    ASSERT_EQ(186, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

// LazySeek probes target's specific bit:
//   * hit  -> state advances to target, returns target.
//   * miss -> value() unchanged, returns target + 1.
// Contrast with seek(), which always positions at the next doc >= target.
TEST(bitset_iterator_test, lazy_seek) {
  using word_t = irs::bitset::word_t;
  constexpr auto kBits = irs::BitsRequired<word_t>();
  static_assert(kBits == 64, "test expectations assume 64-bit words");

  // (1) Empty bitset: any positive target collapses to eof.
  {
    irs::BitsetDocIterator it(nullptr, nullptr);
    ASSERT_EQ(irs::doc_limits::eof(), it.LazySeek(1));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(irs::doc_limits::eof(), it.LazySeek(100));
    ASSERT_FALSE(it.next());
  }

  // (2) Single-bit hit, then drain via advance().
  {
    irs::bitset bs(128);
    bs.set(7);
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(7u, it.LazySeek(7));
    ASSERT_EQ(7u, it.value());
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // (3) In-word miss leaves value() and _word untouched. A follow-up
  //     advance() resumes from the *prior* hit, not from the miss target.
  {
    irs::bitset bs(128);
    for (size_t d : {3u, 7u, 13u, 30u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(7u, it.LazySeek(7));
    ASSERT_EQ(7u, it.value());
    ASSERT_EQ(9u, it.LazySeek(8));  // miss: bit 8 unset
    ASSERT_EQ(7u, it.value());      // value() unchanged
    ASSERT_TRUE(it.next());
    ASSERT_EQ(13u, it.value());  // advance from prior hit
  }

  // (4) Stay clause: target <= value() short-circuits to value().
  {
    irs::bitset bs(128);
    for (size_t d : {1u, 5u, 17u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(5u, it.LazySeek(5));
    for (int i = 0; i < 8; ++i) {
      ASSERT_EQ(5u, it.LazySeek(5));
      ASSERT_EQ(5u, it.value());
    }
    ASSERT_EQ(5u, it.LazySeek(3));  // lower target -> stay
    ASSERT_EQ(5u, it.value());
    ASSERT_EQ(17u, it.LazySeek(17));  // forward progress resumes
  }

  // (5) Bit-0 of word 1 (doc 64): widest possible shift mask in the hit
  //     path; `_word` should hold every higher bit of the word.
  {
    irs::bitset bs(192);
    for (size_t d : {1u, 64u, 65u, 90u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(1u, it.LazySeek(1));
    ASSERT_EQ(64u, it.LazySeek(64));
    ASSERT_EQ(64u, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(65u, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(90u, it.value());
    ASSERT_FALSE(it.next());
  }

  // (6) Bit-63 of word 0: shift `>> 63 >> 1` must zero _word (UB-free
  //     replacement for `>> 64`). Next advance() must cross into word 1.
  {
    irs::bitset bs(192);
    for (size_t d : {1u, 63u, 64u, 65u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(1u, it.LazySeek(1));
    ASSERT_EQ(63u, it.LazySeek(63));
    ASSERT_EQ(63u, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(64u, it.value());  // crossed into word 1
    ASSERT_TRUE(it.next());
    ASSERT_EQ(65u, it.value());
    ASSERT_FALSE(it.next());
  }

  // (7) Bit-63 miss: bit 63 unset, target+1 returned.
  {
    irs::bitset bs(192);
    for (size_t d : {1u, 60u, 64u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(60u, it.LazySeek(60));
    ASSERT_EQ(64u, it.LazySeek(63));  // miss at bit 63 -> target+1
    ASSERT_EQ(60u, it.value());
  }

  // (8) Consecutive LazySeek hits in the same word -- no advance()
  //     between them.
  {
    irs::bitset bs(128);
    for (size_t d : {5u, 12u, 13u, 25u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(5u, it.LazySeek(5));
    ASSERT_EQ(12u, it.LazySeek(12));
    ASSERT_EQ(13u, it.LazySeek(13));
    ASSERT_EQ(25u, it.LazySeek(25));
    ASSERT_EQ(25u, it.value());
    ASSERT_FALSE(it.next());
  }

  // (9) Consecutive misses leave value() pinned to the prior hit.
  {
    irs::bitset bs(128);
    for (size_t d : {1u, 10u, 50u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(1u, it.LazySeek(1));
    ASSERT_EQ(21u, it.LazySeek(20));
    ASSERT_EQ(31u, it.LazySeek(30));
    ASSERT_EQ(1u, it.value());
    ASSERT_EQ(50u, it.LazySeek(50));
  }

  // (10) Miss at bit i, hit at adjacent bit i+1.
  {
    irs::bitset bs(128);
    for (size_t d : {1u, 5u, 6u, 50u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(1u, it.LazySeek(1));
    ASSERT_EQ(5u, it.LazySeek(4));  // miss at 4 -> target+1
    ASSERT_EQ(1u, it.value());
    ASSERT_EQ(5u, it.LazySeek(5));
    ASSERT_EQ(6u, it.LazySeek(6));  // adjacent bit
    ASSERT_EQ(6u, it.value());
    ASSERT_EQ(50u, it.LazySeek(50));
  }

  // (11) Cross-word forward walk: LazySeek + advance never re-emits a doc.
  {
    irs::bitset bs(192);
    const std::vector<size_t> docs{1, 5, 9, 13, 64, 68, 100, 127, 130, 180};
    for (size_t d : docs) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    std::vector<irs::doc_id_t> seen;
    auto r = it.LazySeek(docs.front());
    while (!irs::doc_limits::eof(r)) {
      seen.push_back(it.value());
      r = it.next() ? it.value() : irs::doc_limits::eof();
    }
    std::vector<irs::doc_id_t> expected(docs.begin(), docs.end());
    ASSERT_EQ(expected, seen);
  }

  // (12) Cross-word hit: target in word 2.
  {
    irs::bitset bs(256);
    for (size_t d : {3u, 130u, 200u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(3u, it.LazySeek(3));
    ASSERT_EQ(130u, it.LazySeek(130));
    ASSERT_EQ(130u, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(200u, it.value());
    ASSERT_FALSE(it.next());
  }

  // (13) Cross-word miss: target in word 1 where no bits are set.
  {
    irs::bitset bs(256);
    for (size_t d : {3u, 200u}) {
      bs.set(d);  // word 1 is empty
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(3u, it.LazySeek(3));
    ASSERT_EQ(100u + 1, it.LazySeek(100));  // bit 100 unset -> target+1
    ASSERT_EQ(3u, it.value());
    ASSERT_EQ(200u, it.LazySeek(200));
  }

  // (14) Target past every set bit -> eof.
  {
    irs::bitset bs(128);
    for (size_t d : {1u, 50u, 90u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(90u, it.LazySeek(90));
    // 91..127 in this bitset are all unset; the in-word probe just
    // misses, then `target+1` for each subsequent call until target
    // exceeds the bitset and a refill (returning false) drives eof.
    ASSERT_EQ(92u, it.LazySeek(91));
    ASSERT_EQ(101u, it.LazySeek(100));
    ASSERT_EQ(irs::doc_limits::eof(),
              it.LazySeek(static_cast<irs::doc_id_t>(1000)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // (15) LazySeek interleaved with seek() and advance().
  {
    irs::bitset bs(128);
    for (size_t d : {1u, 3u, 7u, 11u, 15u, 30u}) {
      bs.set(d);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(7u, it.seek(7));        // seek positions at 7
    ASSERT_EQ(11u, it.LazySeek(11));  // forward LazySeek
    ASSERT_EQ(13u, it.LazySeek(12));  // miss -> target+1
    ASSERT_EQ(11u, it.value());
    ASSERT_TRUE(it.next());
    ASSERT_EQ(15u, it.value());  // advance from 11
    ASSERT_EQ(30u, it.LazySeek(30));
    ASSERT_FALSE(it.next());
  }

  // (16) Dense word (all 64 bits set): every LazySeek in the word hits.
  {
    irs::bitset bs(128);
    word_t all{};
    all = ~all;  // 0xffff...
    bs.memset(&all, sizeof(all));
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    for (size_t t = 1; t < kBits; ++t) {
      const irs::doc_id_t prev = it.value();
      const auto r = it.LazySeek(static_cast<irs::doc_id_t>(t));
      if (t <= prev) {
        ASSERT_EQ(prev, r);  // stay clause
      } else {
        ASSERT_EQ(t, r);
        ASSERT_EQ(t, it.value());
      }
    }
  }

  // (17) Sparse word (every second bit set): even targets hit, odd miss.
  {
    irs::bitset bs(128);
    for (size_t i = 0; i < 128; i += 2) {
      bs.set(i);
    }
    irs::BitsetDocIterator it(bs.begin(), bs.end());
    ASSERT_EQ(2u, it.LazySeek(2));
    ASSERT_EQ(4u, it.LazySeek(3));  // odd -> miss -> target+1
    ASSERT_EQ(2u, it.value());
    ASSERT_EQ(4u, it.LazySeek(4));
    ASSERT_EQ(6u, it.LazySeek(5));  // miss
    ASSERT_EQ(4u, it.value());
    ASSERT_EQ(64u, it.LazySeek(64));  // hop into word 1
    ASSERT_EQ(66u, it.LazySeek(66));
  }
}
