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

#include <algorithm>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/index/index_reader.hpp>
#include <latch>
#include <random>
#include <span>
#include <thread>
#include <vector>

#include "connector/row_position.h"
#include "connector/row_position_deduplicator.h"

namespace sdb::connector {
namespace {

class Segment final : public irs::SubReader {
 public:
  explicit Segment(irs::doc_id_t docs) { _meta.docs_count = docs; }

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }

  std::span<const irs::field_id> field_ids() const final { return {}; }

  std::span<const irs::TermReader* const> fields(irs::field_id) const final {
    return {};
  }

  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }

 private:
  irs::SegmentInfo _meta;
};

class View final : public irs::IndexReader {
 public:
  explicit View(std::span<const irs::doc_id_t> docs) {
    _segments.reserve(docs.size());
    for (const auto count : docs) {
      _segments.emplace_back(count);
      _docs += count;
    }
  }

  uint64_t CountMappedMemory() const final { return 0; }

  uint64_t live_docs_count() const final { return _docs; }

  uint64_t docs_count() const final { return _docs; }

  const irs::SubReader& operator[](size_t i) const final {
    return _segments[i];
  }

  size_t size() const final { return _segments.size(); }

 private:
  std::vector<Segment> _segments;
  uint64_t _docs = 0;
};

std::vector<int64_t> Kept(RowPositionDeduplicator& seen,
                          std::span<const uint64_t> positions) {
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                   {duckdb::LogicalType::BIGINT}, positions.size());
  auto* values = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[0]);
  for (size_t i = 0; i != positions.size(); ++i) {
    values[i] = static_cast<int64_t>(positions[i]);
  }
  chunk.SetCardinality(positions.size());
  const auto kept = seen.KeepFirst(chunk, 0);
  EXPECT_EQ(kept, chunk.size());
  std::vector<int64_t> result;
  for (duckdb::idx_t i = 0; i != chunk.size(); ++i) {
    result.push_back(chunk.GetValue(0, i).GetValue<int64_t>());
  }
  return result;
}

std::vector<int64_t> Ints(std::span<const uint64_t> positions) {
  return {positions.begin(), positions.end()};
}

TEST(RowPositionDeduplicatorTest, KeepsTheFirstSightOfEachPosition) {
  const View view{{10, 10}};
  RowPositionDeduplicator seen{view};
  const uint64_t first[] = {MakeRowPosition(0, 3), MakeRowPosition(1, 3),
                            MakeRowPosition(0, 3), MakeRowPosition(0, 1)};
  const uint64_t first_kept[] = {MakeRowPosition(0, 3), MakeRowPosition(1, 3),
                                 MakeRowPosition(0, 1)};
  EXPECT_EQ(Kept(seen, first), Ints(first_kept));
  const uint64_t second[] = {MakeRowPosition(1, 3), MakeRowPosition(1, 10),
                             MakeRowPosition(0, 1)};
  const uint64_t second_kept[] = {MakeRowPosition(1, 10)};
  EXPECT_EQ(Kept(seen, second), Ints(second_kept));
  EXPECT_TRUE(Kept(seen, second).empty());
}

TEST(RowPositionDeduplicatorTest, RunsAcrossWordsAndPages) {
  constexpr irs::doc_id_t kDocs = 3 * 65536;
  const View view{{kDocs}};
  RowPositionDeduplicator seen{view};
  std::vector<uint64_t> run;
  for (irs::doc_id_t doc = 65536 - 100; doc != 65536 + 100; ++doc) {
    run.push_back(MakeRowPosition(0, doc));
  }
  EXPECT_EQ(Kept(seen, run), Ints(run));
  std::vector<uint64_t> wider;
  std::vector<uint64_t> wider_kept;
  for (irs::doc_id_t doc = 65536 - 130; doc != 65536 + 130; ++doc) {
    wider.push_back(MakeRowPosition(0, doc));
    if (doc < 65536 - 100 || doc >= 65536 + 100) {
      wider_kept.push_back(MakeRowPosition(0, doc));
    }
  }
  EXPECT_EQ(Kept(seen, wider), Ints(wider_kept));
  const uint64_t edges[] = {MakeRowPosition(0, 1), MakeRowPosition(0, kDocs),
                            MakeRowPosition(0, 1), MakeRowPosition(0, kDocs)};
  const uint64_t edges_kept[] = {MakeRowPosition(0, 1),
                                 MakeRowPosition(0, kDocs)};
  EXPECT_EQ(Kept(seen, edges), Ints(edges_kept));
}

TEST(RowPositionDeduplicatorTest, ThreadsKeepEachPositionOnce) {
  constexpr irs::doc_id_t kDocs = 200'000;
  const irs::doc_id_t docs[] = {kDocs, kDocs / 2, 7};
  const View view{docs};
  RowPositionDeduplicator seen{view};
  std::vector<uint64_t> all;
  for (uint32_t segment = 0; segment != std::size(docs); ++segment) {
    for (irs::doc_id_t doc = 1; doc <= docs[segment]; ++doc) {
      all.push_back(MakeRowPosition(segment, doc));
    }
  }
  constexpr size_t kThreads = 8;
  constexpr size_t kChunk = 2048;
  std::vector<std::vector<int64_t>> kept(kThreads);
  std::vector<std::thread> threads;
  for (size_t t = 0; t != kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937_64 random{t};
      auto mine = all;
      for (size_t i = 0; i < mine.size(); i += kChunk) {
        const auto end = std::min(mine.size(), i + kChunk);
        if (random() % 2 == 0) {
          std::shuffle(mine.begin() + i, mine.begin() + end, random);
        }
      }
      for (size_t i = 0; i < mine.size(); i += kChunk) {
        const auto end = std::min(mine.size(), i + kChunk);
        auto part = Kept(seen, std::span{mine}.subspan(i, end - i));
        kept[t].insert(kept[t].end(), part.begin(), part.end());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  std::vector<int64_t> merged;
  for (const auto& part : kept) {
    merged.insert(merged.end(), part.begin(), part.end());
  }
  absl::c_sort(merged);
  EXPECT_EQ(merged, Ints(all));
}

TEST(RowPositionDeduplicatorTest, ThreadsRaceToAllocateTheSamePages) {
  constexpr uint32_t kPages = 512;
  constexpr irs::doc_id_t kPageDocs = 1 << 16;
  const View view{{kPages * kPageDocs}};
  RowPositionDeduplicator seen{view};
  std::vector<uint64_t> positions;
  for (uint32_t page = 0; page != kPages; ++page) {
    positions.push_back(MakeRowPosition(0, page * kPageDocs + 1));
  }
  constexpr size_t kThreads = 16;
  std::latch start{kThreads};
  std::vector<std::vector<int64_t>> kept(kThreads);
  std::vector<std::thread> threads;
  for (size_t t = 0; t != kThreads; ++t) {
    threads.emplace_back([&, t] {
      start.arrive_and_wait();
      kept[t] = Kept(seen, positions);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  std::vector<int64_t> merged;
  for (const auto& part : kept) {
    merged.insert(merged.end(), part.begin(), part.end());
  }
  absl::c_sort(merged);
  EXPECT_EQ(merged, Ints(positions));
}

TEST(RowPositionDeduplicatorTest, PositionsHoldEachKeptPositionOnce) {
  const uint32_t units[] = {0, 1, 5, 9};
  const irs::doc_id_t docs[] = {1,     2,     63,    64,     65535,
                                65536, 65537, 70000, 200000, 4000000};
  const View view{std::vector<irs::doc_id_t>(10, 4000000)};
  RowPositionDeduplicator seen{view};
  std::vector<uint64_t> positions;
  for (const auto unit : units) {
    for (const auto doc : docs) {
      positions.push_back(MakeRowPosition(unit, doc));
    }
  }
  EXPECT_EQ(Kept(seen, positions), Ints(positions));
  EXPECT_TRUE(Kept(seen, positions).empty());
  auto removal = std::move(seen).Positions();
  EXPECT_FALSE(removal.Empty());
  for (const auto unit : units) {
    const auto mask = std::move(removal.Group(unit)).Finish();
    std::vector<irs::doc_id_t> read(mask.Count());
    irs::DocumentMask::Cursor cursor{mask};
    ASSERT_EQ(read.size(), cursor.Read(read));
    EXPECT_EQ((std::vector<irs::doc_id_t>{std::begin(docs), std::end(docs)}),
              read)
      << unit;
  }
  EXPECT_TRUE(std::move(removal.Group(2)).Finish().Empty());
}

TEST(RowPositionDeduplicatorTest, NothingSeenIsNoRemoval) {
  const View view{{5, 5, 5}};
  RowPositionDeduplicator seen{view};
  EXPECT_TRUE(std::move(seen).Positions().Empty());
}

}  // namespace
}  // namespace sdb::connector
