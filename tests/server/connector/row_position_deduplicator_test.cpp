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
#include <random>
#include <span>
#include <thread>
#include <vector>

#include "connector/row_position.h"
#include "connector/row_position_deduplicator.h"

namespace sdb::connector {
namespace {

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
  const irs::doc_id_t docs[] = {10, 10};
  RowPositionDeduplicator seen{docs};
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
  const irs::doc_id_t docs[] = {kDocs};
  RowPositionDeduplicator seen{docs};
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
  RowPositionDeduplicator seen{docs};
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

}  // namespace
}  // namespace sdb::connector
