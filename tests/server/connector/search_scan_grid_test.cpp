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

// The scan's work grid: a work item is one row group of one segment, so an
// index compacted into a SINGLE segment still spreads over every worker. These
// pin the claim protocol itself -- exhaustive-once, per-segment affinity,
// stealing, the scan-order permutation -- and the worker count the scan
// declares for each mode.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <iresearch/search/vector_similarity_scorer.hpp>
#include <memory>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "connector/duckdb_search_full_scan.hpp"

namespace sdb::connector {
namespace {

using Item = std::pair<uint32_t, uint32_t>;

// A grid of `segments` segments with the given row-group counts, in segment
// order -- what BuildGrid produces for a reader of that shape. A slot index is
// a segment index, so the identity `segment_order` is what says so.
void FillGridWith(IResearchScanGlobalState& g,
                  const std::vector<uint32_t>& rgs_per_segment,
                  uint32_t run = 1) {
  const auto segments = static_cast<uint32_t>(rgs_per_segment.size());
  g.grid.rg_base.assign(1, 0);
  for (const auto count : rgs_per_segment) {
    g.grid.rg_base.push_back(g.grid.rg_base.back() + count);
  }
  g.grid.rg_order.clear();
  g.grid.cursors = std::vector<IResearchScanGlobalState::RgCursor>(segments);
  g.grid.next_slot.store(0);
  g.grid.run = run;
  g.segment_order.clear();
  g.total_segments = segments;
  g.claimable_segments = segments;
}

void FillGrid(IResearchScanGlobalState& g, uint32_t segments,
              uint32_t rgs_per_segment) {
  FillGridWith(g, std::vector<uint32_t>(segments, rgs_per_segment));
}

std::vector<Item> DrainWith(IResearchScanGlobalState& g, uint32_t workers) {
  std::vector<std::vector<Item>> per_worker(workers);
  std::vector<std::thread> threads;
  threads.reserve(workers);
  for (uint32_t w = 0; w < workers; ++w) {
    threads.emplace_back([&g, &per_worker, w] {
      IResearchScanGlobalState::RgClaim claim;
      while (g.ClaimRowGroup(claim)) {
        per_worker[w].emplace_back(claim.seg, claim.rg);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  std::vector<Item> all;
  for (auto& claims : per_worker) {
    all.insert(all.end(), claims.begin(), claims.end());
  }
  return all;
}

// Claim in lockstep: every worker takes one item in turn, which is what makes
// the distribution deterministic (the claim protocol, not the OS scheduler).
std::vector<std::vector<Item>> DrainRoundRobin(IResearchScanGlobalState& g,
                                               uint32_t workers) {
  std::vector<IResearchScanGlobalState::RgClaim> claims(workers);
  std::vector<std::vector<Item>> per_worker(workers);
  for (bool progress = true; progress;) {
    progress = false;
    for (uint32_t w = 0; w < workers; ++w) {
      if (g.ClaimRowGroup(claims[w])) {
        per_worker[w].emplace_back(claims[w].seg, claims[w].rg);
        progress = true;
      }
    }
  }
  return per_worker;
}

TEST(SearchScanGrid, ClaimsEveryRowGroupExactlyOnce) {
  for (const auto workers : {1U, 2U, 8U, 32U}) {
    for (const auto shape :
         {Item{1, 79}, Item{8, 1}, Item{4, 13}, Item{3, 0}}) {
      IResearchScanGlobalState g;
      FillGrid(g, shape.first, shape.second);
      auto claimed = DrainWith(g, workers);
      ASSERT_EQ(claimed.size(), g.grid.TotalRgs());
      std::set<Item> unique{claimed.begin(), claimed.end()};
      ASSERT_EQ(unique.size(), claimed.size());
      for (const auto& [seg, rg] : unique) {
        EXPECT_LT(seg, shape.first);
        EXPECT_LT(rg, shape.second);
      }
      EXPECT_EQ(g.ClaimedRowGroups(), g.grid.TotalRgs());
    }
  }
}

// The thesis: one segment of many row groups is not one work item. Every
// worker gets a share of it, and the shares differ by at most one claim.
TEST(SearchScanGrid, SingleSegmentSpreadsOverEveryWorker) {
  for (const auto workers : {2U, 8U, 32U}) {
    IResearchScanGlobalState g;
    FillGrid(g, /*segments=*/1, /*rgs_per_segment=*/79);
    const auto per_worker = DrainRoundRobin(g, workers);
    size_t total = 0;
    size_t smallest = 79;
    size_t largest = 0;
    for (const auto& claims : per_worker) {
      total += claims.size();
      smallest = std::min(smallest, claims.size());
      largest = std::max(largest, claims.size());
      for (const auto& [seg, rg] : claims) {
        EXPECT_EQ(seg, 0U);
      }
    }
    EXPECT_EQ(total, 79U);
    EXPECT_GT(smallest, 0U);
    EXPECT_LE(largest - smallest, 1U);
  }
}

// Affinity: with a segment per worker, each worker stays on its own for every
// claim -- the per-segment state it binds (filters, batcher, scanners) is
// rebuilt only when it runs out.
TEST(SearchScanGrid, AffinityKeepsAWorkerOnItsSegment) {
  IResearchScanGlobalState g;
  FillGrid(g, /*segments=*/4, /*rgs_per_segment=*/4);
  const auto per_worker = DrainRoundRobin(g, 4);
  std::set<uint32_t> segments;
  for (const auto& claims : per_worker) {
    ASSERT_EQ(claims.size(), 4U);
    std::set<uint32_t> claimed_segments;
    for (const auto& [seg, rg] : claims) {
      claimed_segments.insert(seg);
    }
    ASSERT_EQ(claimed_segments.size(), 1U);
    segments.insert(*claimed_segments.begin());
  }
  EXPECT_EQ(segments.size(), 4U);
}

// Stealing: a worker whose segment drains and that finds no fresh slot takes
// row groups from a segment another worker is already on.
TEST(SearchScanGrid, StealsFromAnotherSegmentWhenDrained) {
  IResearchScanGlobalState g;
  FillGridWith(g, {1, 5});

  const auto per_worker = DrainRoundRobin(g, 2);
  size_t total = 0;
  for (const auto& claims : per_worker) {
    total += claims.size();
  }
  EXPECT_EQ(total, 6U);
  // The worker that started on the one-row-group segment moved to the other.
  const bool stole = std::ranges::any_of(
    per_worker[0], [](const Item& item) { return item.first == 1; });
  EXPECT_TRUE(stole);
}

// The scan order is a claim policy: the i-th claim of a segment runs the i-th
// row group of its permutation.
TEST(SearchScanGrid, ScanOrderPermutesClaimOrder) {
  IResearchScanGlobalState g;
  FillGrid(g, /*segments=*/1, /*rgs_per_segment=*/4);
  g.grid.rg_order = {3, 1, 2, 0};

  IResearchScanGlobalState::RgClaim claim;
  std::vector<uint32_t> order;
  while (g.ClaimRowGroup(claim)) {
    order.push_back(claim.rg);
  }
  EXPECT_EQ(order, (std::vector<uint32_t>{3, 1, 2, 0}));
}

// The permutation is one flat array over every slot, so a slot reads its own
// run of it and neither slot sees the other's.
TEST(SearchScanGrid, ScanOrderIsFlatAcrossSlots) {
  IResearchScanGlobalState g;
  FillGridWith(g, {3, 2});
  g.grid.rg_order = {2, 0, 1, 1, 0};

  const auto per_worker = DrainRoundRobin(g, 2);
  ASSERT_EQ(per_worker.size(), 2U);
  EXPECT_EQ(per_worker[0], (std::vector<Item>{{0, 2}, {0, 0}, {0, 1}}));
  EXPECT_EQ(per_worker[1], (std::vector<Item>{{1, 1}, {1, 0}}));
}

// A claim takes a RUN of row groups: consecutive claims by one worker are
// consecutive positions of one slot's policy, and no atomic is touched inside
// a run. Every row group is still claimed exactly once.
TEST(SearchScanGrid, ClaimsRunsOfRowGroups) {
  for (const auto run : {2U, 4U, 8U}) {
    IResearchScanGlobalState g;
    FillGridWith(g, {17, 17}, run);
    const auto per_worker = DrainRoundRobin(g, 4);
    std::set<Item> unique;
    size_t total = 0;
    size_t inside_run = 0;
    for (const auto& claims : per_worker) {
      total += claims.size();
      for (size_t i = 1; i < claims.size(); ++i) {
        // A step inside a run keeps the segment and advances one row group.
        // The only other step a worker may take is the one that opens a new
        // run, and a run always begins at a multiple of its length.
        const bool inside = claims[i].first == claims[i - 1].first &&
                            claims[i].second == claims[i - 1].second + 1;
        inside_run += inside ? 1 : 0;
        EXPECT_TRUE(inside || claims[i].second % run == 0)
          << "run: " << run << " at " << i;
      }
      unique.insert(claims.begin(), claims.end());
    }
    EXPECT_EQ(total, 34U) << "run: " << run;
    EXPECT_EQ(unique.size(), 34U) << "run: " << run;
    // A gate that would otherwise pass with every claim its own run.
    EXPECT_GT(inside_run, 34U - 34U / run - 4) << "run: " << run;
  }
}

// The ts_dict grid: `units_per_segment` term ranges of one field in each of
// `segments` segments, which is what BuildTermRangeGrid produces when every
// field splits evenly.
void FillTermGrid(IResearchScanGlobalState& g, uint32_t segments,
                  uint32_t units_per_segment) {
  g.term_grid.units.clear();
  g.term_grid.slots =
    std::vector<IResearchScanGlobalState::TermRangeSlot>(segments);
  for (uint32_t i = 0; i < segments; ++i) {
    auto& slot = g.term_grid.slots[i];
    slot.seg = i;
    slot.begin = static_cast<uint32_t>(g.term_grid.units.size());
    slot.count = units_per_segment;
    for (uint32_t u = 0; u < units_per_segment; ++u) {
      g.term_grid.units.emplace_back(0, irs::TermRange{}, u == 0);
    }
  }
  g.total_segments = segments;
  g.claimable_segments = segments;
}

std::vector<std::vector<Item>> DrainTermRangesRoundRobin(
  IResearchScanGlobalState& g, uint32_t workers) {
  std::vector<IResearchScanGlobalState::TermRangeClaim> claims(workers);
  std::vector<std::vector<Item>> per_worker(workers);
  for (bool progress = true; progress;) {
    progress = false;
    for (uint32_t w = 0; w < workers; ++w) {
      if (g.ClaimTermRange(claims[w])) {
        per_worker[w].emplace_back(claims[w].seg, claims[w].unit);
        progress = true;
      }
    }
  }
  return per_worker;
}

// The thesis for term enumeration: one segment's dictionary is not one work
// item. Every worker gets a share of its term ranges, and the shares differ by
// at most one claim.
TEST(SearchScanGrid, TermRangesSpreadOverEveryWorker) {
  for (const auto workers : {1U, 2U, 8U, 32U}) {
    IResearchScanGlobalState g;
    FillTermGrid(g, /*segments=*/1, /*units_per_segment=*/79);
    const auto per_worker = DrainTermRangesRoundRobin(g, workers);
    std::set<Item> unique;
    size_t total = 0;
    size_t smallest = 79;
    size_t largest = 0;
    for (const auto& claims : per_worker) {
      total += claims.size();
      smallest = std::min(smallest, claims.size());
      largest = std::max(largest, claims.size());
      for (const auto& item : claims) {
        EXPECT_EQ(item.first, 0U);
        unique.insert(item);
      }
    }
    EXPECT_EQ(total, 79U);
    EXPECT_EQ(unique.size(), 79U);
    EXPECT_GT(smallest, 0U);
    EXPECT_LE(largest - smallest, 1U);
    EXPECT_EQ(g.ClaimedTermRanges(), 79U);
  }
}

// Affinity and stealing, the same policy the row-group grid follows: a worker
// stays on the segment it classified, and takes another's ranges only once
// there is no fresh segment left.
TEST(SearchScanGrid, TermRangeAffinityAndStealing) {
  {
    IResearchScanGlobalState g;
    FillTermGrid(g, /*segments=*/4, /*units_per_segment=*/4);
    const auto per_worker = DrainTermRangesRoundRobin(g, 4);
    std::set<uint32_t> segments;
    for (const auto& claims : per_worker) {
      ASSERT_EQ(claims.size(), 4U);
      for (const auto& item : claims) {
        EXPECT_EQ(item.first, claims.front().first);
      }
      segments.insert(claims.front().first);
    }
    EXPECT_EQ(segments.size(), 4U);
  }
  {
    IResearchScanGlobalState g;
    FillTermGrid(g, /*segments=*/2, /*units_per_segment=*/0);
    g.term_grid.slots[1].count = 5;
    g.term_grid.units.resize(5);
    const auto per_worker = DrainTermRangesRoundRobin(g, 2);
    size_t total = 0;
    for (const auto& claims : per_worker) {
      total += claims.size();
    }
    EXPECT_EQ(total, 5U);
    // The worker that found its segment empty moved to the other one.
    EXPECT_FALSE(per_worker[0].empty());
    EXPECT_FALSE(per_worker[1].empty());
  }
}

TEST(SearchScanGrid, MaxThreadsFollowsTheClaimableWork) {
  IResearchScanGlobalState g;
  g.pool_threads = 32;

  // The whole-reader live count is one answer, computed at init.
  g.mode = ScanMode::CountFast;
  FillGrid(g, 4, 8);
  EXPECT_EQ(g.MaxThreads(), 1U);

  // One worker per row group, never more than the pool could run.
  g.mode = ScanMode::Stream;
  FillGrid(g, 1, 79);
  EXPECT_EQ(g.MaxThreads(), 32U);
  g.pool_threads = 8;
  EXPECT_EQ(g.MaxThreads(), 8U);
  g.pool_threads = 32;
  FillGrid(g, 2, 3);
  EXPECT_EQ(g.MaxThreads(), 6U);

  // Term enumeration claims term ranges: the row-group grid says nothing
  // about how many workers it can use, the term grid does.
  g.mode = ScanMode::TsDict;
  FillGrid(g, 5, 20);
  FillTermGrid(g, 5, 3);
  EXPECT_EQ(g.MaxThreads(), 15U);
  FillTermGrid(g, 5, 0);
  EXPECT_EQ(g.MaxThreads(), 1U);
  g.pool_threads = 4;
  FillTermGrid(g, 5, 3);
  EXPECT_EQ(g.MaxThreads(), 4U);
  g.pool_threads = 32;

  // A scored scan additionally gets a worker per segment for the prepare
  // phase, which walks every segment for the corpus statistics.
  g.mode = ScanMode::TopK;
  g.scorer_obj = std::make_unique<irs::VectorSimilarityScorer>();
  FillGrid(g, 9, 1);
  g.total_segments = 20;  // segments the filter classification excluded
  EXPECT_EQ(g.MaxThreads(), 20U);

  // Never zero: an empty grid still runs one worker.
  g.scorer_obj.reset();
  g.mode = ScanMode::Stream;
  FillGrid(g, 0, 0);
  EXPECT_EQ(g.MaxThreads(), 1U);
}

}  // namespace
}  // namespace sdb::connector
