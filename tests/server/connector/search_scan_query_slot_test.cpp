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

// The per-segment prepared query is published lock-free: `published` is the
// whole read path and null means "not built yet", never "wait". These pin what
// that costs when two workers reach one unbuilt segment together -- exactly one
// publication wins, the loser drops its own build and returns the winner's, and
// neither waits for the other. The race is legal only because the paths that
// can reach it are unscored (no PrepareCollector), which EnsureSegmentQuery
// states at the CAS; these tests drive that function itself rather than a copy
// of its protocol.

#include <gtest/gtest.h>

#include <atomic>
#include <iresearch/search/filter.hpp>
#include <memory>
#include <thread>
#include <vector>

#include "connector/duckdb_search_full_scan.hpp"

namespace sdb::connector {
namespace {

std::atomic_uint32_t gQueriesAlive{0};

// A prepared query that is never executed: it exists to be published, or to be
// the copy the loser drops, which is why it counts its own lifetime.
class CountingQuery : public irs::QueryBuilder {
 public:
  explicit CountingQuery(const irs::SubReader& segment) noexcept
    : irs::QueryBuilder{segment} {
    gQueriesAlive.fetch_add(1, std::memory_order_relaxed);
  }

  ~CountingQuery() override {
    gQueriesAlive.fetch_sub(1, std::memory_order_relaxed);
  }

  irs::DocIterator::ptr Execute(const irs::ExecutionContext&,
                                const irs::StatsBuffer&) const override {
    return irs::DocIterator::empty();
  }

  void Visit(irs::PreparedStateVisitor&, irs::score_t) const override {}

  irs::score_t Boost() const noexcept override { return irs::kNoBoost; }
};

// PrepareSegment parks inside the build until the test releases it, so the two
// builds provably overlap instead of merely being scheduled close together.
class GatedFilter final : public irs::Filter {
 public:
  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext&) const final {
    inside.fetch_add(1, std::memory_order_release);
    while (!open.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    builds.fetch_add(1, std::memory_order_relaxed);
    return irs::memory::make_managed<CountingQuery>(segment);
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<irs::Empty>::id();
  }

  mutable std::atomic_uint32_t inside{0};
  mutable std::atomic_uint32_t builds{0};
  std::atomic_bool open{false};
};

void SpinUntil(const std::atomic_uint32_t& counter, uint32_t value) {
  while (counter.load(std::memory_order_acquire) < value) {
    std::this_thread::yield();
  }
}

// Two workers inside one unbuilt slot at the same time: both build, one
// publishes, the loser returns the published query and destroys its own.
TEST(SearchScanQuerySlot, RacingBuildersPublishExactlyOnce) {
  ASSERT_EQ(gQueriesAlive.load(), 0U);
  GatedFilter filter;
  IResearchScanGlobalState g;
  g.total_segments = 1;
  g.InitQuerySlots();
  g.filter = &filter;

  std::array<const irs::QueryBuilder*, 2> got{};
  std::vector<std::thread> workers;
  workers.reserve(got.size());
  for (uint32_t w = 0; w != got.size(); ++w) {
    workers.emplace_back([&g, &got, w] {
      irs::PrepareCollector* collector = nullptr;
      got[w] = &g.EnsureSegmentQuery(collector, irs::SubReader::empty(), 0);
      // Unscored, so no collector slot was taken -- the property that makes a
      // duplicate build free of accumulating side effects.
      EXPECT_EQ(collector, nullptr);
    });
  }
  // Both are inside PrepareSegment on the same unbuilt slot before either can
  // reach the CAS: this is the race, not an interleaving that happens to work.
  SpinUntil(filter.inside, 2);
  EXPECT_EQ(g.queries[0].published.load(), nullptr);
  filter.open.store(true, std::memory_order_release);
  for (auto& w : workers) {
    w.join();
  }

  EXPECT_EQ(filter.builds.load(), 2U);
  const auto* published = g.queries[0].published.load();
  ASSERT_NE(published, nullptr);
  // One publication wins and owns the build; both callers hold that one.
  EXPECT_EQ(g.queries[0].owned.get(), published);
  EXPECT_EQ(got[0], published);
  EXPECT_EQ(got[1], published);
  // The loser's copy is gone, not leaked and not kept alive by the slot.
  EXPECT_EQ(gQueriesAlive.load(), 1U);
  EXPECT_EQ(g.collector_slots.load(), 0U);
  g.queries.clear();
  EXPECT_EQ(gQueriesAlive.load(), 0U);
}

// The loser blocking the winner would be invisible above, where both builds end
// together. Here one worker is held inside its build until the other has
// already returned: an unbuilt slot never makes a worker wait.
TEST(SearchScanQuerySlot, NoWorkerWaitsOnAnUnbuiltSlot) {
  ASSERT_EQ(gQueriesAlive.load(), 0U);
  GatedFilter slow;
  GatedFilter fast;
  fast.open.store(true, std::memory_order_release);
  IResearchScanGlobalState g;
  g.total_segments = 1;
  g.InitQuerySlots();

  const irs::QueryBuilder* held = nullptr;
  g.filter = &slow;
  std::thread holder{[&g, &held] {
    irs::PrepareCollector* collector = nullptr;
    held = &g.EnsureSegmentQuery(collector, irs::SubReader::empty(), 0);
  }};
  SpinUntil(slow.inside, 1);

  // The other worker runs to completion while the first is still building.
  g.filter = &fast;
  irs::PrepareCollector* collector = nullptr;
  const auto* first =
    &g.EnsureSegmentQuery(collector, irs::SubReader::empty(), 0);
  EXPECT_EQ(fast.builds.load(), 1U);
  EXPECT_EQ(slow.builds.load(), 0U);
  EXPECT_EQ(g.queries[0].published.load(), first);

  slow.open.store(true, std::memory_order_release);
  holder.join();
  // The late builder still answers with the published query, not its own.
  EXPECT_EQ(slow.builds.load(), 1U);
  EXPECT_EQ(held, first);
  EXPECT_EQ(gQueriesAlive.load(), 1U);
  g.queries.clear();
  EXPECT_EQ(gQueriesAlive.load(), 0U);
}

// The same protocol under real contention: many workers, many slots, released
// together, repeated so the CAS is decided by the scheduler rather than by a
// gate the test holds.
TEST(SearchScanQuerySlot, ContendedPublicationOwnsOneBuildPerSegment) {
  constexpr uint32_t kSegments = 16;
  constexpr uint32_t kWorkers = 8;
  for (uint32_t round = 0; round != 32; ++round) {
    ASSERT_EQ(gQueriesAlive.load(), 0U);
    GatedFilter filter;
    filter.open.store(true, std::memory_order_release);
    IResearchScanGlobalState g;
    g.total_segments = kSegments;
    g.InitQuerySlots();
    g.filter = &filter;

    std::atomic_uint32_t ready{0};
    std::vector<std::vector<const irs::QueryBuilder*>> seen(kWorkers);
    std::vector<std::thread> workers;
    workers.reserve(kWorkers);
    for (uint32_t w = 0; w != kWorkers; ++w) {
      workers.emplace_back([&g, &ready, &seen, w] {
        auto& mine = seen[w];
        mine.resize(kSegments);
        irs::PrepareCollector* collector = nullptr;
        ready.fetch_add(1, std::memory_order_release);
        SpinUntil(ready, kWorkers);
        for (uint32_t seg = 0; seg != kSegments; ++seg) {
          mine[seg] =
            &g.EnsureSegmentQuery(collector, irs::SubReader::empty(), seg);
        }
      });
    }
    for (auto& w : workers) {
      w.join();
    }

    for (uint32_t seg = 0; seg != kSegments; ++seg) {
      const auto* published = g.queries[seg].published.load();
      ASSERT_NE(published, nullptr);
      EXPECT_EQ(g.queries[seg].owned.get(), published);
      for (uint32_t w = 0; w != kWorkers; ++w) {
        EXPECT_EQ(seen[w][seg], published);
      }
    }
    // Every build beyond the kSegments winners was dropped by its loser.
    EXPECT_EQ(gQueriesAlive.load(), kSegments);
    EXPECT_GE(filter.builds.load(), kSegments);
    g.queries.clear();
    EXPECT_EQ(gQueriesAlive.load(), 0U);
  }
}

}  // namespace
}  // namespace sdb::connector
