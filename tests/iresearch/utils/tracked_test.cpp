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

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_set.h>

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/detail/stats_arena.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/huge_page_allocator.hpp>
#include <iresearch/utils/query_memory.hpp>
#include <iresearch/utils/tracked.hpp>
#include <string_view>
#include <utility>

#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace {

constexpr size_t kMiB = size_t{1} << 20;
constexpr irs::field_id kName = 1;

irs::DirectoryReader BuildTermIndex(irs::Directory& dir, irs::doc_id_t n) {
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                       irs::tests::DefaultWriterOptions());
  EXPECT_NE(nullptr, writer);
  {
    auto trx = writer->GetBatch();
    for (irs::doc_id_t i = 0; i < n; ++i) {
      const duckdb::string_t value{i % 3 == 0 ? "b" : "a", 1};
      auto doc = trx.Insert();
      const auto d = doc.DocId();
      EXPECT_TRUE(doc.WithField(
        kName, irs::StringLayout::kStrings, irs::IndexFeatures::Freq,
        [&](irs::FieldInverter& field) {
          return field.InvertKeywords([&](auto&& emit) { emit(value, d); });
        }));
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

struct Counted : irs::memory::Managed {
  explicit Counted(int& destroyed) noexcept : destroyed{destroyed} {}
  ~Counted() override { ++destroyed; }

  int& destroyed;
};

duckdb::BufferManager& Manager() {
  return duckdb::BufferManager::GetBufferManager(
    irs::DuckDBEngine::Instance().instance());
}

class MemoryLimit {
 public:
  explicit MemoryLimit(size_t headroom) : _old{Manager().GetMaxMemory()} {
    Manager().SetMemoryLimit(Manager().GetUsedMemory() + headroom);
  }
  ~MemoryLimit() { Manager().SetMemoryLimit(_old); }

 private:
  size_t _old;
};

struct Object : irs::Tracked {
  uint64_t words[64];
};

struct Huge : irs::Tracked {
  uint8_t bytes[1024 * kMiB];
};

TEST(TrackedTest, VectorChargesItsCapacity) {
  const auto base = TrackedMemory();
  {
    irs::TrackedVector<uint64_t> values;
    values.reserve(kMiB);
    EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, ObjectChargesItsSize) {
  const auto base = TrackedMemory();
  auto object = std::make_unique<Object>();
  EXPECT_EQ(base + sizeof(Object), TrackedMemory());
  object.reset();
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, QueriesAndPlansChargePerArenaBlock) {
  if constexpr (irs::kArenaRewindFrees) {
    GTEST_SKIP();
  }
  const auto base = TrackedMemory();
  {
    irs::QueryMemory query_memory;
    irs::PlanMemory plan_memory;
    const auto& segment = irs::SubReader::empty();
    const auto build = [&] {
      irs::QueryBuilder::ptr query =
        query_memory.Make<irs::AllQuery>(segment, 1.f);
      auto node = query->PlanLead({.memory = plan_memory});
      EXPECT_TRUE(node);
      return std::pair{std::move(query), std::move(node)};
    };
    {
      auto first = build();
      EXPECT_LT(base, TrackedMemory());
    }
    query_memory.Rewind();
    plan_memory.Rewind();
    const auto warm = TrackedMemory();
    for (int segment_no = 0; segment_no != 4; ++segment_no) {
      {
        auto next = build();
        EXPECT_EQ(warm, TrackedMemory());
      }
      query_memory.Rewind();
      plan_memory.Rewind();
    }
    EXPECT_EQ(warm, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, ReplanningAfterRewindChargesNothing) {
  if constexpr (irs::kArenaRewindFrees) {
    GTEST_SKIP();
  }
  irs::MemoryDirectory dir;
  const auto reader = BuildTermIndex(dir, 3000);
  ASSERT_EQ(1, reader.size());
  const auto& segment = reader[0];

  const irs::BM25 scorer;
  irs::ByTerm filter;
  *filter.mutable_field_id() = kName;
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  irs::StatsArena stats{duckdb::Allocator::DefaultAllocator()};
  irs::PreparedCollector collector{filter, scorer, stats, 1};
  irs::QueryMemory query_memory;
  const auto query = filter.PrepareSegment(
    segment, {.memory = query_memory, .collector = collector.Get()});
  collector.Finish();
  ASSERT_NE(nullptr, query);

  irs::PlanMemory plan_memory;
  irs::ColumnArgsFetcher fetcher;
  const auto check = [&](std::string_view kind, auto&& plan) {
    SCOPED_TRACE(kind);
    int64_t planned = 0;
    const auto cycle = [&] {
      {
        auto root = plan();
        EXPECT_NE(nullptr, root);
        planned = TrackedMemory();
      }
      fetcher.Clear();
      plan_memory.Rewind();
    };
    cycle();
    cycle();
    const auto warm_planned = planned;
    const auto warm = TrackedMemory();
    for (int segment_no = 0; segment_no != 4; ++segment_no) {
      cycle();
      EXPECT_EQ(warm_planned, planned);
      EXPECT_EQ(warm, TrackedMemory());
    }
  };
  check("count", [&] { return query->PlanCount({.memory = plan_memory}); });
  check("docs", [&] { return query->PlanDocs({.memory = plan_memory}); });
  check("hits", [&] {
    return query->PlanScored(
      {.memory = plan_memory, .scorer = scorer, .fetcher = fetcher});
  });
  check("top", [&] {
    return query->PlanTop(
      {.memory = plan_memory, .scorer = scorer, .fetcher = fetcher, .k = 10});
  });
}

TEST(TrackedTest, ArenaObjectsAreDestroyedOnceByTheirOwner) {
  int destroyed = 0;
  irs::PlanMemory plan_memory;
  {
    auto first = plan_memory.Make<Counted>(destroyed);
    auto second = plan_memory.Make<Counted>(destroyed);
    EXPECT_EQ(0, destroyed);
    first.reset();
    EXPECT_EQ(1, destroyed);
  }
  EXPECT_EQ(2, destroyed);
  plan_memory.Rewind();
  EXPECT_EQ(2, destroyed);

  irs::QueryMemory query_memory;
  {
    auto query = query_memory.Make<Counted>(destroyed);
    EXPECT_EQ(2, destroyed);
  }
  EXPECT_EQ(3, destroyed);
  query_memory.Rewind();
  EXPECT_EQ(3, destroyed);

  auto reused = plan_memory.Make<Counted>(destroyed);
  reused.reset();
  EXPECT_EQ(4, destroyed);
}

TEST(TrackedTest, ArenaAlignsOverAlignedObjects) {
  struct alignas(64) Wide {
    uint8_t bytes[64];
  };
  irs::MemoryArena arena;
  arena.Allocate(3, 1);
  auto* wide = arena.Allocate<Wide>(2);
  EXPECT_EQ(0, reinterpret_cast<uintptr_t>(wide) % alignof(Wide));
  irs::memory::ArenaVector<Wide> vector{arena};
  vector.resize(3);
  EXPECT_EQ(0, reinterpret_cast<uintptr_t>(vector.data()) % alignof(Wide));
}

TEST(TrackedTest, StringChargesItsCapacity) {
  const auto base = TrackedMemory();
  {
    irs::TrackedString text;
    text.reserve(kMiB);
    EXPECT_LE(base + static_cast<int64_t>(kMiB), TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, ChargeFollowsItsSize) {
  const auto base = TrackedMemory();
  {
    irs::MemoryCharge charge{kMiB};
    EXPECT_EQ(base + kMiB, TrackedMemory());
    charge.Resize(4 * kMiB);
    EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
    charge.Resize(kMiB / 2);
    EXPECT_EQ(base + kMiB / 2, TrackedMemory());
    irs::MemoryCharge moved{std::move(charge)};
    EXPECT_EQ(0, charge.Bytes());
    EXPECT_EQ(base + kMiB / 2, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, ChargeOverTheLimitThrows) {
  const auto base = TrackedMemory();
  {
    MemoryLimit limit{16 * kMiB};
    irs::MemoryCharge charge{kMiB};
    EXPECT_THROW(charge.Resize(1024 * kMiB), duckdb::OutOfMemoryException);
    EXPECT_EQ(kMiB, charge.Bytes());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, ArrayDestroysItsElements) {
  const auto base = TrackedMemory();
  auto vectors = irs::MakeTrackedArray<irs::TrackedVector<uint64_t>>(4);
  for (size_t i = 0; i != 4; ++i) {
    vectors[i].reserve(kMiB);
  }
  EXPECT_LE(base + 32 * kMiB, TrackedMemory());
  vectors.reset();
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, DuckAllocatorChargesItsAllocations) {
  auto& allocator = irs::TrackedDuckAllocator();
  const auto base = TrackedMemory();
  auto* data = allocator.AllocateData(kMiB);
  EXPECT_EQ(base + kMiB, TrackedMemory());
  data = allocator.ReallocateData(data, kMiB, 4 * kMiB);
  EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
  data = allocator.ReallocateData(data, 4 * kMiB, kMiB / 2);
  EXPECT_EQ(base + kMiB / 2, TrackedMemory());
  allocator.FreeData(data, kMiB / 2);
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, DuckAllocatorOverTheLimitThrows) {
  auto& allocator = irs::TrackedDuckAllocator();
  const auto base = TrackedMemory();
  {
    MemoryLimit limit{16 * kMiB};
    EXPECT_THROW(allocator.AllocateData(1024 * kMiB),
                 duckdb::OutOfMemoryException);
    duckdb::idx_t size = kMiB;
    auto* data = allocator.AllocateData(size);
    absl::Cleanup release = [&] { allocator.FreeData(data, size); };
    EXPECT_THROW(
      {
        data = allocator.ReallocateData(data, size, 1024 * kMiB);
        size = 1024 * kMiB;
      },
      duckdb::OutOfMemoryException);
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, HugePageVectorChargesItsCapacity) {
  const auto base = TrackedMemory();
  {
    irs::HugePageVector<uint8_t> values(4 * kMiB);
    EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, AllocationOverTheLimitThrows) {
  const auto base = TrackedMemory();
  {
    MemoryLimit limit{16 * kMiB};
    irs::TrackedVector<uint8_t> values;
    EXPECT_THROW(values.reserve(1024 * kMiB), duckdb::OutOfMemoryException);
    EXPECT_EQ(0, values.capacity());
    EXPECT_THROW(std::make_unique<Huge>(), duckdb::OutOfMemoryException);
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetedVectorDrawsFromTheReservation) {
  const auto base = TrackedMemory();
  {
    auto budget = irs::ReserveMemory(irs::kMemoryTag, 8 * kMiB);
    EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
    {
      irs::TrackedVector<uint64_t> values{
        irs::TrackedAllocator<uint64_t>{&budget}};
      values.reserve(kMiB / 2);
      EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
      EXPECT_EQ(4 * kMiB, budget.size);
      irs::TrackedVector<uint64_t> moved{std::move(values)};
      EXPECT_EQ(&budget, moved.get_allocator().budget);
      irs::TrackedVector<uint64_t> copied{moved};
      EXPECT_EQ(nullptr, copied.get_allocator().budget);
    }
    EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
    EXPECT_EQ(8 * kMiB, budget.size);
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetOverflowChargesThePool) {
  const auto base = TrackedMemory();
  {
    auto budget = irs::ReserveMemory(irs::kMemoryTag, kMiB);
    irs::MemoryCharge charge{&budget, 4 * kMiB};
    EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
    EXPECT_EQ(0, budget.size);
    charge.Resize(0);
    EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
    EXPECT_EQ(4 * kMiB, budget.size);
    budget.Resize(0);
    EXPECT_EQ(base, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, HashSetSurvivesAFailedGrowth) {
  absl::flat_hash_set<uint64_t, absl::Hash<uint64_t>, std::equal_to<>,
                      irs::TrackedAllocator<uint64_t>>
    set;
  {
    MemoryLimit limit{0};
    EXPECT_THROW(set.reserve(kMiB), duckdb::OutOfMemoryException);
  }
  for (uint64_t i = 0; i < 1000; ++i) {
    set.insert(i);
  }
  {
    MemoryLimit limit{0};
    EXPECT_THROW(set.reserve(kMiB), duckdb::OutOfMemoryException);
  }
  ASSERT_EQ(1000, set.size());
  for (uint64_t i = 0; i < 1000; ++i) {
    EXPECT_TRUE(set.contains(i));
  }
}

TEST(TrackedTest, BudgetFreesWhatIsReturnedAfterItsRelease) {
  const auto base = TrackedMemory();
  auto budget = irs::ReserveMemory(irs::kMemoryTag, 4 * kMiB);
  {
    irs::MemoryCharge charge{&budget, 3 * kMiB};
    budget.Resize(0);
    EXPECT_EQ(base + 3 * kMiB, TrackedMemory());
  }
  EXPECT_EQ(3 * kMiB, budget.size);
  EXPECT_EQ(base + 3 * kMiB, TrackedMemory());
  budget.Resize(0);
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetedAllocationPassesAtTheLimit) {
  const auto base = TrackedMemory();
  {
    auto budget = irs::ReserveMemory(irs::kMemoryTag, 8 * kMiB);
    MemoryLimit limit{0};
    EXPECT_THROW(irs::MemoryCharge{kMiB}, duckdb::OutOfMemoryException);
    EXPECT_THROW(budget.Merge(irs::ReserveMemory(irs::kMemoryTag, 8 * kMiB)),
                 duckdb::OutOfMemoryException);
    auto array = irs::MakeTrackedArray<uint64_t>(kMiB / 2, &budget);
    auto allocator = irs::BudgetAllocator(&budget);
    auto* data = allocator.AllocateData(2 * kMiB);
    allocator.FreeData(data, 2 * kMiB);
    EXPECT_EQ(4 * kMiB, budget.size);
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetFundsTransientBlocksAtTheLimit) {
  auto& manager = Manager();
  auto& block_manager = manager.GetTemporaryBlockManager();
  const auto block_size = block_manager.GetBlockSize();
  const auto base = TrackedMemory();
  {
    auto budget = irs::ReserveMemory(irs::kMemoryTag, 4 * block_size);
    auto short_budget = irs::ReserveMemory(irs::kMemoryTag, block_size / 2);
    MemoryLimit limit{0};
    EXPECT_THROW(manager.RegisterTransientMemory(block_size, block_manager),
                 duckdb::OutOfMemoryException);
    EXPECT_THROW(manager.RegisterTransientMemoryFrom(block_size, block_manager,
                                                     short_budget),
                 duckdb::OutOfMemoryException);
    EXPECT_EQ(block_size / 2, short_budget.size);
    for (const auto size : {block_size, block_size / 2}) {
      auto block =
        manager.RegisterTransientMemoryFrom(size, block_manager, budget);
      auto& memory = block->GetMemory();
      const auto charge = memory.GetMemoryUsage();
      EXPECT_LE(size, charge);
      EXPECT_EQ(4 * block_size - charge, budget.size);
      {
        auto lock = memory.GetLock();
        budget.Merge(memory.TakeMemoryCharge(lock));
      }
      EXPECT_EQ(0, memory.GetMemoryUsage());
      block.reset();
      EXPECT_EQ(4 * block_size, budget.size);
    }
    EXPECT_EQ(base + 4 * block_size + block_size / 2, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BufferAllocatorReallocOverTheLimitThrows) {
  auto& allocator =
    duckdb::BufferAllocator::Get(irs::DuckDBEngine::Instance().instance());
  MemoryLimit limit{16 * kMiB};
  duckdb::idx_t size = kMiB;
  auto* data = allocator.AllocateData(size);
  absl::Cleanup release = [&] { allocator.FreeData(data, size); };
  EXPECT_THROW(
    {
      data = allocator.ReallocateData(data, size, 1024 * kMiB);
      size = 1024 * kMiB;
    },
    duckdb::OutOfMemoryException);
}

}  // namespace
