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

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/utils/huge_page_allocator.hpp>
#include <iresearch/utils/tracked.hpp>

#include "tests_shared.hpp"

namespace {

constexpr size_t kMiB = size_t{1} << 20;

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
    irs::MemoryBudget budget;
    budget.Reserve(8 * kMiB);
    EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
    {
      irs::TrackedVector<uint64_t> values{
        irs::TrackedAllocator<uint64_t>{&budget}};
      values.reserve(kMiB / 2);
      EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
      irs::TrackedVector<uint64_t> moved{std::move(values)};
      EXPECT_EQ(&budget, moved.get_allocator().budget);
      irs::TrackedVector<uint64_t> copied{moved};
      EXPECT_EQ(nullptr, copied.get_allocator().budget);
    }
    EXPECT_EQ(base + 8 * kMiB, TrackedMemory());
    EXPECT_EQ(8 * kMiB, budget.Reserved());
    EXPECT_EQ(0, budget.Overflow());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetOverflowChargesThePool) {
  const auto base = TrackedMemory();
  {
    irs::MemoryBudget budget;
    budget.Reserve(kMiB);
    irs::MemoryCharge charge{&budget, 4 * kMiB};
    EXPECT_EQ(base + 4 * kMiB, TrackedMemory());
    EXPECT_EQ(3 * kMiB, budget.Overflow());
    charge.Resize(0);
    budget.Release();
    EXPECT_EQ(base, TrackedMemory());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetedAllocationPassesAtTheLimit) {
  const auto base = TrackedMemory();
  {
    irs::MemoryBudget budget;
    budget.Reserve(8 * kMiB);
    MemoryLimit limit{0};
    EXPECT_THROW(irs::MemoryCharge{kMiB}, duckdb::OutOfMemoryException);
    EXPECT_THROW(budget.Reserve(16 * kMiB), duckdb::OutOfMemoryException);
    auto array = irs::MakeTrackedArray<uint64_t>(kMiB / 2, &budget);
    auto* data = budget.Allocator().AllocateData(2 * kMiB);
    budget.Allocator().FreeData(data, 2 * kMiB);
    EXPECT_EQ(0, budget.Overflow());
  }
  EXPECT_EQ(base, TrackedMemory());
}

TEST(TrackedTest, BudgetFundsTransientBlocksAtTheLimit) {
  auto& manager = Manager();
  auto& block_manager = manager.GetTemporaryBlockManager();
  const auto block_size = block_manager.GetBlockSize();
  const auto base = TrackedMemory();
  {
    irs::MemoryBudget budget;
    budget.Reserve(4 * block_size + kMiB);
    MemoryLimit limit{0};
    EXPECT_THROW(manager.RegisterTransientMemory(block_size, block_manager),
                 duckdb::OutOfMemoryException);
    auto source = budget.Detach(2 * block_size + kMiB);
    absl::Cleanup attach = [&] { budget.Attach(source); };
    auto handle =
      manager.AllocateTransientMemory(block_size, block_manager, source);
    const auto charge = handle.GetBlockHandle()->GetMemory().GetMemoryUsage();
    EXPECT_LE(block_size, charge);
    EXPECT_EQ(2 * block_size + kMiB - charge, source.size);
    budget.Attach(source);
    EXPECT_EQ(4 * block_size + kMiB - charge, budget.Reserved());
    budget.Reclaim(charge);
    handle.Destroy();
    EXPECT_EQ(4 * block_size + kMiB, budget.Reserved());
    EXPECT_EQ(0, budget.Overflow());
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
