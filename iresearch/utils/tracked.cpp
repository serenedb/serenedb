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

#include "iresearch/utils/tracked.hpp"

#include <absl/base/no_destructor.h>

#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/fault_hits.hpp"

namespace irs {
namespace {

struct BudgetAllocatorData final : duckdb::PrivateAllocatorData {
  explicit BudgetAllocatorData(MemoryBudget* budget) noexcept
    : budget{budget} {}

  MemoryBudget* budget;
};

MemoryBudget* BudgetOf(duckdb::PrivateAllocatorData* data) noexcept {
  return data->Cast<BudgetAllocatorData>().budget;
}

duckdb::data_ptr_t ChargedAllocate(duckdb::PrivateAllocatorData* data,
                                   duckdb::idx_t size) {
  auto* budget = BudgetOf(data);
  ChargeMemory(budget, kMemoryTag, size);
  absl::Cleanup release = [&] { FreeCharge(budget, kMemoryTag, size); };
  auto* p = duckdb::Allocator::DefaultAllocate(data, size);
  if (p) {
    std::move(release).Cancel();
  }
  return p;
}

void ChargedFree(duckdb::PrivateAllocatorData* data, duckdb::data_ptr_t p,
                 duckdb::idx_t size) {
  duckdb::Allocator::DefaultFree(data, p, size);
  FreeCharge(BudgetOf(data), kMemoryTag, size);
}

duckdb::data_ptr_t ChargedReallocate(duckdb::PrivateAllocatorData* data,
                                     duckdb::data_ptr_t p,
                                     duckdb::idx_t old_size,
                                     duckdb::idx_t size) {
  auto* budget = BudgetOf(data);
  if (size <= old_size) {
    auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
    if (r) {
      FreeCharge(budget, kMemoryTag, old_size - size);
    }
    return r;
  }
  ChargeMemory(budget, kMemoryTag, size - old_size);
  absl::Cleanup release = [&] {
    FreeCharge(budget, kMemoryTag, size - old_size);
  };
  auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
  if (r) {
    std::move(release).Cancel();
  }
  return r;
}

}  // namespace

void ChargeMemory(MemoryBudget* budget, duckdb::MemoryTag tag, size_t size) {
  if (!budget) {
    auto charge = ReserveMemory(tag, size);
    charge.size = 0;
    return;
  }
  SDB_ASSERT(budget->tag == tag);
  if (budget->size < size) {
    budget->Merge(ReserveMemory(tag, size - budget->size));
  }
  budget->size -= size;
}

void FreeCharge(MemoryBudget* budget, duckdb::MemoryTag tag,
                size_t size) noexcept {
  if (!budget) {
    FreeReservedMemory(tag, size);
    return;
  }
  SDB_ASSERT(budget->tag == tag);
  budget->size += size;
}

duckdb::Allocator& TrackedDuckAllocator() {
  static absl::NoDestructor<duckdb::Allocator> gAllocator{
    ChargedAllocate, ChargedFree, ChargedReallocate,
    duckdb::make_uniq<BudgetAllocatorData>(nullptr)};
  return *gAllocator;
}

duckdb::Allocator BudgetAllocator(MemoryBudget* budget) {
  return duckdb::Allocator{ChargedAllocate, ChargedFree, ChargedReallocate,
                           duckdb::make_uniq<BudgetAllocatorData>(budget)};
}

duckdb::TempBufferPoolReservation ReserveMemory(duckdb::MemoryTag tag,
                                                size_t size) {
  SDB_IF_FAILURE("irs::ReserveMemory") {
    if (FaultHitFires("irs::ReserveMemory")) {
      throw duckdb::OutOfMemoryException(
        "failed to reserve memory data of size %s (fault point)",
        duckdb::StringUtil::BytesToHumanReadableString(size));
    }
  }
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  return manager->ReserveMemory(tag, size);
}

void FreeReservedMemory(duckdb::MemoryTag tag, size_t size) noexcept {
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  manager->FreeReservedMemory(tag, size);
}

}  // namespace irs
