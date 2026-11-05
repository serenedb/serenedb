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

#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/duckdb_engine.hpp"

namespace irs {
namespace {

duckdb::data_ptr_t TrackedAllocate(duckdb::PrivateAllocatorData* data,
                                   duckdb::idx_t size) {
  ReserveMemory(kMemoryTag, size);
  absl::Cleanup release = [&] { FreeReservedMemory(kMemoryTag, size); };
  auto* p = duckdb::Allocator::DefaultAllocate(data, size);
  if (p) {
    std::move(release).Cancel();
  }
  return p;
}

void TrackedFree(duckdb::PrivateAllocatorData* data, duckdb::data_ptr_t p,
                 duckdb::idx_t size) {
  duckdb::Allocator::DefaultFree(data, p, size);
  FreeReservedMemory(kMemoryTag, size);
}

duckdb::data_ptr_t TrackedReallocate(duckdb::PrivateAllocatorData* data,
                                     duckdb::data_ptr_t p,
                                     duckdb::idx_t old_size,
                                     duckdb::idx_t size) {
  if (size <= old_size) {
    auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
    if (r) {
      FreeReservedMemory(kMemoryTag, old_size - size);
    }
    return r;
  }
  ReserveMemory(kMemoryTag, size - old_size);
  absl::Cleanup release = [&] {
    FreeReservedMemory(kMemoryTag, size - old_size);
  };
  auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
  if (r) {
    std::move(release).Cancel();
  }
  return r;
}

struct BudgetAllocatorData final : duckdb::PrivateAllocatorData {
  explicit BudgetAllocatorData(MemoryBudget& budget) noexcept
    : budget{&budget} {}

  MemoryBudget* budget;
};

MemoryBudget& BudgetOf(duckdb::PrivateAllocatorData* data) noexcept {
  return *data->Cast<BudgetAllocatorData>().budget;
}

duckdb::data_ptr_t BudgetAllocate(duckdb::PrivateAllocatorData* data,
                                  duckdb::idx_t size) {
  auto& budget = BudgetOf(data);
  budget.Take(size);
  auto* p = duckdb::Allocator::DefaultAllocate(data, size);
  if (!p) {
    budget.Return(size);
  }
  return p;
}

void BudgetFree(duckdb::PrivateAllocatorData* data, duckdb::data_ptr_t p,
                duckdb::idx_t size) {
  duckdb::Allocator::DefaultFree(data, p, size);
  BudgetOf(data).Return(size);
}

duckdb::data_ptr_t BudgetReallocate(duckdb::PrivateAllocatorData* data,
                                    duckdb::data_ptr_t p,
                                    duckdb::idx_t old_size,
                                    duckdb::idx_t size) {
  auto& budget = BudgetOf(data);
  if (size <= old_size) {
    auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
    if (r) {
      budget.Return(old_size - size);
    }
    return r;
  }
  budget.Take(size - old_size);
  auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
  if (!r) {
    budget.Return(size - old_size);
  }
  return r;
}

duckdb::BufferPool& Pool() {
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  return manager->GetBufferPool();
}

}  // namespace

MemoryBudget::MemoryBudget()
  : _allocator{BudgetAllocate, BudgetFree, BudgetReallocate,
               duckdb::make_uniq<BudgetAllocatorData>(*this)} {}

MemoryBudget::~MemoryBudget() {
  SDB_ASSERT(_lent == 0);
  FreeReservedMemory(kMemoryTag, _reserved);
}

void MemoryBudget::Reserve(size_t bytes) {
  absl::MutexLock lock{&_mutex};
  const auto available = _reserved - _lent;
  if (bytes <= available) {
    return;
  }
  ReserveMemory(kMemoryTag, bytes - available);
  _reserved += bytes - available;
}

void MemoryBudget::Release(size_t keep) noexcept {
  absl::MutexLock lock{&_mutex};
  const auto available = _reserved - _lent;
  if (available <= keep) {
    return;
  }
  FreeReservedMemory(kMemoryTag, available - keep);
  _reserved = _lent + keep;
}

void MemoryBudget::Take(size_t bytes) {
  absl::MutexLock lock{&_mutex};
  if (const auto lent = _lent + bytes; lent > _reserved) {
    const auto deficit = lent - _reserved;
    ReserveMemory(kMemoryTag, deficit);
    _reserved = lent;
    _overflow += deficit;
  }
  _lent += bytes;
}

void MemoryBudget::Return(size_t bytes) noexcept {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(bytes <= _lent);
  _lent -= bytes;
}

duckdb::BufferPoolReservation MemoryBudget::Detach(size_t bytes) {
  Take(bytes);
  absl::MutexLock lock{&_mutex};
  _lent -= bytes;
  _reserved -= bytes;
  duckdb::BufferPoolReservation reservation{kMemoryTag, Pool()};
  reservation.size = bytes;
  return reservation;
}

void MemoryBudget::Attach(duckdb::BufferPoolReservation& reservation) noexcept {
  SDB_ASSERT(reservation.tag == kMemoryTag);
  absl::MutexLock lock{&_mutex};
  _reserved += std::exchange(reservation.size, 0);
}

void MemoryBudget::Reclaim(size_t bytes) noexcept {
  duckdb::BufferPoolReservation reservation{kMemoryTag, Pool()};
  reservation.Resize(bytes);
  Attach(reservation);
}

size_t MemoryBudget::Reserved() const noexcept {
  absl::MutexLock lock{&_mutex};
  return _reserved;
}

size_t MemoryBudget::Overflow() const noexcept {
  absl::MutexLock lock{&_mutex};
  return _overflow;
}

void ChargeMemory(MemoryBudget* budget, duckdb::MemoryTag tag, size_t size) {
  if (budget) {
    budget->Take(size);
  } else {
    ReserveMemory(tag, size);
  }
}

void FreeCharge(MemoryBudget* budget, duckdb::MemoryTag tag,
                size_t size) noexcept {
  if (budget) {
    budget->Return(size);
  } else {
    FreeReservedMemory(tag, size);
  }
}

duckdb::Allocator& TrackedDuckAllocator() {
  static absl::NoDestructor<duckdb::Allocator> gAllocator{
    TrackedAllocate, TrackedFree, TrackedReallocate, nullptr};
  return *gAllocator;
}

void ReserveMemory(duckdb::MemoryTag tag, size_t size) {
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  manager->ReserveMemory(tag, size);
}

void FreeReservedMemory(duckdb::MemoryTag tag, size_t size) noexcept {
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  manager->FreeReservedMemory(tag, size);
}

}  // namespace irs
