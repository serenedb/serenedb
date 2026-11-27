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

#include <absl/cleanup/cleanup.h>
#include <malloc.h>

#include <cstddef>
#include <cstdlib>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/fault_hits.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

struct BudgetAllocatorData final : duckdb::PrivateAllocatorData {
  explicit BudgetAllocatorData(LocalBudget& budget) noexcept : budget{budget} {}

  LocalBudget& budget;
};

LocalBudget& BudgetOf(duckdb::PrivateAllocatorData* data) noexcept {
  return data->Cast<BudgetAllocatorData>().budget;
}

duckdb::data_ptr_t ChargedAllocate(duckdb::PrivateAllocatorData* data,
                                   duckdb::idx_t size) {
  auto& budget = BudgetOf(data);
  budget.Charge(size);
  absl::Cleanup release = [&] { budget.Free(size); };
  auto* p = duckdb::Allocator::DefaultAllocate(data, size);
  if (p) {
    std::move(release).Cancel();
  }
  return p;
}

void ChargedFree(duckdb::PrivateAllocatorData* data, duckdb::data_ptr_t p,
                 duckdb::idx_t size) {
  duckdb::Allocator::DefaultFree(data, p, size);
  BudgetOf(data).Free(size);
}

duckdb::data_ptr_t ChargedReallocate(duckdb::PrivateAllocatorData* data,
                                     duckdb::data_ptr_t p,
                                     duckdb::idx_t old_size,
                                     duckdb::idx_t size) {
  auto& budget = BudgetOf(data);
  if (size <= old_size) {
    auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
    if (r) {
      budget.Free(old_size - size);
    }
    return r;
  }
  budget.Charge(size - old_size);
  absl::Cleanup release = [&] { budget.Free(size - old_size); };
  auto* r = duckdb::Allocator::DefaultReallocate(data, p, old_size, size);
  if (r) {
    std::move(release).Cancel();
  }
  return r;
}

}  // namespace

void ChargeRoot(size_t size) {
  auto charge = ReserveMemory(kMemoryTag, size);
  charge.size = 0;
}

void MemoryBudget::Adopt(size_t bytes) {
  if (!TryAttach(bytes)) [[unlikely]] {
    Exceeded(bytes, Used());
  }
}

void MemoryBudget::Charge(size_t bytes) {
  Adopt(bytes);
  absl::Cleanup undo = [&] { Detach(bytes); };
  ChargeRoot(bytes);
  std::move(undo).Cancel();
}

void MemoryBudget::Free(size_t bytes) noexcept {
  Detach(bytes);
  FreeReservedMemory(kMemoryTag, bytes);
}

void MemoryBudget::Admit(size_t bytes) const {
  if (const auto used = Used(); used + bytes > Limit()) {
    Exceeded(bytes, used);
  }
  auto room = ReserveMemory(kMemoryTag, bytes);
}

void MemoryBudget::Exceeded(size_t bytes, size_t used) const {
  throw duckdb::OutOfMemoryException(
    "failed to allocate data of size %s (the session's memory limit: %s/%s "
    "used)",
    duckdb::StringUtil::BytesToHumanReadableString(bytes),
    duckdb::StringUtil::BytesToHumanReadableString(used),
    duckdb::StringUtil::BytesToHumanReadableString(Limit()));
}

LocalBudget::~LocalBudget() {
  SDB_ASSERT(_used == 0);
  if (_held != 0) {
    _parent->Free(_held);
  }
}

void LocalBudget::Grow(size_t bytes) {
  Take(bytes - Available());
  _used += bytes;
}

void LocalBudget::Take(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  _parent->Charge(bytes);
  _held += bytes;
}

void LocalBudget::Reserve(size_t bytes) {
  if (bytes > Available()) {
    Take(bytes - Available());
  }
}

void LocalBudget::Rebind(MemoryBudget& parent) {
  if (&parent == _parent) {
    return;
  }
  if (_held != 0) {
    parent.Adopt(_held);
    _parent->Detach(_held);
  }
  _parent = &parent;
}

void LocalBudget::Return() noexcept {
  if (_parent == &gRootBudget) {
    return;
  }
  if (_held != 0) {
    gRootBudget.Attach(_held);
    _parent->Detach(_held);
  }
  _parent = &gRootBudget;
}

void LocalBudget::Trim() noexcept {
  if (const auto unused = Available(); unused != 0) {
    _held = _used;
    _parent->Free(unused);
  }
}

duckdb::TempBufferPoolReservation LocalBudget::Lend(duckdb::MemoryTag tag,
                                                    size_t bytes) {
  Reserve(bytes);
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  duckdb::TempBufferPoolReservation lent{kMemoryTag, manager->GetBufferPool(),
                                         0};
  _held -= bytes;
  _parent->Detach(bytes);
  lent.size = bytes;
  lent.Retag(tag);
  return lent;
}

duckdb::Allocator DuckAllocator(TrackedAllocator<> alloc) {
  return duckdb::Allocator{
    ChargedAllocate, ChargedFree, ChargedReallocate,
    duckdb::make_uniq<BudgetAllocatorData>(alloc.Budget())};
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
  SDB_ENSURE(manager, "tracked memory is allocated while no engine runs");
  return manager->ReserveMemory(tag, size);
}

void FreeReservedMemory(duckdb::MemoryTag tag, size_t size) noexcept {
  if (auto* manager = DuckDBEngine::TrackingBufferManager()) {
    manager->FreeReservedMemory(tag, size);
  }
}

void* TrackedMalloc(TrackedAllocator<> alloc, size_t size) noexcept {
  auto* p = std::malloc(size);
  if (!p) {
    return nullptr;
  }
  try {
    alloc.Budget().Charge(malloc_usable_size(p));
  } catch (const duckdb::OutOfMemoryException&) {
    std::free(p);
    return nullptr;
  }
  return p;
}

void TrackedFree(TrackedAllocator<> alloc, void* p) noexcept {
  if (!p) {
    return;
  }
  alloc.Budget().Free(malloc_usable_size(p));
  std::free(p);
}

}  // namespace irs
