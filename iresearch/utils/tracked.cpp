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

void ChargeRoot(duckdb::MemoryTag tag, size_t size) {
  auto charge = ReserveMemory(tag, size);
  charge.size = 0;
}

BudgetNode::BudgetNode(MemoryBudget* parent, size_t reserve) : _parent{parent} {
  Reserve(reserve);
}

BudgetNode::~BudgetNode() {
  SDB_ASSERT(_used == 0);
  if (_held != 0) {
    Give(_held);
  }
}

void BudgetNode::Reserve(size_t bytes) {
  if (bytes > Available()) {
    Take(bytes - Available());
  }
}

void BudgetNode::Trim() noexcept {
  if (const auto unused = Available(); unused != 0) {
    Give(unused);
  }
}

void BudgetNode::Take(size_t bytes) {
  ChargeMemory(_parent, kMemoryTag, bytes);
  _held += bytes;
}

void BudgetNode::Give(size_t bytes) noexcept {
  SDB_ASSERT(bytes <= Available());
  _held -= bytes;
  FreeCharge(_parent, kMemoryTag, bytes);
}

void BudgetNode::Detach(size_t bytes) noexcept {
  SDB_ASSERT(bytes <= Available());
  _held -= bytes;
  for (BudgetNode* node = _parent; node; node = node->_parent) {
    SDB_ASSERT(bytes <= node->_used && bytes <= node->_held);
    node->_used -= bytes;
    node->_held -= bytes;
  }
}

duckdb::TempBufferPoolReservation MemoryBudget::Lend(duckdb::MemoryTag tag,
                                                     size_t bytes) {
  Reserve(bytes);
  auto* manager = DuckDBEngine::TrackingBufferManager();
  SDB_ASSERT(manager);
  duckdb::TempBufferPoolReservation lent{kMemoryTag, manager->GetBufferPool(),
                                         0};
  Detach(bytes);
  lent.size = bytes;
  lent.Retag(tag);
  return lent;
}

void FixedBudget::Exhausted(size_t bytes) const {
  throw duckdb::OutOfMemoryException(
    "a fixed memory reservation of %s is exhausted: %s more requested",
    duckdb::StringUtil::BytesToHumanReadableString(Held()),
    duckdb::StringUtil::BytesToHumanReadableString(bytes - Available()));
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

void* TrackedMalloc(MemoryBudget* budget, size_t size) noexcept {
  auto* p = std::malloc(size);
  if (!p) {
    return nullptr;
  }
  try {
    ChargeMemory(budget, kMemoryTag, malloc_usable_size(p));
  } catch (const duckdb::OutOfMemoryException&) {
    std::free(p);
    return nullptr;
  }
  return p;
}

void TrackedFree(MemoryBudget* budget, void* p) noexcept {
  if (!p) {
    return;
  }
  FreeCharge(budget, kMemoryTag, malloc_usable_size(p));
  std::free(p);
}

}  // namespace irs
