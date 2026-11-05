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

}  // namespace

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
