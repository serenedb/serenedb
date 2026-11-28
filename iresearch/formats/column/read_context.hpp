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

#pragma once

#include <atomic>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/formats/column/internal/block_manager.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

class ColReader;

class ReadContext final : public BlockManager {
 public:
  ReadContext(duckdb::DatabaseInstance& db, TrackedAllocator<> alloc);
  ReadContext(const ColReader& reader, TrackedAllocator<> alloc,
              bool random_access = false);
  ReadContext(duckdb::DatabaseInstance& db, const duckdb::InputFile* file,
              TrackedAllocator<> alloc);
  ~ReadContext() final;

  ReadContext(const ReadContext&) = delete;
  ReadContext& operator=(const ReadContext&) = delete;
  ReadContext(ReadContext&&) = delete;
  ReadContext& operator=(ReadContext&&) = delete;

  void Reset(const ColReader& reader);

  const duckdb::InputFile* File() const noexcept { return _in.File().get(); }
  bool RandomAccess() const noexcept { return _random_access; }
  duckdb::ObjectCache* Cache() const noexcept { return _cache; }
  TrackedAllocator<> Allocator() const noexcept { return _alloc; }

  duckdb::string_t ReadString(duckdb::Vector& result, duckdb::block_id_t block,
                              int32_t offset);

  duckdb::const_data_ptr_t TryReadStable(duckdb::idx_t position,
                                         duckdb::idx_t size);
  void Read(duckdb::idx_t position, duckdb::data_ptr_t target,
            duckdb::idx_t size);

  struct CacheSlot {
    uint64_t file_id = 0;
    uint64_t file_offset = 0;
    std::atomic<bool>* touched = nullptr;
    duckdb::ObjectCache* cache = nullptr;
  };

  duckdb::shared_ptr<duckdb::BlockHandle> RegisterColBlock(uint64_t offset,
                                                           uint64_t size);

  duckdb::unique_ptr<duckdb::Block> CreateBlock(
    duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) final;

  void Read(duckdb::QueryContext context, duckdb::Block& block) final;
  void ReadBlocks(duckdb::QueryContext context, duckdb::FileBuffer& buffer,
                  duckdb::block_id_t start_block,
                  duckdb::idx_t block_count) final;

  duckdb::block_id_t GetFreeBlockId() final;
  duckdb::block_id_t PeekFreeBlockId() final;
  void Write(duckdb::FileBuffer& block, duckdb::block_id_t block_id) final;
  void Write(duckdb::QueryContext context, duckdb::FileBuffer& block,
             duckdb::block_id_t block_id) final;
  bool IsRootBlock(duckdb::MetaBlockPointer root) final;
  duckdb::idx_t GetMetaBlock() final;
  void WriteHeader(duckdb::QueryContext context,
                   duckdb::DatabaseHeader header) final;
  duckdb::idx_t TotalBlocks() final;

 private:
  void ResetFile(const duckdb::InputFile* file);
  bool Maps(uint64_t offset, uint64_t size) const noexcept;
  bool Wraps(uint64_t offset, uint64_t size) const noexcept;

  duckdb::InputFileReader _in;
  duckdb::MemoryMappedFile* _mapping = nullptr;
  containers::FlatHashMap<uint64_t, uint64_t> _sizes;
  bool _random_access = false;
  duckdb::ObjectCache* _cache;
  TrackedAllocator<> _alloc;
};

inline bool Touched(const ReadContext::CacheSlot& slot) noexcept {
  return slot.touched->load(std::memory_order_relaxed);
}

inline bool SecondTouch(const ReadContext::CacheSlot& slot) noexcept {
  return Touched(slot) ||
         slot.touched->exchange(true, std::memory_order_relaxed);
}

}  // namespace irs
