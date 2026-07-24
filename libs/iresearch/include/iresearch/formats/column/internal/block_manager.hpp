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

#include <cstdint>
#include <duckdb/storage/block.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/metadata/metadata_manager.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <limits>

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

// Overflow-block ids are logical handles resolved through a footer id->offset
// table (compressors flush blocks out of allocation order); the bias keeps
// them disjoint from ReadContext's dense RegisterColBlock ids.
inline constexpr duckdb::block_id_t kColBlockIdBias = duckdb::block_id_t{1}
                                                      << 40;
inline constexpr uint64_t kColBlockUnwritten =
  std::numeric_limits<uint64_t>::max();

class BlockManager : public duckdb::BlockManager {
 public:
  BlockManager(duckdb::DatabaseInstance& db,
               duckdb::idx_t block_header_size) noexcept
    : duckdb::BlockManager{duckdb::BufferManager::GetBufferManager(db),
                           DEFAULT_BLOCK_ALLOC_SIZE, block_header_size},
      _db{&db},
      _allocator{&duckdb::BlockAllocator::Get(db)} {}

  duckdb::DatabaseInstance& Database() noexcept { return *_db; }

  bool InMemory() final { return false; }

  duckdb::unique_ptr<duckdb::Block> ConvertBlock(
    duckdb::block_id_t block_id, duckdb::FileBuffer& source_buffer) final {
    return duckdb::make_uniq<duckdb::Block>(source_buffer, block_id,
                                            GetBlockHeaderSize());
  }
  duckdb::unique_ptr<duckdb::Block> CreateBlock(
    duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) override {
    if (source_buffer) {
      return ConvertBlock(block_id, *source_buffer);
    }
    return duckdb::make_uniq<duckdb::Block>(*_allocator, block_id, *this);
  }

  duckdb::block_id_t GetFreeBlockIdForCheckpoint() final {
    return GetFreeBlockId();
  }

  void MarkBlockAsCheckpointed(duckdb::block_id_t /*block_id*/) final {}
  void MarkBlockAsUsed(duckdb::block_id_t /*block_id*/) final {}
  void MarkBlockAsModified(duckdb::block_id_t /*block_id*/) final {}
  void IncreaseBlockReferenceCount(duckdb::block_id_t /*block_id*/) final {}

  duckdb::idx_t FreeBlocks() final { return 0; }
  void FileSync() final {}

 protected:
  duckdb::DatabaseInstance* _db;
  duckdb::BlockAllocator* _allocator;
};

}  // namespace irs
