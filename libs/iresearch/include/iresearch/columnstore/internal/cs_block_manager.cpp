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

#include "iresearch/columnstore/internal/cs_block_manager.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/metadata/metadata_manager.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"

namespace irs::columnstore {

CsBlockManager::CsBlockManager(duckdb::BufferManager& bm,
                               duckdb::BlockAllocator& allocator,
                               IndexOutput& out)
  : duckdb::BlockManager(bm, DEFAULT_BLOCK_ALLOC_SIZE,
                         duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE),
    _allocator{&allocator},
    _out{&out} {}

CsBlockManager::CsBlockManager(duckdb::BufferManager& bm,
                               duckdb::BlockAllocator& allocator,
                               IndexInput& in)
  : duckdb::BlockManager(bm, DEFAULT_BLOCK_ALLOC_SIZE,
                         duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE),
    _allocator{&allocator},
    _in{&in} {}

CsBlockManager::~CsBlockManager() = default;

duckdb::block_id_t CsBlockManager::GetFreeBlockId() {
  const auto cursor = static_cast<duckdb::block_id_t>(_out->Position());
  if (_next_id < cursor) {
    _next_id = cursor;
  }
  const auto id = _next_id;
  _next_id += static_cast<duckdb::block_id_t>(GetBlockAllocSize());
  return id;
}

duckdb::block_id_t CsBlockManager::PeekFreeBlockId() {
  const auto cursor =
    static_cast<duckdb::block_id_t>(_out ? _out->Position() : 0);
  return _next_id < cursor ? cursor : _next_id;
}

duckdb::block_id_t CsBlockManager::GetFreeBlockIdForCheckpoint() {
  return GetFreeBlockId();
}

void CsBlockManager::Write(duckdb::FileBuffer& block,
                           duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}

void CsBlockManager::Write(duckdb::QueryContext /*context*/,
                           duckdb::FileBuffer& block,
                           duckdb::block_id_t block_id) {
  const auto cursor = static_cast<duckdb::block_id_t>(_out->Position());
  SDB_ENSURE(cursor == block_id, sdb::ERROR_INTERNAL,
             "CsBlockManager::Write out-of-order: cursor=", cursor,
             " expected=", block_id);
  _out->WriteBytes(reinterpret_cast<const byte_type*>(block.InternalBuffer()),
                   block.AllocSize());
}

void CsBlockManager::Read(duckdb::QueryContext /*context*/,
                          duckdb::Block& block) {
  _in->ReadBytes(static_cast<uint64_t>(block.id),
                 reinterpret_cast<byte_type*>(block.InternalBuffer()),
                 block.AllocSize());
}

void CsBlockManager::ReadBlocks(duckdb::FileBuffer& /*buffer*/,
                                duckdb::block_id_t /*start_block*/,
                                duckdb::idx_t /*block_count*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "CsBlockManager::ReadBlocks: contiguous multi-block reads not "
            "supported");
}

duckdb::unique_ptr<duckdb::Block> CsBlockManager::ConvertBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer& source_buffer) {
  return duckdb::make_uniq<duckdb::Block>(source_buffer, block_id,
                                          GetBlockHeaderSize());
}

duckdb::unique_ptr<duckdb::Block> CsBlockManager::CreateBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) {
  if (source_buffer) {
    return ConvertBlock(block_id, *source_buffer);
  }
  return duckdb::make_uniq<duckdb::Block>(*_allocator, block_id, *this);
}

bool CsBlockManager::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "CsBlockManager::IsRootBlock");
}

void CsBlockManager::MarkBlockAsCheckpointed(duckdb::block_id_t /*block_id*/) {}
void CsBlockManager::MarkBlockAsUsed(duckdb::block_id_t /*block_id*/) {}
void CsBlockManager::MarkBlockAsModified(duckdb::block_id_t /*block_id*/) {}
void CsBlockManager::IncreaseBlockReferenceCount(
  duckdb::block_id_t /*block_id*/) {}

duckdb::idx_t CsBlockManager::GetMetaBlock() {
  SDB_THROW(sdb::ERROR_INTERNAL, "CsBlockManager::GetMetaBlock");
}

void CsBlockManager::WriteHeader(duckdb::QueryContext /*context*/,
                                 duckdb::DatabaseHeader /*header*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "CsBlockManager::WriteHeader");
}

duckdb::idx_t CsBlockManager::TotalBlocks() {
  return static_cast<duckdb::idx_t>(_next_id / GetBlockAllocSize());
}
duckdb::idx_t CsBlockManager::FreeBlocks() { return 0; }
void CsBlockManager::FileSync() {}

}  // namespace irs::columnstore
