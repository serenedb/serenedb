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

#include "iresearch/formats/column/internal/write_context.hpp"

#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/metadata/metadata_manager.hpp>
#include <duckdb/storage/storage_info.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"

namespace irs {

WriteContext::WriteContext(duckdb::DatabaseInstance& db, IndexOutput& out)
  : duckdb::BlockManager(duckdb::BufferManager::GetBufferManager(db),
                         DEFAULT_BLOCK_ALLOC_SIZE,
                         duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE),
    _db{&db},
    _allocator{&duckdb::BlockAllocator::Get(db)},
    _out{&out} {}

WriteContext::~WriteContext() = default;

duckdb::block_id_t WriteContext::GetFreeBlockId() {
  const auto cursor = static_cast<duckdb::block_id_t>(_out->Position());
  if (_next_id < cursor) {
    _next_id = cursor;
  }
  const auto id = _next_id;
  _next_id += static_cast<duckdb::block_id_t>(GetBlockAllocSize());
  return id;
}

duckdb::block_id_t WriteContext::PeekFreeBlockId() {
  const auto cursor = static_cast<duckdb::block_id_t>(_out->Position());
  return _next_id < cursor ? cursor : _next_id;
}

duckdb::block_id_t WriteContext::GetFreeBlockIdForCheckpoint() {
  return GetFreeBlockId();
}

void WriteContext::Write(duckdb::FileBuffer& block,
                         duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}

void WriteContext::Write(duckdb::QueryContext /*context*/,
                         duckdb::FileBuffer& block,
                         duckdb::block_id_t block_id) {
  const auto cursor = static_cast<duckdb::block_id_t>(_out->Position());
  SDB_ENSURE(cursor == block_id, sdb::ERROR_INTERNAL,
             "WriteContext::Write out-of-order: cursor=", cursor,
             " expected=", block_id);
  _out->WriteData(reinterpret_cast<const byte_type*>(block.InternalBuffer()),
                  block.AllocSize());
}

duckdb::unique_ptr<duckdb::Block> WriteContext::ConvertBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer& source_buffer) {
  return duckdb::make_uniq<duckdb::Block>(source_buffer, block_id,
                                          GetBlockHeaderSize());
}

duckdb::unique_ptr<duckdb::Block> WriteContext::CreateBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) {
  if (source_buffer) {
    return ConvertBlock(block_id, *source_buffer);
  }
  return duckdb::make_uniq<duckdb::Block>(*_allocator, block_id, *this);
}

void WriteContext::Read(duckdb::QueryContext /*context*/,
                        duckdb::Block& /*block*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "WriteContext::Read on write-only context");
}

void WriteContext::ReadBlocks(duckdb::QueryContext /*context*/,
                              duckdb::FileBuffer& /*buffer*/,
                              duckdb::block_id_t /*start_block*/,
                              duckdb::idx_t /*block_count*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "WriteContext::ReadBlocks on write-only context");
}

bool WriteContext::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "WriteContext::IsRootBlock");
}

void WriteContext::MarkBlockAsCheckpointed(duckdb::block_id_t /*block_id*/) {}
void WriteContext::MarkBlockAsUsed(duckdb::block_id_t /*block_id*/) {}
void WriteContext::MarkBlockAsModified(duckdb::block_id_t /*block_id*/) {}
void WriteContext::IncreaseBlockReferenceCount(
  duckdb::block_id_t /*block_id*/) {}

duckdb::idx_t WriteContext::GetMetaBlock() {
  SDB_THROW(sdb::ERROR_INTERNAL, "WriteContext::GetMetaBlock");
}

void WriteContext::WriteHeader(duckdb::QueryContext /*context*/,
                               duckdb::DatabaseHeader /*header*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "WriteContext::WriteHeader");
}

duckdb::idx_t WriteContext::TotalBlocks() {
  return static_cast<duckdb::idx_t>(_next_id / GetBlockAllocSize());
}

duckdb::idx_t WriteContext::FreeBlocks() { return 0; }
void WriteContext::FileSync() {}

}  // namespace irs
