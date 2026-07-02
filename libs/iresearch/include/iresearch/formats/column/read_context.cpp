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

#include "iresearch/formats/column/read_context.hpp"

#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/metadata/metadata_manager.hpp>
#include <duckdb/storage/storage_info.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_reader.hpp"

namespace irs {

ReadContext::ReadContext(duckdb::DatabaseInstance& db) noexcept
  : duckdb::BlockManager{duckdb::BufferManager::GetBufferManager(db),
                         DEFAULT_BLOCK_ALLOC_SIZE,
                         duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE},
    _db{&db},
    _allocator{&duckdb::BlockAllocator::Get(db)} {}

ReadContext::ReadContext(const ColReader& reader)
  : ReadContext{reader.Database()} {
  _in = reader.ReopenIn();
}

ReadContext::ReadContext(duckdb::DatabaseInstance& db, IndexInput::ptr in)
  : ReadContext{db} {
  _in = std::move(in);
}

ReadContext::~ReadContext() = default;

void ReadContext::Reset(const ColReader& reader) {
  SDB_ASSERT(_db == &reader.Database(),
             "ReadContext::Reset: Database mismatch");
  _in = reader.ReopenIn();
}

void ReadContext::Read(duckdb::QueryContext /*context*/, duckdb::Block& block) {
  _in->ReadData(static_cast<uint64_t>(block.id),
                reinterpret_cast<byte_type*>(block.InternalBuffer()),
                block.AllocSize());
}

void ReadContext::ReadBlocks(duckdb::QueryContext /*context*/,
                             duckdb::FileBuffer& /*buffer*/,
                             duckdb::block_id_t /*start_block*/,
                             duckdb::idx_t /*block_count*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::ReadBlocks: contiguous multi-block "
            "reads are not supported");
}

duckdb::unique_ptr<duckdb::Block> ReadContext::ConvertBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer& source_buffer) {
  return duckdb::make_uniq<duckdb::Block>(source_buffer, block_id,
                                          GetBlockHeaderSize());
}

duckdb::unique_ptr<duckdb::Block> ReadContext::CreateBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) {
  if (source_buffer) {
    return ConvertBlock(block_id, *source_buffer);
  }
  return duckdb::make_uniq<duckdb::Block>(*_allocator, block_id, *this);
}

duckdb::block_id_t ReadContext::GetFreeBlockId() {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::GetFreeBlockId on read-only context");
}
duckdb::block_id_t ReadContext::PeekFreeBlockId() { return 0; }
duckdb::block_id_t ReadContext::GetFreeBlockIdForCheckpoint() {
  return GetFreeBlockId();
}
void ReadContext::Write(duckdb::FileBuffer& /*block*/,
                        duckdb::block_id_t /*block_id*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "ReadContext::Write on read-only context");
}
void ReadContext::Write(duckdb::QueryContext /*context*/,
                        duckdb::FileBuffer& block,
                        duckdb::block_id_t block_id) {
  Write(block, block_id);
}
bool ReadContext::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::IsRootBlock on read-only context");
}
void ReadContext::MarkBlockAsCheckpointed(duckdb::block_id_t /*block_id*/) {}
void ReadContext::MarkBlockAsUsed(duckdb::block_id_t /*block_id*/) {}
void ReadContext::MarkBlockAsModified(duckdb::block_id_t /*block_id*/) {}
void ReadContext::IncreaseBlockReferenceCount(duckdb::block_id_t /*block_id*/) {
}
duckdb::idx_t ReadContext::GetMetaBlock() {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::GetMetaBlock on read-only context");
}
void ReadContext::WriteHeader(duckdb::QueryContext /*context*/,
                              duckdb::DatabaseHeader /*header*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::WriteHeader on read-only context");
}
duckdb::idx_t ReadContext::TotalBlocks() { return 0; }
duckdb::idx_t ReadContext::FreeBlocks() { return 0; }
void ReadContext::FileSync() {}

}  // namespace irs
