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

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/types.hpp"

namespace irs {
namespace {

class ColMapping final : public duckdb::MemoryMappedFile {
 public:
  ColMapping(const byte_type* data, uint64_t size)
    : MemoryMappedFile{/*path=*/{}, duckdb::FileOpenFlags::FILE_FLAGS_READ,
                       const_cast<duckdb::data_ptr_t>(data),
                       static_cast<duckdb::idx_t>(size)} {}

  void Sync() final {}
  bool Trim(duckdb::idx_t /*offset*/, duckdb::idx_t /*length*/) final {
    return false;
  }
  void Close() final {}
};

void AdviseWillNeed(const duckdb::MemoryMappedFile& mapping, uint64_t offset,
                    uint64_t size) {
#ifdef __linux__
  static const auto kPage = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
  const auto base = reinterpret_cast<uintptr_t>(mapping.GetData(offset, size));
  const auto aligned = base & ~(kPage - 1);
  ::madvise(reinterpret_cast<void*>(aligned), size + (base - aligned),
            MADV_WILLNEED);
#endif
}

}  // namespace

ReadContext::ReadContext(duckdb::DatabaseInstance& db) noexcept
  : BlockManager{db, /*block_header_size=*/0} {}

ReadContext::ReadContext(const ColReader& reader)
  : ReadContext{reader.Database()} {
  _in = reader.ReopenIn();
  ResetMapping();
}

ReadContext::ReadContext(duckdb::DatabaseInstance& db, IndexInput::ptr in)
  : ReadContext{db} {
  _in = std::move(in);
  ResetMapping();
}

ReadContext::~ReadContext() {
  SDB_ASSERT(_live_handles == 0,
             "block handles must not outlive their ReadContext");
}

void ReadContext::Reset(const ColReader& reader) {
  SDB_ASSERT(_db == &reader.Database(),
             "ReadContext::Reset: Database mismatch");
  SDB_ASSERT(_live_handles == 0, "ReadContext::Reset with live block handles");
  _in = reader.ReopenIn();
  ResetMapping();
}

void ReadContext::ResetMapping() {
  _mapping.reset();
  _ranges.clear();
  if (!_in) {
    return;
  }
  const auto pos = _in->Position();
  const auto len = _in->Length();
  if (const byte_type* data = _in->ReadStable(0, len); data != nullptr) {
    _mapping = duckdb::make_uniq<ColMapping>(data, len);
    _in->Seek(pos);
  }
}

duckdb::shared_ptr<duckdb::BlockHandle> ReadContext::RegisterColBlock(
  uint64_t offset, uint64_t size) {
  const auto id = static_cast<duckdb::block_id_t>(_ranges.size());
  _ranges.emplace_back(offset, size);
  ++_live_handles;
  return RegisterBlock(id);
}

void ReadContext::UnregisterBlock(duckdb::block_id_t id) {
  SDB_ASSERT(_live_handles != 0);
  --_live_handles;
  duckdb::BlockManager::UnregisterBlock(id);
}

duckdb::unique_ptr<duckdb::Block> ReadContext::CreateBlock(
  duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) {
  const auto id = static_cast<size_t>(block_id);
  if (id >= _ranges.size()) {
    return BlockManager::CreateBlock(block_id, source_buffer);
  }
  SDB_ASSERT(source_buffer == nullptr);
  return duckdb::make_uniq<duckdb::Block>(
    *_allocator, block_id, static_cast<duckdb::idx_t>(_ranges[id].second),
    /*block_header_size=*/0);
}

void ReadContext::Read(duckdb::QueryContext context, duckdb::Block& block) {
  const auto id = static_cast<size_t>(block.id);
  SDB_ENSURE(id < _ranges.size() && _in, sdb::ERROR_INTERNAL,
             "ReadContext::Read: unregistered block ", block.id);
  const auto [offset, size] = _ranges[id];
  SDB_ASSERT(block.Size() >= size, "block buffer sector-rounds up");
  if (_mapping && offset % 8 == 0 &&
      offset + block.Size() <= _mapping->Size()) {
    block.Read(context, *_mapping, offset);
    AdviseWillNeed(*_mapping, offset, size);
    return;
  }
  _in->ReadData(offset, block.InternalBuffer(), size);
}

void ReadContext::ReadBlocks(duckdb::QueryContext /*context*/,
                             duckdb::FileBuffer& /*buffer*/,
                             duckdb::block_id_t /*start_block*/,
                             duckdb::idx_t /*block_count*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::ReadBlocks: contiguous multi-block "
            "reads are not supported");
}

duckdb::block_id_t ReadContext::GetFreeBlockId() {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::GetFreeBlockId on read-only context");
}
duckdb::block_id_t ReadContext::PeekFreeBlockId() {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::PeekFreeBlockId on read-only context");
}
void ReadContext::Write(duckdb::FileBuffer& block,
                        duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}
void ReadContext::Write(duckdb::QueryContext /*context*/,
                        duckdb::FileBuffer& /*block*/,
                        duckdb::block_id_t /*block_id*/) {
  SDB_THROW(sdb::ERROR_INTERNAL, "ReadContext::Write on read-only context");
}
bool ReadContext::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::IsRootBlock on read-only context");
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

}  // namespace irs
