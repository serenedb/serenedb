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

#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

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

}  // namespace

ReadContext::ReadContext(duckdb::DatabaseInstance& db) noexcept
  : BlockManager{db, /*block_header_size=*/0} {}

ReadContext::ReadContext(const ColReader& reader, bool random_access)
  : ReadContext{reader.Database()} {
  _random_access = random_access;
  ResetFile(reader.File());
}

ReadContext::ReadContext(duckdb::DatabaseInstance& db, const InputFile* file)
  : ReadContext{db} {
  ResetFile(file);
}

ReadContext::~ReadContext() {
  SDB_ASSERT(_live_handles == 0,
             "block handles must not outlive their ReadContext");
}

void ReadContext::Reset(const ColReader& reader) {
  SDB_ASSERT(_db == &reader.Database(),
             "ReadContext::Reset: Database mismatch");
  SDB_ASSERT(_live_handles == 0, "ReadContext::Reset with live block handles");
  ResetFile(reader.File());
}

void ReadContext::ResetFile(const InputFile* file) {
  _file = file;
  _in = file ? IndexInput{*file} : IndexInput{};
  _mapping.reset();
  _ranges.clear();
  if (const auto span = file ? file->Span() : bytes_view{}; !span.empty()) {
    _mapping = duckdb::make_uniq<ColMapping>(span.data(), span.size());
  }
}

duckdb::string_t ReadContext::ReadString(duckdb::Vector& result,
                                         duckdb::block_id_t block,
                                         int32_t offset) {
  SDB_ASSERT(offset == 0);
  if (_mapping) {
    BytesViewInput in{*_file};
    in.Seek(static_cast<uint64_t>(block));
    const auto len = static_cast<uint32_t>(in.ReadI32());
    const auto body = in.Pin(len);
    return duckdb::string_t{reinterpret_cast<const char*>(body.bytes.data()),
                            len};
  }
  _in.Seek(static_cast<uint64_t>(block));
  const auto len = static_cast<uint32_t>(_in.ReadI32());
  auto out = duckdb::StringVector::EmptyString(result, len);
  _in.ReadData(reinterpret_cast<byte_type*>(out.GetDataWriteable()), len);
  out.Finalize();
  return out;
}

duckdb::const_data_ptr_t ReadContext::TryReadStable(duckdb::idx_t position,
                                                    duckdb::idx_t size) {
  return _mapping ? _mapping->GetData(position, size) : nullptr;
}

void ReadContext::Read(duckdb::idx_t position, duckdb::data_ptr_t target,
                       duckdb::idx_t size) {
  _in.ReadData(position, reinterpret_cast<byte_type*>(target), size);
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
  SDB_ENSURE(id < _ranges.size() && _file,
             "ReadContext::Read: unregistered block ", block.id);
  const auto [offset, size] = _ranges[id];
  SDB_ASSERT(block.Size() >= size, "block buffer sector-rounds up");
  if (_mapping && offset % 8 == 0 &&
      offset + block.Size() <= _mapping->Size()) {
    block.Read(context, *_mapping, offset);
    return;
  }
  _in.ReadData(offset, block.InternalBuffer(), size);
}

void ReadContext::ReadBlocks(duckdb::QueryContext /*context*/,
                             duckdb::FileBuffer& /*buffer*/,
                             duckdb::block_id_t /*start_block*/,
                             duckdb::idx_t /*block_count*/) {
  THROW_SQL_ERROR(
    ERR_MSG("ReadContext::ReadBlocks: contiguous multi-block "
            "reads are not supported"));
}

duckdb::block_id_t ReadContext::GetFreeBlockId() {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::GetFreeBlockId on read-only context"));
}
duckdb::block_id_t ReadContext::PeekFreeBlockId() {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::PeekFreeBlockId on read-only context"));
}
void ReadContext::Write(duckdb::FileBuffer& block,
                        duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}
void ReadContext::Write(duckdb::QueryContext /*context*/,
                        duckdb::FileBuffer& /*block*/,
                        duckdb::block_id_t /*block_id*/) {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::Write on read-only context"));
}
bool ReadContext::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::IsRootBlock on read-only context"));
}
duckdb::idx_t ReadContext::GetMetaBlock() {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::GetMetaBlock on read-only context"));
}
void ReadContext::WriteHeader(duckdb::QueryContext /*context*/,
                              duckdb::DatabaseHeader /*header*/) {
  THROW_SQL_ERROR(ERR_MSG("ReadContext::WriteHeader on read-only context"));
}
duckdb::idx_t ReadContext::TotalBlocks() { return 0; }

}  // namespace irs
