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

#include "pg/sql_exception_macro.h"

namespace irs {

WriteContext::WriteContext(duckdb::DatabaseInstance& db, IndexOutput& out)
  : BlockManager{db, duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE}, _out{&out} {}

WriteContext::~WriteContext() = default;

duckdb::block_id_t WriteContext::GetFreeBlockId() {
  const auto id =
    kColBlockIdBias + static_cast<duckdb::block_id_t>(_block_offsets.size());
  _block_offsets.push_back(kColBlockUnwritten);
  return id;
}

duckdb::block_id_t WriteContext::PeekFreeBlockId() {
  return kColBlockIdBias +
         static_cast<duckdb::block_id_t>(_block_offsets.size());
}

void WriteContext::Write(duckdb::FileBuffer& block,
                         duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}

void WriteContext::Write(duckdb::QueryContext /*context*/,
                         duckdb::FileBuffer& block,
                         duckdb::block_id_t block_id) {
  SDB_ENSURE(block_id >= kColBlockIdBias,
             "WriteContext::Write: foreign block id ", block_id);
  const auto idx = static_cast<uint64_t>(block_id - kColBlockIdBias);
  SDB_ENSURE(idx < _block_offsets.size(),
             "WriteContext::Write: unallocated block id ", block_id);
  SDB_ENSURE(_block_offsets[idx] == kColBlockUnwritten,
             "WriteContext::Write: block ", block_id, " written twice");
  if (const uint64_t misalign = _out->Position() % 8; misalign != 0) {
    static constexpr byte_type kPad[8]{};
    _out->WriteData(kPad, 8 - misalign);
  }
  _block_offsets[idx] = _out->Position();
  _out->WriteData(reinterpret_cast<const byte_type*>(block.InternalBuffer()),
                  block.AllocSize());
}

void WriteContext::Read(duckdb::QueryContext /*context*/,
                        duckdb::Block& /*block*/) {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::Read on write-only context"));
}

void WriteContext::ReadBlocks(duckdb::QueryContext /*context*/,
                              duckdb::FileBuffer& /*buffer*/,
                              duckdb::block_id_t /*start_block*/,
                              duckdb::idx_t /*block_count*/) {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::ReadBlocks on write-only context"));
}

bool WriteContext::IsRootBlock(duckdb::MetaBlockPointer /*root*/) {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::IsRootBlock"));
}

duckdb::idx_t WriteContext::GetMetaBlock() {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::GetMetaBlock"));
}

void WriteContext::WriteHeader(duckdb::QueryContext /*context*/,
                               duckdb::DatabaseHeader /*header*/) {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::WriteHeader"));
}

duckdb::idx_t WriteContext::TotalBlocks() {
  return static_cast<duckdb::idx_t>(_block_offsets.size());
}

}  // namespace irs
