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

#include <duckdb/common/types/string_type.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/block.hpp>
#include <limits>

#include "iresearch/types.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {

WriteContext::WriteContext(duckdb::DatabaseInstance& db, IndexOutput& out)
  : BlockManager{db, duckdb::Storage::DEFAULT_BLOCK_HEADER_SIZE}, _out{&out} {}

WriteContext::~WriteContext() = default;

void WriteContext::WriteString(
  duckdb::UncompressedStringSegmentState& /*state*/, duckdb::string_t string,
  duckdb::block_id_t& result_block, int32_t& result_offset) {
  result_block = static_cast<duckdb::block_id_t>(_out->Position());
  result_offset = 0;
  const auto len = string.GetSize();
  SDB_ENSURE(len <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  _out->WriteU32(len);
  _out->WriteData(reinterpret_cast<const byte_type*>(string.GetData()), len);
}

duckdb::idx_t WriteContext::Position() const {
  return static_cast<duckdb::idx_t>(_out->Position());
}

void WriteContext::Append(duckdb::const_data_ptr_t data, duckdb::idx_t size) {
  _out->WriteData(reinterpret_cast<const byte_type*>(data), size);
}

duckdb::block_id_t WriteContext::GetFreeBlockId() {
  THROW_SQL_ERROR(
    ERR_MSG("WriteContext::GetFreeBlockId: column data goes "
            "through ColumnStreamWriter, not blocks"));
}

duckdb::block_id_t WriteContext::PeekFreeBlockId() {
  THROW_SQL_ERROR(ERR_MSG("WriteContext::PeekFreeBlockId"));
}

void WriteContext::Write(duckdb::FileBuffer& block,
                         duckdb::block_id_t block_id) {
  Write(duckdb::QueryContext{}, block, block_id);
}

void WriteContext::Write(duckdb::QueryContext /*context*/,
                         duckdb::FileBuffer& /*block*/,
                         duckdb::block_id_t /*block_id*/) {
  THROW_SQL_ERROR(
    ERR_MSG("WriteContext::Write: column data goes through "
            "ColumnStreamWriter, not blocks"));
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

duckdb::idx_t WriteContext::TotalBlocks() { return 0; }

}  // namespace irs
