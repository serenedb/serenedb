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

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_reader.hpp"

namespace irs {

ReadContext::ReadContext(duckdb::DatabaseInstance& db) noexcept
  : IresearchColBlockManager{db} {}

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

void ReadContext::Read(duckdb::QueryContext /*context*/,
                       duckdb::Block& /*block*/) {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ReadContext::Read: unexpected block read on .col read context; "
            "reads go through IndexInput::ReadData");
}

void ReadContext::ReadBlocks(duckdb::FileBuffer& /*buffer*/,
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
