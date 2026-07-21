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

#include "iresearch/formats/column/internal/block_manager.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class WriteContext final : public BlockManager {
 public:
  WriteContext(duckdb::DatabaseInstance& db, IndexOutput& out);
  ~WriteContext() final;

  WriteContext(const WriteContext&) = delete;
  WriteContext& operator=(const WriteContext&) = delete;
  WriteContext(WriteContext&&) = delete;
  WriteContext& operator=(WriteContext&&) = delete;

  IndexOutput& Out() noexcept { return *_out; }

  duckdb::block_id_t GetFreeBlockId() final;
  duckdb::block_id_t PeekFreeBlockId() final;
  void Write(duckdb::FileBuffer& block, duckdb::block_id_t block_id) final;
  void Write(duckdb::QueryContext context, duckdb::FileBuffer& block,
             duckdb::block_id_t block_id) final;

  void Read(duckdb::QueryContext context, duckdb::Block& block) final;
  void ReadBlocks(duckdb::QueryContext context, duckdb::FileBuffer& buffer,
                  duckdb::block_id_t start_block,
                  duckdb::idx_t block_count) final;

  bool IsRootBlock(duckdb::MetaBlockPointer root) final;
  duckdb::idx_t GetMetaBlock() final;
  void WriteHeader(duckdb::QueryContext context,
                   duckdb::DatabaseHeader header) final;
  duckdb::idx_t TotalBlocks() final;

 private:
  IndexOutput* _out;
  duckdb::block_id_t _next_id = 0;
};

}  // namespace irs
