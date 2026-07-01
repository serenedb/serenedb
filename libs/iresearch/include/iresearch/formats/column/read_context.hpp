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

#include "iresearch/formats/column/internal/iresearch_col_block_manager.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class ColReader;

class ReadContext final : public IresearchColBlockManager {
 public:
  explicit ReadContext(duckdb::DatabaseInstance& db) noexcept;
  explicit ReadContext(const ColReader& reader);
  ReadContext(duckdb::DatabaseInstance& db, IndexInput::ptr in);
  ~ReadContext() final;

  ReadContext(const ReadContext&) = delete;
  ReadContext& operator=(const ReadContext&) = delete;
  ReadContext(ReadContext&&) = delete;
  ReadContext& operator=(ReadContext&&) = delete;

  void Reset(const ColReader& reader);

  IndexInput& In() noexcept { return *_in; }
  const IndexInput& In() const noexcept { return *_in; }
  bool HasIn() const noexcept { return _in != nullptr; }

  // duckdb::BlockManager interface -- read side
  void Read(duckdb::QueryContext context, duckdb::Block& block) final;
  void ReadBlocks(duckdb::FileBuffer& buffer, duckdb::block_id_t start_block,
                  duckdb::idx_t block_count) final;

  // duckdb::BlockManager interface -- write side (unused for reads)
  duckdb::block_id_t GetFreeBlockId() final;
  duckdb::block_id_t PeekFreeBlockId() final;
  void Write(duckdb::FileBuffer& block, duckdb::block_id_t block_id) final;
  void Write(duckdb::QueryContext context, duckdb::FileBuffer& block,
             duckdb::block_id_t block_id) final;
  bool IsRootBlock(duckdb::MetaBlockPointer root) final;
  duckdb::idx_t GetMetaBlock() final;
  void WriteHeader(duckdb::QueryContext context,
                   duckdb::DatabaseHeader header) final;
  duckdb::idx_t TotalBlocks() final;

 private:
  IndexInput::ptr _in;
};

}  // namespace irs
