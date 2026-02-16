////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <memory>
#include <span>
#include <string>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "common.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

namespace sdb::connector {

class SSTBlockBuilder {
 public:
  SSTBlockBuilder(int64_t generated_pk_counter, ObjectId table_id,
                  catalog::Column::Id column_id);

  void AddEntry(std::span<const rocksdb::Slice> value_slices);

  bool ShouldFlush() const { return _cur.buffer.size() >= kFlushThreshold; }

  std::string BuildNextKey() const;

  // Appends some block metadata as BlockBuilder does
  rocksdb::BlockFlushData Finish(
    std::span<const rocksdb::Slice> next_block_first_value);

  void NextBlock();

  bool IsEmpty() const { return _cur.entry_cnt == 0; }

 private:
  struct Block {
    std::string buffer;
    size_t last_pk_size = 0;
    size_t last_pk_offset = 0;

    uint64_t entry_cnt = 0;
    size_t raw_key_size = 0;
    size_t raw_value_size = 0;
  };

  void AddEntryImpl(Block& block, std::span<const rocksdb::Slice> value_slices);

  std::string BuildLastKey() const;

  size_t AppendPK(Block& block);

  static constexpr size_t kFlushThreshold = 64 * 1024;  // 1MB
  static constexpr size_t kPrefixSize =
    sizeof(ObjectId) + sizeof(catalog::Column::Id);

  Block _cur;
  Block _next;

  int64_t _generated_pk_counter;
  ObjectId _table_id;
  catalog::Column::Id _column_id;
  uint64_t _total_entry_cnt = 0;

  std::string _last_key_buffer;
};

class SSTSinkWriter {
 public:
  SSTSinkWriter(ObjectId table_id, rocksdb::DB& db,
                rocksdb::ColumnFamilyHandle& cf,
                std::span<const ColumnInfo> columns);

  void SetColumnIndex(size_t column_idx) { _column_idx = column_idx; }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key);

  void Finish();

  void Abort();

 private:
  void FlushBlockBuilder(
    size_t column_idx, std::span<const rocksdb::Slice> next_block_first_value);

  rocksdb::DB* _db;
  rocksdb::ColumnFamilyHandle* _cf;
  std::vector<std::unique_ptr<rocksdb::SstFileWriter>> _writers;
  std::vector<std::unique_ptr<SSTBlockBuilder>> _block_builders;
  std::string _sst_directory;
  std::string _next_key_buffer;
  int64_t _column_idx = -1;
};

}  // namespace sdb::connector
