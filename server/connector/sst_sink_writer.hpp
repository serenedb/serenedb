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
#include "connector/primary_key.hpp"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

namespace sdb::connector {

template<bool IsGeneratedPK>
class SSTBlockBuilder {
 public:
  SSTBlockBuilder(int64_t generated_pk_counter, ObjectId table_id,
                  catalog::Column::Id column_id);

  void SetRowIdx(size_t row_idx)
    requires(!IsGeneratedPK)
  {
    _row_idx = row_idx;
  }

  void SetKeys(const primary_key::Keys* keys)
    requires(!IsGeneratedPK)
  {
    _keys = keys;
  }

  void AddEntry(std::span<const rocksdb::Slice> value_slices);

  bool ShouldFlush() const { return _cur.buffer.size() >= kFlushThreshold; }

  // Appends some block metadata as BlockBuilder does
  rocksdb::BlockFlushData Finish(
    std::span<const rocksdb::Slice> next_block_first_value);

  void NextBlock();

  bool IsEmpty() const { return _cur.entry_cnt == 0; }

 private:
  struct UnitializedChar {
    char val;

    // necessary to prevent zero-initialization of val
    UnitializedChar() {}

    operator char() const { return val; }
  };

  struct Block {
    std::vector<UnitializedChar> buffer;
    size_t last_pk_size = 0;
    size_t last_pk_offset = 0;

    uint64_t entry_cnt = 0;
    size_t raw_key_size = 0;
    size_t raw_value_size = 0;
  };

  void AddEntryImpl(Block& block, std::span<const rocksdb::Slice> value_slices);

  std::string BuildLastKey() const;

  size_t GetPKSize() const;

  static constexpr size_t kFlushThreshold = 64 * 1024;  // 64 KB
  static constexpr size_t kPrefixSize =
    sizeof(ObjectId) + sizeof(catalog::Column::Id);

  Block _cur;
  Block _next;

  int64_t _generated_pk_counter = 0;
  ObjectId _table_id;
  catalog::Column::Id _column_id = 0;
  uint64_t _total_entry_cnt = 0;

  std::string _last_key_buffer;
  size_t _row_idx = 0;
  const primary_key::Keys* _keys = nullptr;
};

extern template class SSTBlockBuilder<true>;
extern template class SSTBlockBuilder<false>;

template<bool IsGeneratedPK>
class SSTSinkWriter {
 public:
  SSTSinkWriter(ObjectId table_id, rocksdb::DB& db,
                rocksdb::ColumnFamilyHandle& cf,
                std::span<const ColumnInfo> columns);

  void SetColumnIndex(size_t column_idx) { _column_idx = column_idx; }

  void SetRowIdx(size_t row_idx)
    requires(!IsGeneratedPK)
  {
    SDB_ASSERT(_column_idx < _block_builders.size());
    SDB_ASSERT(_column_idx < _block_builders.size());
    if (_block_builders[_column_idx]) {
      _block_builders[_column_idx]->SetRowIdx(row_idx);
    }
  }

  void SetKeys(const primary_key::Keys* keys)
    requires(!IsGeneratedPK)
  {
    for (auto& builder : _block_builders) {
      if (builder) {
        builder->SetKeys(keys);
      }
    }
  }

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
  std::vector<std::unique_ptr<SSTBlockBuilder<IsGeneratedPK>>> _block_builders;
  std::string _sst_directory;
  int64_t _column_idx = -1;
};

extern template class SSTSinkWriter<true>;
extern template class SSTSinkWriter<false>;

template<typename T>
inline constexpr bool kIsSSTSinkWriter = false;
template<bool IsGeneratedPK>
inline constexpr bool kIsSSTSinkWriter<SSTSinkWriter<IsGeneratedPK>> = true;

}  // namespace sdb::connector
