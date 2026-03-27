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

#include <absl/base/internal/endian.h>
#include <velox/type/Type.h>
#include <velox/vector/ComplexVector.h>

#include "key_utils.hpp"
#include "primary_key.hpp"
#include "rocksdb/utilities/transaction.h"
#include "sink_writer_base.hpp"

namespace sdb::connector {

// Utilities for building secondary index keys.
//
// Key layout:
//   <shard_ObjectId (8B)> <null_marker + sortable_encoded_value>... <PK bytes>
//
// Null marker: 0x00 = NULL (sorts first), 0x01 = NOT NULL.
// Values are encoded in binary-sortable format (big-endian, sign-bit flipped
// for signed integers) so that RocksDB's default byte-wise comparator produces
// the correct ordering.
namespace secondary_key {

inline void AppendShardPrefix(std::string& key, ObjectId shard_id) {
  const auto base = key.size();
  basics::StrAppend(key, sizeof(ObjectId));
  absl::big_endian::Store64(key.data() + base, shard_id.id());
}

constexpr char kNull = '\x00';
constexpr char kNotNull = '\x01';

inline void AppendNullMarker(std::string& key) { key.push_back(kNull); }
inline void AppendNotNullMarker(std::string& key) { key.push_back(kNotNull); }

// Build a secondary index key: <null_marker><encoded_value><PK_bytes>.
// For generated PK (key_childs empty), reads PK from the last input column.
// Append <null_marker><encoded_value> for the indexed column.
inline void AppendValue(std::string& key, const velox::BaseVector& col,
                        velox::vector_size_t row_idx) {
  if (col.isNullAt(row_idx)) {
    AppendNullMarker(key);
  } else {
    AppendNotNullMarker(key);
    primary_key::AppendKeyValue(key, col, row_idx);
  }
}

inline void Create(const velox::RowVector& input,
                   std::span<const velox::column_index_t> key_childs,
                   velox::column_index_t indexed_col_idx,
                   velox::vector_size_t row_idx, std::string& key) {
  AppendValue(key, *input.childAt(indexed_col_idx), row_idx);
  if (!key_childs.empty()) {
    primary_key::Create(input, key_childs, row_idx, key);
  } else {
    // Generated PK is the last column (same convention as MakeColumnKey)
    primary_key::AppendKeyValue(key, *input.childAt(input.childrenSize() - 1),
                                row_idx);
  }
}

}  // namespace secondary_key

// Base for secondary index writers. Stores the input RowVector from Init
// and builds keys using primary_key::AppendKeyValue (no re-encoding needed).
class SecondarySinkWriteBase : public SinkIndexWriter,
                               public ColumnSinkWriterImplBase {
 public:
  SecondarySinkWriteBase(rocksdb::Transaction& trx, ObjectId shard_id,
                         std::span<const catalog::Column::Id> columns,
                         std::string indexed_col_name)
    : ColumnSinkWriterImplBase{columns},
      _trx{trx},
      _shard_id{shard_id},
      _indexed_col_name{std::move(indexed_col_name)} {}

  bool SwitchColumn(const velox::Type&, bool,
                    catalog::Column::Id column_id) final {
    return IsIndexed(column_id);
  }

  void Finish() final {}
  void Abort() final {}

 protected:
  void InitBase(const velox::RowVectorPtr& input) {
    _input = input;
    _row_idx = 0;
    auto maybe = input->type()->asRow().getChildIdxIfExists(_indexed_col_name);
    _indexed_col_idx = maybe.value_or(0);
  }

  void BuildKeyFromVector(std::string_view full_key) {
    auto pk_bytes = key_utils::ExtractRowKey(full_key);
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendValue(_key_buffer, *_input->childAt(_indexed_col_idx),
                               _row_idx);
    _key_buffer.append(pk_bytes.data(), pk_bytes.size());
    ++_row_idx;
  }

  rocksdb::Transaction& _trx;
  ObjectId _shard_id;
  velox::RowVectorPtr _input;
  std::string _indexed_col_name;
  velox::column_index_t _indexed_col_idx{0};
  velox::vector_size_t _row_idx{0};
  std::string _key_buffer;
};

// INSERT writer
class SecondarySinkInsertWriter final : public SecondarySinkWriteBase {
 public:
  using SecondarySinkWriteBase::SecondarySinkWriteBase;

  void Init(size_t, const velox::RowVectorPtr& input) final { InitBase(input); }

  void Write(std::span<const rocksdb::Slice>, std::string_view full_key) final {
    BuildKeyFromVector(full_key);
    auto s = _trx.Put(_key_buffer, rocksdb::Slice{});
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }
};

// DELETE writer — indexed column is in the input (added by sql analyzer).
class SecondarySinkDeleteWriter final : public SecondarySinkWriteBase {
 public:
  using SecondarySinkWriteBase::SecondarySinkWriteBase;

  void Init(size_t, const velox::RowVectorPtr& input) final { InitBase(input); }

  void DeleteRow(std::string_view encoded_pk) final {
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendValue(_key_buffer, *_input->childAt(_indexed_col_idx),
                               _row_idx);
    _key_buffer.append(encoded_pk.data(), encoded_pk.size());
    ++_row_idx;
    _trx.Delete(_key_buffer);
  }
};

// UPDATE writer — old value is in the input as _sdb_old_<name> (added by
// sql analyzer). DeleteRow uses old value, Write uses new value.
class SecondarySinkUpdateWriter final : public SecondarySinkWriteBase {
 public:
  using SecondarySinkWriteBase::SecondarySinkWriteBase;

  void Init(size_t, const velox::RowVectorPtr& input) final {
    InitBase(input);
    auto old_name = "_sdb_old_" + _indexed_col_name;
    auto maybe = input->type()->asRow().getChildIdxIfExists(old_name);
    _old_col_idx = maybe.value_or(_indexed_col_idx);
    _del_row_idx = 0;
  }

  void Write(std::span<const rocksdb::Slice>, std::string_view full_key) final {
    BuildKeyFromVector(full_key);
    auto s = _trx.Put(_key_buffer, rocksdb::Slice{});
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }

  void DeleteRow(std::string_view encoded_pk) final {
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendValue(_key_buffer, *_input->childAt(_old_col_idx),
                               _del_row_idx);

    _key_buffer.append(encoded_pk.data(), encoded_pk.size());
    ++_del_row_idx;
    _trx.Delete(_key_buffer);
  }

 private:
  velox::column_index_t _old_col_idx{0};
  velox::vector_size_t _del_row_idx{0};
};

// Secondary index backfill is handled by SSTInsertDataSink<false, true>.

}  // namespace sdb::connector
