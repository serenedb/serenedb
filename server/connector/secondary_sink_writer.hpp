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

#include "common.h"
#include "key_utils.hpp"
#include "pg/sql_exception_macro.h"
#include "primary_key.hpp"
#include "rocksdb/utilities/transaction.h"
#include "sink_writer_base.hpp"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

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
inline void AppendSKValue(std::string& key, const velox::RowVector& input,
                          std::span<const velox::column_index_t> sk_children,
                          velox::vector_size_t row_idx) {
  for (const auto sk_child : sk_children) {
    const velox::BaseVector& col = *input.childAt(sk_child);
    if (col.isNullAt(row_idx)) {
      AppendNullMarker(key);
    } else {
      AppendNotNullMarker(key);
      primary_key::AppendKeyValue(key, col, row_idx);
    }
  }
}

// Non-unique: appends SK values + PK to `key`.
// Unique: appends SK values to `key`, PK to `value`.
template<bool Unique>
inline void Create(const velox::RowVector& input,
                   std::span<const velox::column_index_t> pk_children,
                   std::span<const velox::column_index_t> sk_children,
                   velox::vector_size_t row_idx, std::string& sk_buffer,
                   std::string& pk_buffer) {
  AppendSKValue(sk_buffer, input, sk_children, row_idx);
  auto& pk_target = Unique ? pk_buffer : sk_buffer;
  if (!pk_children.empty()) {
    primary_key::Create(input, pk_children, row_idx, pk_target);
  } else {  // generated pk
    const velox::BaseVector& pk_col = *input.childAt(input.childrenSize() - 1);
    primary_key::AppendKeyValue(pk_target, pk_col, row_idx);
  }
}

}  // namespace secondary_key

// Base for secondary index writers. Stores the input RowVector from Init
// and builds keys using primary_key::AppendKeyValue (no re-encoding needed).
template<bool Unique>
class SecondarySinkWriteBase : public SinkIndexWriter,
                               public ColumnSinkWriterImplBase {
 public:
  SecondarySinkWriteBase(rocksdb::Transaction& trx, ObjectId shard_id,
                         std::span<const catalog::Column::Id> columns,
                         std::vector<velox::column_index_t> sk_children)
    : ColumnSinkWriterImplBase{columns},
      _trx{trx},
      _shard_id{shard_id},
      _sk_children{std::move(sk_children)} {}

  bool SwitchColumn(const velox::Type&, bool,
                    catalog::Column::Id column_id) final {
    return IsIndexed(column_id);
  }

  void Finish() final {}
  void Abort() final {}

 protected:
  void InitBase(const velox::RowVectorPtr& input) {
    _input = input.get();
    _row_idx = 0;
    _del_row_idx = 0;
  }

  std::string_view BuildSK(std::string_view full_pk, rocksdb::Slice& value) {
    auto pk_bytes = key_utils::ExtractRowKey(full_pk);
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendSKValue(_key_buffer, *_input, _sk_children, _row_idx);
    if constexpr (Unique) {
      value = rocksdb::Slice{pk_bytes.data(), pk_bytes.size()};
    } else {
      _key_buffer.append(pk_bytes.data(), pk_bytes.size());
      value = rocksdb::Slice{};
    }
    ++_row_idx;
    return _key_buffer;
  }

  std::string_view BuildDeleteSK(
    std::string_view encoded_pk,
    std::span<const velox::column_index_t> sk_children) {
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendSKValue(_key_buffer, *_input, sk_children,
                                 _del_row_idx);
    if constexpr (!Unique) {
      _key_buffer.append(encoded_pk.data(), encoded_pk.size());
    }
    ++_del_row_idx;
    return _key_buffer;
  }

  rocksdb::Transaction& _trx;
  ObjectId _shard_id;
  std::vector<velox::column_index_t> _sk_children;
  const velox::RowVector* _input;
  velox::vector_size_t _row_idx;
  velox::vector_size_t _del_row_idx;
  std::string _key_buffer;
};

template<bool Unique>
class SecondarySinkInsertWriter final : public SecondarySinkWriteBase<Unique> {
  using Base = SecondarySinkWriteBase<Unique>;

 public:
  using Base::Base;

  void Init(size_t batch_size, const velox::RowVectorPtr& input) final {
    Base::InitBase(input);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_pk) final {
    rocksdb::Slice value;
    auto sk = this->BuildSK(full_pk, value);
    if constexpr (Unique) {
      rocksdb::PinnableSlice existing;
      auto gs = this->_trx.GetForUpdate(rocksdb::ReadOptions{}, sk, &existing);
      if (gs.ok()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
          ERR_MSG("duplicate key value violates unique constraint on "
                  "secondary index"),
          ERR_DETAIL(BuildUniqueViolationDetail(
            this->_sk_children, *this->_input, this->_row_idx - 1)));
      }
    }
    auto s = this->_trx.Put(sk, value);
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }
};

template<bool Unique>
class SecondarySinkDeleteWriter final : public SecondarySinkWriteBase<Unique> {
  using Base = SecondarySinkWriteBase<Unique>;

 public:
  using Base::Base;

  void Init(size_t batch_size, const velox::RowVectorPtr& input) final {
    Base::InitBase(input);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    auto s =
      this->_trx.Delete(this->BuildDeleteSK(encoded_pk, this->_sk_children));
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }
};

template<bool Unique>
class SecondarySinkUpdateWriter final : public SecondarySinkWriteBase<Unique> {
  using Base = SecondarySinkWriteBase<Unique>;

 public:
  SecondarySinkUpdateWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            std::span<const catalog::Column::Id> columns,
                            std::vector<velox::column_index_t> sk_children,
                            std::vector<velox::column_index_t> old_sk_children)
    : Base{trx, shard_id, columns, std::move(sk_children)},
      _old_sk_children{std::move(old_sk_children)} {}

  void Init(size_t batch_size, const velox::RowVectorPtr& input) final {
    Base::InitBase(input);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    rocksdb::Slice value;
    auto sk = this->BuildSK(full_key, value);
    if constexpr (Unique) {
      rocksdb::PinnableSlice existing;
      auto gs = this->_trx.GetForUpdate(rocksdb::ReadOptions{}, sk, &existing);
      if (gs.ok()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
          ERR_MSG("duplicate key value violates unique constraint on "
                  "secondary index"),
          ERR_DETAIL(BuildUniqueViolationDetail(
            this->_sk_children, *this->_input, this->_row_idx - 1)));
      }
    }
    auto s = this->_trx.Put(sk, value);
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }

  void DeleteRow(std::string_view encoded_pk) final {
    auto s =
      this->_trx.Delete(this->BuildDeleteSK(encoded_pk, _old_sk_children));
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }

 private:
  std::vector<velox::column_index_t> _old_sk_children;
};

}  // namespace sdb::connector
