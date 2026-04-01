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
// Secondary index key layout:
//
// Every key starts with <shard (8B)> <dummy column_id=0 (8B)> to satisfy
// CappedPrefixTransform(16).  After the 16-byte prefix, each SK column is
// encoded as a 1-byte null/not-null marker followed by the encoded value
// (omitted when NULL).
//
// 1) Unique, non-NULL:
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//    Value: <PK bytes>
//    One entry per SK value. Uniqueness enforced via GetForUpdate.
//
// 2) Unique, NULL:
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//           <PK bytes>
//    Value: empty
//    PK in key -- multiple NULLs allowed (PostgreSQL semantics).
//
// 3) Non-unique (any value):
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//           <PK bytes>
//    Value: empty
//    PK always in key. Multiple rows per SK value.
//
// Marker values (1 byte): 0x01 = NULL, 0x02 = NOT NULL (nulls-first).
namespace secondary_key {

inline void AppendShardPrefix(std::string& key, ObjectId shard_id) {
  const auto base = key.size();
  basics::StrAppend(key, sizeof(ObjectId));
  absl::big_endian::Store64(key.data() + base, shard_id.id());
}

// Appends a dummy Column::Id (0) so that the key reaches the 16-byte prefix
// required by CappedPrefixTransform(16) on the default column family.
inline void AppendDummyColumnId(std::string& key) {
  const auto base = key.size();
  basics::StrAppend(key, sizeof(catalog::Column::Id));
  absl::big_endian::Store64(key.data() + base, 0);
}

inline void AppendNullMarker(std::string& key) { key.push_back('\1'); }

inline void AppendNotNullMarker(std::string& key) { key.push_back('\2'); }

// Appends <marker><encoded_value> for each SK column.
// Returns true if any SK column is NULL.
inline bool AppendSKValue(std::string& key, const velox::RowVector& input,
                          std::span<const velox::column_index_t> sk_children,
                          velox::vector_size_t row_idx) {
  bool has_null = false;
  for (const auto sk_child : sk_children) {
    const velox::BaseVector& col = *input.childAt(sk_child);
    if (col.isNullAt(row_idx)) {
      AppendNullMarker(key);
      has_null = true;
    } else {
      AppendNotNullMarker(key);
      primary_key::AppendKeyValue(key, col, row_idx);
    }
  }
  return has_null;
}

inline void AppendPK(const velox::RowVector& input,
                     std::span<const velox::column_index_t> pk_children,
                     velox::vector_size_t row_idx, std::string& target) {
  if (!pk_children.empty()) {
    primary_key::Create(input, pk_children, row_idx, target);
  } else {
    primary_key::AppendKeyValue(
      target, *input.childAt(input.childrenSize() - 1), row_idx);
  }
}

// Build secondary index key + value.
//
// Non-unique (any value):
//   key = [shard][marker][SK values][PK]   value = empty
//
// Unique + NULL:
//   key = [shard][marker][PK]              value = empty
//   (PK in key -- multiple NULLs allowed, like non-unique)
//
// Unique + non-NULL:
//   key = [shard][marker][SK values]       value = [PK]
//   (PK as value -- uniqueness enforced)
template<bool Unique>
inline void Create(const velox::RowVector& input,
                   std::span<const velox::column_index_t> pk_children,
                   std::span<const velox::column_index_t> sk_children,
                   velox::vector_size_t row_idx, std::string& sk_buffer,
                   std::string& pk_buffer) {
  bool has_null = AppendSKValue(sk_buffer, input, sk_children, row_idx);
  if constexpr (Unique) {
    if (has_null) {
      AppendPK(input, pk_children, row_idx, sk_buffer);
    } else {
      AppendPK(input, pk_children, row_idx, pk_buffer);
    }
  } else {
    AppendPK(input, pk_children, row_idx, sk_buffer);
  }
}

}  // namespace secondary_key

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
      _sk_children{std::move(sk_children)},
      _trigger_column_id{columns[0]} {}

  bool SwitchColumn(const velox::Type&, bool,
                    catalog::Column::Id column_id) final {
    return column_id == _trigger_column_id;
  }

  void Finish() final {}
  void Abort() final {}

 protected:
  void InitBase(const velox::RowVectorPtr& input) {
    _input = input.get();
    _row_idx = 0;
    _del_row_idx = 0;
  }

  bool BuildSK(std::string_view full_pk, rocksdb::Slice& value) {
    auto pk_bytes = key_utils::ExtractRowKey(full_pk);
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendDummyColumnId(_key_buffer);
    bool has_null = secondary_key::AppendSKValue(_key_buffer, *_input,
                                                 _sk_children, _row_idx);
    if constexpr (Unique) {
      if (has_null) {
        _key_buffer.append(pk_bytes);
      } else {
        value = pk_bytes;
      }
    } else {
      _key_buffer.append(pk_bytes);
    }
    ++_row_idx;
    return has_null;
  }

  std::string_view BuildDeleteSK(
    std::string_view encoded_pk,
    std::span<const velox::column_index_t> sk_children) {
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendDummyColumnId(_key_buffer);
    bool has_null = secondary_key::AppendSKValue(_key_buffer, *_input,
                                                 sk_children, _del_row_idx);
    if constexpr (Unique) {
      if (has_null) {
        _key_buffer.append(encoded_pk);
      }
    } else {
      _key_buffer.append(encoded_pk);
    }
    ++_del_row_idx;
    return _key_buffer;
  }

  rocksdb::Transaction& _trx;
  ObjectId _shard_id;
  std::vector<velox::column_index_t> _sk_children;
  catalog::Column::Id _trigger_column_id;
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
    bool has_null = this->BuildSK(full_pk, value);
    if constexpr (Unique) {
      if (!has_null) {
        rocksdb::PinnableSlice existing;
        auto gs = this->_trx.GetForUpdate(rocksdb::ReadOptions{},
                                          this->_key_buffer, &existing);
        if (gs.ok()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
            ERR_MSG("duplicate key value violates unique constraint on "
                    "secondary index"),
            ERR_DETAIL(BuildUniqueViolationDetail(
              this->_sk_children, *this->_input, this->_row_idx - 1)));
        }
      }
    }
    auto s = this->_trx.Put(this->_key_buffer, value);
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
    bool has_null = this->BuildSK(full_key, value);
    if constexpr (Unique) {
      if (!has_null) {
        rocksdb::PinnableSlice existing;
        auto gs = this->_trx.GetForUpdate(rocksdb::ReadOptions{},
                                          this->_key_buffer, &existing);
        if (gs.ok()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
            ERR_MSG("duplicate key value violates unique constraint on "
                    "secondary index"),
            ERR_DETAIL(BuildUniqueViolationDetail(
              this->_sk_children, *this->_input, this->_row_idx - 1)));
        }
      }
    }
    auto s = this->_trx.Put(this->_key_buffer, value);
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
