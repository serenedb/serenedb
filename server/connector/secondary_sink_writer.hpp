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

#include <functional>

#include "key_utils.hpp"
#include "primary_key.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"
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

// Re-encode a signed integer from little-endian RocksDB storage format to
// big-endian sortable format (with sign-bit flip).
template<typename T>
void AppendSortableSigned(std::string& key, const char* le_data) {
  static_assert(std::is_signed_v<T>);
  T value;
  std::memcpy(&value, le_data, sizeof(T));
  primary_key::AppendSigned(key, value);
}

// Re-encode a floating-point value from little-endian storage to sortable
// format.
inline void AppendSortableFloat(std::string& key, const char* le_data) {
  float value;
  std::memcpy(&value, le_data, sizeof(float));
  // Use the same encoding as primary_key for float: convert to sortable int32
  auto bits = std::bit_cast<uint32_t>(value);
  if (bits & 0x80000000u) {
    bits = ~bits;  // negative: flip all bits
  } else {
    bits ^= 0x80000000u;  // positive: flip sign bit
  }
  const auto base = key.size();
  basics::StrAppend(key, sizeof(uint32_t));
  absl::big_endian::Store32(key.data() + base, bits);
}

inline void AppendSortableDouble(std::string& key, const char* le_data) {
  double value;
  std::memcpy(&value, le_data, sizeof(double));
  auto bits = std::bit_cast<uint64_t>(value);
  if (bits & 0x8000000000000000ull) {
    bits = ~bits;
  } else {
    bits ^= 0x8000000000000000ull;
  }
  const auto base = key.size();
  basics::StrAppend(key, sizeof(uint64_t));
  absl::big_endian::Store64(key.data() + base, bits);
}

// Append a string value. The stored format has a \x00 prefix for empty strings
// or strings starting with \x00. For the index key we use the primary_key
// string encoding: raw bytes terminated with \x00\x00, with embedded \x00
// escaped as \x00\x01.
inline void AppendSortableString(std::string& key,
                                 std::span<const rocksdb::Slice> cell_slices) {
  // Reconstruct the original string from the stored format.
  // Storage: if empty or starts with \x00, a \x00 prefix byte is prepended.
  // We need to strip that prefix to get the real string, then re-encode.
  std::string_view raw;
  std::string concat;

  if (cell_slices.size() == 1) {
    raw = {cell_slices[0].data(), cell_slices[0].size()};
  } else {
    // Multiple slices: prefix byte + actual data
    for (const auto& s : cell_slices) {
      concat.append(s.data(), s.size());
    }
    raw = concat;
  }

  // Strip the \x00 prefix byte if present (used for empty strings and strings
  // starting with \x00)
  if (!raw.empty() && raw[0] == '\x00') {
    raw.remove_prefix(1);
  }

  // Encode using primary_key string format: raw bytes + \x00\x00 terminator,
  // with \x00 in the string escaped as \x00\x01.
  for (char c : raw) {
    if (c == '\x00') {
      key.push_back('\x00');
      key.push_back('\x01');
    } else {
      key.push_back(c);
    }
  }
  key.push_back('\x00');
  key.push_back('\x00');
}

// Append a boolean value (1 byte: 0x00 false, 0x01 true).
inline void AppendSortableBool(std::string& key,
                               std::span<const rocksdb::Slice> cell_slices) {
  SDB_ASSERT(!cell_slices.empty() && !cell_slices[0].empty());
  key.push_back(cell_slices[0].data()[0]);
}

// Build a secondary index key: <null_marker><encoded_value><PK_bytes>.
// For generated PK (key_childs empty), reads PK from the last input column.
inline void Create(const velox::RowVector& input,
                   std::span<const velox::column_index_t> key_childs,
                   velox::column_index_t indexed_col_idx,
                   velox::vector_size_t row_idx, std::string& key) {
  const auto& indexed_col = *input.childAt(indexed_col_idx);
  if (indexed_col.isNullAt(row_idx)) {
    AppendNullMarker(key);
  } else {
    AppendNotNullMarker(key);
    primary_key::AppendKeyValue(key, indexed_col, row_idx);
  }
  if (!key_childs.empty()) {
    primary_key::Create(input, key_childs, row_idx, key);
  } else {
    // Generated PK is the last column (same convention as MakeColumnKey)
    primary_key::AppendKeyValue(
      key, *input.childAt(input.childrenSize() - 1), row_idx);
  }
}

}  // namespace secondary_key

// Type-erased value encoder set per column in SwitchColumn.
using SecondaryValueEncoder = std::function<void(
  std::string& key, std::span<const rocksdb::Slice> cell_slices)>;

// Re-encode a RocksDB column value (little-endian) into sortable key format.
inline void EncodeOldValueToSortableKey(std::string& key,
                                        const rocksdb::Slice& value,
                                        velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN:
      key.push_back(value.data()[0]);
      break;
    case velox::TypeKind::TINYINT:
      primary_key::AppendSigned(key,
        absl::little_endian::Load<int8_t>(value.data()));
      break;
    case velox::TypeKind::SMALLINT:
      primary_key::AppendSigned(key,
        absl::little_endian::Load<int16_t>(value.data()));
      break;
    case velox::TypeKind::INTEGER:
      primary_key::AppendSigned(key,
        absl::little_endian::Load<int32_t>(value.data()));
      break;
    case velox::TypeKind::BIGINT:
      primary_key::AppendSigned(key,
        absl::little_endian::Load<int64_t>(value.data()));
      break;
    case velox::TypeKind::REAL: {
      float v;
      std::memcpy(&v, value.data(), sizeof(float));
      auto bits = std::bit_cast<uint32_t>(v);
      if (bits & 0x80000000u) bits = ~bits;
      else bits ^= 0x80000000u;
      auto base = key.size();
      basics::StrAppend(key, sizeof(uint32_t));
      absl::big_endian::Store32(key.data() + base, bits);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      double v;
      std::memcpy(&v, value.data(), sizeof(double));
      auto bits = std::bit_cast<uint64_t>(v);
      if (bits & 0x8000000000000000ull) bits = ~bits;
      else bits ^= 0x8000000000000000ull;
      auto base = key.size();
      basics::StrAppend(key, sizeof(uint64_t));
      absl::big_endian::Store64(key.data() + base, bits);
      break;
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      for (size_t i = 0; i < value.size(); ++i) {
        char c = value.data()[i];
        if (c == '\x00') {
          key.push_back('\x00');
          key.push_back('\x01');
        } else {
          key.push_back(c);
        }
      }
      key.push_back('\x00');
      key.push_back('\x00');
      break;
    }
    default:
      SDB_ASSERT(false, "Unsupported type kind for secondary index");
  }
}

// Read old column value from the main table by PK, encode it, and delete
// the corresponding secondary index entry. Used by UPDATE and DELETE writers.
inline void DeleteOldEntryByPK(
  std::string_view encoded_pk, rocksdb::Transaction& trx, ObjectId shard_id,
  ObjectId table_id, catalog::Column::Id indexed_column_id,
  velox::TypeKind indexed_type_kind,
  std::string& old_key_buf, std::string& delete_key_buf) {
  // Build key: <table_id><column_id><pk> to read old column value
  old_key_buf.clear();
  basics::StrAppend(old_key_buf,
                    sizeof(ObjectId) + sizeof(catalog::Column::Id));
  absl::big_endian::Store64(old_key_buf.data(), table_id.id());
  key_utils::SetupColumnForKey(old_key_buf, indexed_column_id);
  old_key_buf.append(encoded_pk.data(), encoded_pk.size());

  std::string old_value;
  auto s = trx.Get(rocksdb::ReadOptions{}, old_key_buf, &old_value);

  // Build and delete old secondary index entry
  delete_key_buf.clear();
  secondary_key::AppendShardPrefix(delete_key_buf, shard_id);

  if (!s.ok() || old_value.empty()) {
    secondary_key::AppendNullMarker(delete_key_buf);
  } else {
    secondary_key::AppendNotNullMarker(delete_key_buf);
    rocksdb::Slice old_slice{old_value};
    EncodeOldValueToSortableKey(delete_key_buf, old_slice, indexed_type_kind);
  }

  delete_key_buf.append(encoded_pk.data(), encoded_pk.size());
  trx.Delete(delete_key_buf);
}

// Base implementation shared by insert/update/backfill writers.
class SecondarySinkWriteBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SecondarySinkWriteBaseImpl(rocksdb::Transaction& trx, ObjectId shard_id,
                             std::span<const catalog::Column::Id> columns)
    : ColumnSinkWriterImplBase{columns},
      _trx{trx},
      _shard_id{shard_id} {}

  void InitImpl(size_t /*batch_size*/) {}

  bool SwitchColumnImpl(const velox::Type& type, bool have_nulls,
                        catalog::Column::Id column_id) {
    if (!IsIndexed(column_id)) {
      return false;
    }
    _current_column_may_have_nulls = have_nulls;
    SetupEncoder(type.kind());
    return true;
  }

  void FinishImpl() {}
  void AbortImpl() {}

 protected:
  void PutImpl(std::span<const rocksdb::Slice> cell_slices,
               std::string_view full_key) {
    BuildKey(cell_slices, full_key);
    auto s = _trx.Put(_key_buffer, rocksdb::Slice{});
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }

  void DeleteImpl(std::span<const rocksdb::Slice> cell_slices,
                  std::string_view full_key) {
    BuildKey(cell_slices, full_key);
    auto s = _trx.Delete(_key_buffer);
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }

  rocksdb::Transaction& _trx;
  ObjectId _shard_id;

 private:
  void BuildKey(std::span<const rocksdb::Slice> cell_slices,
                std::string_view full_key) {
    auto pk_bytes = key_utils::ExtractRowKey(full_key);
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);

    bool is_null = _current_column_may_have_nulls && cell_slices.size() == 1 &&
                   cell_slices[0].empty();
    if (is_null) {
      secondary_key::AppendNullMarker(_key_buffer);
    } else {
      secondary_key::AppendNotNullMarker(_key_buffer);
      SDB_ASSERT(_encoder, "Encoder not set — SwitchColumn not called?");
      _encoder(_key_buffer, cell_slices);
    }

    _key_buffer.append(pk_bytes.data(), pk_bytes.size());
  }

  void SetupEncoder(velox::TypeKind kind) {
    switch (kind) {
      case velox::TypeKind::BOOLEAN:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableBool(key, s);
        };
        break;
      case velox::TypeKind::TINYINT:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableSigned<int8_t>(key, s[0].data());
        };
        break;
      case velox::TypeKind::SMALLINT:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableSigned<int16_t>(key, s[0].data());
        };
        break;
      case velox::TypeKind::INTEGER:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableSigned<int32_t>(key, s[0].data());
        };
        break;
      case velox::TypeKind::BIGINT:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableSigned<int64_t>(key, s[0].data());
        };
        break;
      case velox::TypeKind::REAL:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableFloat(key, s[0].data());
        };
        break;
      case velox::TypeKind::DOUBLE:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableDouble(key, s[0].data());
        };
        break;
      case velox::TypeKind::VARCHAR:
      case velox::TypeKind::VARBINARY:
        _encoder = [](std::string& key, std::span<const rocksdb::Slice> s) {
          secondary_key::AppendSortableString(key, s);
        };
        break;
      default:
        SDB_ASSERT(false, "Unsupported type kind for secondary index: ",
                   velox::TypeKindName::toName(kind));
    }
  }

  std::string _key_buffer;
  SecondaryValueEncoder _encoder;
  bool _current_column_may_have_nulls{false};
};

// INSERT writer: maintains secondary index on row insertion.
class SecondarySinkInsertWriter final : public SinkIndexWriter,
                                        public SecondarySinkWriteBaseImpl {
 public:
  SecondarySinkInsertWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            std::span<const catalog::Column::Id> columns)
    : SecondarySinkWriteBaseImpl{trx, shard_id, columns} {}

  void Init(size_t batch_size) final { InitImpl(batch_size); }

  bool SwitchColumn(const velox::Type& type, bool have_nulls,
                    catalog::Column::Id column_id) final {
    return SwitchColumnImpl(type, have_nulls, column_id);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    PutImpl(cell_slices, full_key);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }
};

// DELETE writer: removes secondary index entries on row deletion.
// DeleteRow reads old column value from RocksDB, encodes it, and deletes
// the corresponding secondary index entry.
class SecondarySinkDeleteWriter final : public SinkIndexWriter,
                                        public SecondarySinkWriteBaseImpl {
 public:
  SecondarySinkDeleteWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            ObjectId table_id,
                            std::span<const catalog::Column::Id> columns,
                            velox::TypeKind indexed_type_kind)
    : SecondarySinkWriteBaseImpl{trx, shard_id, columns},
      _table_id{table_id},
      _indexed_column_id{columns[0]},
      _indexed_type_kind{indexed_type_kind} {}

  void Init(size_t batch_size) final { InitImpl(batch_size); }

  bool SwitchColumn(const velox::Type& type, bool have_nulls,
                    catalog::Column::Id column_id) final {
    return SwitchColumnImpl(type, have_nulls, column_id);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    DeleteImpl(cell_slices, full_key);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteOldEntryByPK(encoded_pk, _trx, _shard_id, _table_id,
                       _indexed_column_id, _indexed_type_kind,
                       _old_key_buffer, _delete_key_buffer);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }

 private:
  ObjectId _table_id;
  catalog::Column::Id _indexed_column_id;
  velox::TypeKind _indexed_type_kind;
  std::string _old_key_buffer;
  std::string _delete_key_buffer;
};

// UPDATE writer: deletes old index entry, inserts new one.
// DeleteRow reads old column values from RocksDB, encodes them,
// and deletes the corresponding secondary index entry.
class SecondarySinkUpdateWriter final : public SinkIndexWriter,
                                        public SecondarySinkWriteBaseImpl {
 public:
  SecondarySinkUpdateWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            ObjectId table_id,
                            std::span<const catalog::Column::Id> columns,
                            velox::TypeKind indexed_type_kind)
    : SecondarySinkWriteBaseImpl{trx, shard_id, columns},
      _table_id{table_id},
      _indexed_column_id{columns[0]},
      _indexed_type_kind{indexed_type_kind} {}

  void Init(size_t batch_size) final {
    SecondarySinkWriteBaseImpl::InitImpl(batch_size);
  }

  bool SwitchColumn(const velox::Type& type, bool have_nulls,
                    catalog::Column::Id column_id) final {
    return SecondarySinkWriteBaseImpl::SwitchColumnImpl(type, have_nulls,
                                                        column_id);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    PutImpl(cell_slices, full_key);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteOldEntryByPK(encoded_pk, _trx, _shard_id, _table_id,
                       _indexed_column_id, _indexed_type_kind,
                       _old_key_buffer, _delete_key_buffer);
  }

  void Finish() final { SecondarySinkWriteBaseImpl::FinishImpl(); }

  void Abort() final { SecondarySinkWriteBaseImpl::AbortImpl(); }

 private:
  ObjectId _table_id;
  catalog::Column::Id _indexed_column_id;
  velox::TypeKind _indexed_type_kind;
  std::string _old_key_buffer;
  std::string _delete_key_buffer;
};

// Secondary index backfill is handled by SSTInsertDataSink<false, true>.

}  // namespace sdb::connector
