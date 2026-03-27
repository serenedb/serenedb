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
    primary_key::AppendKeyValue(key, *input.childAt(input.childrenSize() - 1),
                                row_idx);
  }
}

// Encode a value (in RocksDB little-endian storage format) into sortable key.
// Used by both SinkIndexWriter (cell_slices from Write) and DeleteRow (raw
// bytes from trx.Get). Single source of truth for all value encoding.
inline void EncodeValue(std::string& key, velox::TypeKind kind,
                        const char* data, size_t size) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN:
      key.push_back(data[0]);
      break;
    case velox::TypeKind::TINYINT:
      AppendSortableSigned<int8_t>(key, data);
      break;
    case velox::TypeKind::SMALLINT:
      AppendSortableSigned<int16_t>(key, data);
      break;
    case velox::TypeKind::INTEGER:
      AppendSortableSigned<int32_t>(key, data);
      break;
    case velox::TypeKind::BIGINT:
      AppendSortableSigned<int64_t>(key, data);
      break;
    case velox::TypeKind::REAL:
      AppendSortableFloat(key, data);
      break;
    case velox::TypeKind::DOUBLE:
      AppendSortableDouble(key, data);
      break;
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY:
      AppendSortableString(key, {rocksdb::Slice{data, size}});
      break;
    default:
      SDB_ASSERT(false, "Unsupported type kind for secondary index: ",
                 velox::TypeKindName::toName(kind));
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

    const auto& col = *_input->childAt(_indexed_col_idx);
    if (col.isNullAt(_row_idx)) {
      secondary_key::AppendNullMarker(_key_buffer);
    } else {
      secondary_key::AppendNotNullMarker(_key_buffer);
      primary_key::AppendKeyValue(_key_buffer, col, _row_idx);
    }

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

// DELETE writer
class SecondarySinkDeleteWriter final : public SecondarySinkWriteBase {
 public:
  SecondarySinkDeleteWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            ObjectId table_id,
                            std::span<const catalog::Column::Id> columns,
                            velox::TypeKind indexed_type_kind,
                            std::string indexed_col_name)
    : SecondarySinkWriteBase{trx, shard_id, columns,
                             std::move(indexed_col_name)},
      _table_id{table_id},
      _indexed_column_id{columns[0]},
      _indexed_type_kind{indexed_type_kind} {}

  void Init(size_t, const velox::RowVectorPtr& input) final { InitBase(input); }

  void Write(std::span<const rocksdb::Slice>, std::string_view full_key) final {
    BuildKeyFromVector(full_key);
    auto s = _trx.Delete(_key_buffer);
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteOldEntry(encoded_pk);
  }

 private:
  void DeleteOldEntry(std::string_view encoded_pk) {
    // Read old indexed column value: <table_id><column_id><pk>
    _read_key.clear();
    basics::StrAppend(_read_key,
                      sizeof(ObjectId) + sizeof(catalog::Column::Id));
    absl::big_endian::Store64(_read_key.data(), _table_id.id());
    key_utils::SetupColumnForKey(_read_key, _indexed_column_id);
    _read_key.append(encoded_pk.data(), encoded_pk.size());

    std::string old_value;
    auto s = _trx.Get(rocksdb::ReadOptions{}, _read_key, &old_value);

    // Build and delete old secondary index key
    _del_key.clear();
    secondary_key::AppendShardPrefix(_del_key, _shard_id);
    if (!s.ok() || old_value.empty()) {
      secondary_key::AppendNullMarker(_del_key);
    } else {
      secondary_key::AppendNotNullMarker(_del_key);
      secondary_key::EncodeValue(_del_key, _indexed_type_kind, old_value.data(),
                                 old_value.size());
    }
    _del_key.append(encoded_pk.data(), encoded_pk.size());
    _trx.Delete(_del_key);
  }

  ObjectId _table_id;
  catalog::Column::Id _indexed_column_id;
  velox::TypeKind _indexed_type_kind;
  std::string _read_key;
  std::string _del_key;
};

// UPDATE writer
class SecondarySinkUpdateWriter final : public SecondarySinkWriteBase {
 public:
  SecondarySinkUpdateWriter(rocksdb::Transaction& trx, ObjectId shard_id,
                            ObjectId table_id,
                            std::span<const catalog::Column::Id> columns,
                            velox::TypeKind indexed_type_kind,
                            std::string indexed_col_name)
    : SecondarySinkWriteBase{trx, shard_id, columns,
                             std::move(indexed_col_name)},
      _table_id{table_id},
      _indexed_column_id{columns[0]},
      _indexed_type_kind{indexed_type_kind} {}

  void Init(size_t, const velox::RowVectorPtr& input) final { InitBase(input); }

  void Write(std::span<const rocksdb::Slice>, std::string_view full_key) final {
    BuildKeyFromVector(full_key);
    auto s = _trx.Put(_key_buffer, rocksdb::Slice{});
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteOldEntry(encoded_pk);
  }

 private:
  void DeleteOldEntry(std::string_view encoded_pk) {
    _read_key.clear();
    basics::StrAppend(_read_key,
                      sizeof(ObjectId) + sizeof(catalog::Column::Id));
    absl::big_endian::Store64(_read_key.data(), _table_id.id());
    key_utils::SetupColumnForKey(_read_key, _indexed_column_id);
    _read_key.append(encoded_pk.data(), encoded_pk.size());

    std::string old_value;
    auto s = _trx.Get(rocksdb::ReadOptions{}, _read_key, &old_value);

    _del_key.clear();
    secondary_key::AppendShardPrefix(_del_key, _shard_id);
    if (!s.ok() || old_value.empty()) {
      secondary_key::AppendNullMarker(_del_key);
    } else {
      secondary_key::AppendNotNullMarker(_del_key);
      secondary_key::EncodeValue(_del_key, _indexed_type_kind, old_value.data(),
                                 old_value.size());
    }
    _del_key.append(encoded_pk.data(), encoded_pk.size());
    _trx.Delete(_del_key);
  }

  ObjectId _table_id;
  catalog::Column::Id _indexed_column_id;
  velox::TypeKind _indexed_type_kind;
  std::string _read_key;
  std::string _del_key;
};

// Secondary index backfill is handled by SSTInsertDataSink<false, true>.

}  // namespace sdb::connector
