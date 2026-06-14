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

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>

#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/key_utils.hpp"
#include "connector/secondary_sink_writer.hpp"  // for secondary_key:: helpers
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

// DuckDB-native secondary key helpers (parallel to secondary_key:: for Velox).
namespace duckdb_secondary_key {

// Column mapping: which DataChunk column index + DuckDB type for each SK column
struct SKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
};

// Pre-build UnifiedVectorFormat for each SK column. Call once per chunk in
// the writer's Init; AppendSKValue then resolves rows zero-copy.
inline void PrepareSKFormats(
  const duckdb::DataChunk& input, std::span<const SKColumn> sk_columns,
  std::vector<duckdb::UnifiedVectorFormat>& sk_formats) {
  const auto num_rows = input.size();
  sk_formats.resize(sk_columns.size());
  for (size_t c = 0; c < sk_columns.size(); ++c) {
    input.data[sk_columns[c].input_col_idx].ToUnifiedFormat(num_rows,
                                                            sk_formats[c]);
  }
}

// Appends <marker><encoded_value> for each SK column. Reads via the
// pre-built UnifiedVectorFormat to avoid per-row Value allocation.
// Returns true if any SK column is NULL.
inline bool AppendSKValue(
  std::string& key, std::span<const duckdb::UnifiedVectorFormat> sk_formats,
  std::span<const SKColumn> sk_columns, duckdb::idx_t row_idx) {
  SDB_ASSERT(sk_formats.size() == sk_columns.size());
  bool has_null = false;
  for (size_t c = 0; c < sk_columns.size(); ++c) {
    const auto& fmt = sk_formats[c];
    const auto idx = fmt.sel->get_index(row_idx);
    if (!fmt.validity.RowIsValid(idx)) {
      secondary_key::AppendNullMarker(key);
      has_null = true;
    } else {
      secondary_key::AppendNotNullMarker(key);
      AppendPKValue(key, fmt, row_idx, sk_columns[c].type);
    }
  }
  return has_null;
}

}  // namespace duckdb_secondary_key

template<bool Unique>
class DuckDBSecondarySinkWriteBase : public DuckDBSinkIndexWriter,
                                     public DuckDBColumnSinkWriterImplBase {
 public:
  DuckDBSecondarySinkWriteBase(
    rocksdb::Transaction& trx, ObjectId shard_id,
    std::span<const catalog::Column::Id> columns,
    std::vector<duckdb_secondary_key::SKColumn> sk_columns)
    : DuckDBColumnSinkWriterImplBase{columns},
      _trx{trx},
      _shard_id{shard_id},
      _sk_columns{std::move(sk_columns)},
      _trigger_column_id{columns[0]} {}

  bool SwitchColumn(const ColumnDescriptor& col, const duckdb::Vector& /*vec*/,
                    duckdb::idx_t /*count*/) final {
    return col.id == _trigger_column_id;
  }

  void Finish() final {}
  void Abort() final {}

 protected:
  void InitBase(const duckdb::DataChunk& input) {
    _row_idx = 0;
    _del_row_idx = 0;
    duckdb_secondary_key::PrepareSKFormats(input, _sk_columns, _sk_formats);
  }

  bool BuildSK(std::string_view full_pk, rocksdb::Slice& value) {
    auto pk_bytes = key_utils::ExtractRowKey(full_pk);
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendDummyColumnId(_key_buffer);
    bool has_null = duckdb_secondary_key::AppendSKValue(
      _key_buffer, _sk_formats, _sk_columns, _row_idx);
    constexpr bool kAlwaysPKInKey = !Unique;
    bool pk_in_key = kAlwaysPKInKey || has_null;
    _value_buffer.clear();
    _value_buffer.push_back(pk_in_key ? secondary_key::kPKInKey
                                      : secondary_key::kPKInValue);
    if (pk_in_key) {
      _value_buffer.push_back(static_cast<char>(pk_bytes.size()));
      _key_buffer.append(pk_bytes);
    } else {
      _value_buffer.append(pk_bytes);
    }
    value = _value_buffer;
    ++_row_idx;
    return has_null;
  }

  // Shared by the insert and update writers: build the SK entry for the current
  // row and Put it, enforcing uniqueness first for a unique index. Non-virtual
  // so each writer's final Write() inlines it -- no per-row dispatch cost.
  void WriteSK(std::string_view full_key) {
    rocksdb::Slice value;
    bool has_null = BuildSK(full_key, value);
    if constexpr (Unique) {
      if (!has_null) {
        rocksdb::PinnableSlice existing;
        auto gs = _trx.GetForUpdate(rocksdb::ReadOptions{}, _key_buffer,
                                    &existing);
        if (gs.ok()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
            ERR_MSG("duplicate key value violates unique constraint on "
                    "secondary index"));
        }
      }
    }
    auto s = _trx.Put(_key_buffer, value);
    SDB_ASSERT(s.ok(), "Secondary index Put failed: ", s.ToString());
  }

  // For DELETE / UPDATE-OLD lookups using a (possibly different) sk_columns
  // mapping: caller passes the matching pre-built formats span, so we keep
  // `sk_columns` and `sk_formats` paired.
  std::string_view BuildDeleteSK(
    std::string_view encoded_pk,
    std::span<const duckdb_secondary_key::SKColumn> sk_columns,
    std::span<const duckdb::UnifiedVectorFormat> sk_formats) {
    _key_buffer.clear();
    secondary_key::AppendShardPrefix(_key_buffer, _shard_id);
    secondary_key::AppendDummyColumnId(_key_buffer);
    bool has_null = duckdb_secondary_key::AppendSKValue(
      _key_buffer, sk_formats, sk_columns, _del_row_idx);
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
  std::vector<duckdb_secondary_key::SKColumn> _sk_columns;
  catalog::Column::Id _trigger_column_id;
  std::vector<duckdb::UnifiedVectorFormat> _sk_formats;
  duckdb::idx_t _row_idx = 0;
  duckdb::idx_t _del_row_idx = 0;
  std::string _key_buffer;
  std::string _value_buffer;
};

template<bool Unique>
class DuckDBSecondarySinkInsertWriter final
  : public DuckDBSecondarySinkWriteBase<Unique> {
  using Base = DuckDBSecondarySinkWriteBase<Unique>;

 public:
  using Base::Base;

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk& input) final {
    Base::InitBase(input);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_pk) final {
    this->WriteSK(full_pk);
  }
};

template<bool Unique>
class DuckDBSecondarySinkDeleteWriter final
  : public DuckDBSecondarySinkWriteBase<Unique> {
  using Base = DuckDBSecondarySinkWriteBase<Unique>;

 public:
  using Base::Base;

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk& input) final {
    Base::InitBase(input);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    auto s = this->_trx.Delete(
      this->BuildDeleteSK(encoded_pk, this->_sk_columns, this->_sk_formats));
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }
};

template<bool Unique>
class DuckDBSecondarySinkUpdateWriter final
  : public DuckDBSecondarySinkWriteBase<Unique> {
  using Base = DuckDBSecondarySinkWriteBase<Unique>;

 public:
  DuckDBSecondarySinkUpdateWriter(
    rocksdb::Transaction& trx, ObjectId shard_id,
    std::span<const catalog::Column::Id> columns,
    std::vector<duckdb_secondary_key::SKColumn> sk_columns,
    std::vector<duckdb_secondary_key::SKColumn> old_sk_columns)
    : Base{trx, shard_id, columns, std::move(sk_columns)},
      _old_sk_columns{std::move(old_sk_columns)} {
    // The default trigger is columns[0] (first SK column), but that column may
    // not be in the SET clause. SwitchColumn() won't fire for it, so Write()
    // and the GetForUpdate uniqueness check would be silently skipped.
    // Find the first SK column whose chunk position differs between ins and del
    // mappings -- that column is actually being SET and will appear in step 2.
    for (size_t i = 0; i < this->_sk_columns.size(); ++i) {
      if (this->_sk_columns[i].input_col_idx !=
          _old_sk_columns[i].input_col_idx) {
        this->_trigger_column_id = columns[i];
        break;
      }
    }
  }

  void Init(duckdb::idx_t batch_size, const duckdb::DataChunk& input) final {
    Base::InitBase(input);
    duckdb_secondary_key::PrepareSKFormats(input, _old_sk_columns,
                                           _old_sk_formats);
  }

  void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) final {
    this->WriteSK(full_key);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    auto s = this->_trx.Delete(
      this->BuildDeleteSK(encoded_pk, _old_sk_columns, _old_sk_formats));
    SDB_ASSERT(s.ok(), "Secondary index Delete failed: ", s.ToString());
  }

 private:
  std::vector<duckdb_secondary_key::SKColumn> _old_sk_columns;
  std::vector<duckdb::UnifiedVectorFormat> _old_sk_formats;
};

}  // namespace sdb::connector
