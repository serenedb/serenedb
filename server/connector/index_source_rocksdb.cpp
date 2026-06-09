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

#include "connector/index_source_rocksdb.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/types/data_chunk.hpp>
#include <numeric>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {
namespace {

// TODO(mbkkt) benchmark and choose best threshold
constexpr size_t kSeekThreshold = 100;

constexpr size_t kPrefixGap = PrimaryKeysBytes::kPrefixGap;

static_assert(kPrefixGap == sizeof(ObjectId) + sizeof(catalog::Column::Id),
              "PrimaryKeysBytes::kPrefixGap must match the RocksDB key "
              "[ObjectId][ColumnId] prefix size");

// const_cast is safe: the gap is arena-owned writable memory.
inline char* PrefixGapOf(std::string_view pk) {
  return const_cast<char*>(pk.data()) - kPrefixGap;
}

}  // namespace

RocksDBIndexSource::RocksDBIndexSource(
  ObjectId table_id, const rocksdb::Snapshot* snapshot,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  rocksdb::Transaction* txn)
  : _table_id(table_id),
    _read_options{[&] {
      rocksdb::ReadOptions opts;
      opts.async_io = IsIOUringEnabled();
      opts.snapshot = snapshot;
      return opts;
    }()},
    _db(GetServerEngine().db()),
    _cf(RocksDBColumnFamilyManager::get(
      RocksDBColumnFamilyManager::Family::Default)),
    _txn(txn),
    _multiget_ctx(*_cf, _read_options),
    _projected_columns(projected_columns.begin(), projected_columns.end()),
    _projected_types(projected_types.begin(), projected_types.end()),
    _bind_column_ids(bind_column_ids.begin(), bind_column_ids.end()) {
  SDB_ASSERT(_db);
  SDB_ASSERT(_cf);
}

void RocksDBIndexSource::Materialize(duckdb::ClientContext& /*context*/,
                                     PrimaryKeyBatch& batch,
                                     duckdb::idx_t start, duckdb::idx_t count,
                                     duckdb::DataChunk& output) {
  auto& arena = std::get<PrimaryKeysBytes>(batch);
  SDB_ASSERT(start + count <= arena.views.size());
  std::span<const std::string_view> pk_bytes{arena.views.data() + start, count};
  if (pk_bytes.empty()) {
    return;
  }

  _new_batch = true;

  std::string table_key = key_utils::PrepareTableKey(_table_id);
  const auto table_prefix_size = table_key.size();

  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    const auto bind_col = _projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }

    const auto col_id = _bind_column_ids[bind_col];
    const auto& type = _projected_types[proj];

    basics::StrResize(table_key, table_prefix_size);
    key_utils::AppendColumnKey(table_key, col_id);
    SDB_ASSERT(table_key.size() == kPrefixGap);

    auto& vec = output.data[proj];
    DispatchColumnRead(table_key, col_id, pk_bytes,
                       [&](size_t original_idx, std::string_view value) {
                         DeserializeValueIntoDuckDB(
                           value, vec, type,
                           static_cast<duckdb::idx_t>(original_idx));
                       });
  }
}

void RocksDBIndexSource::LookupInto(
  std::span<const std::string_view> pk_bytes,
  std::span<duckdb::Vector* const> per_col_targets) {
  if (pk_bytes.empty()) {
    return;
  }
  _new_batch = true;

  std::string table_key = key_utils::PrepareTableKey(_table_id);
  const auto table_prefix_size = table_key.size();

  duckdb::idx_t real_idx = 0;
  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    const auto bind_col = _projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(real_idx < per_col_targets.size());
    auto* target = per_col_targets[real_idx++];
    SDB_ASSERT(target);

    const auto col_id = _bind_column_ids[bind_col];
    const auto& type = _projected_types[proj];

    basics::StrResize(table_key, table_prefix_size);
    key_utils::AppendColumnKey(table_key, col_id);
    SDB_ASSERT(table_key.size() == kPrefixGap);

    DispatchColumnRead(
      table_key, col_id, pk_bytes,
      [&, target](size_t original_idx, std::string_view value) {
        DeserializeValueIntoDuckDB(value, *target, type,
                                   static_cast<duckdb::idx_t>(original_idx));
      });
  }
}

void RocksDBIndexSource::DispatchColumnRead(
  std::string_view column_key_prefix, catalog::Column::Id column_id,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  // TODO(Dronplane) Maybe multiply by pk count by columns count?
  if (pk_bytes.size() > kSeekThreshold) {
    SeekIterateColumnKeys(column_key_prefix, column_id, pk_bytes, decode);
  } else if (pk_bytes.size() > MultiGetContext::kMultiGetThreshold) {
    MultiGetIterateColumnKeys(column_key_prefix, pk_bytes, decode);
  } else {
    IterateColumnKeys(column_key_prefix, pk_bytes, decode);
  }
}

void RocksDBIndexSource::IterateColumnKeys(
  std::string_view column_key_prefix,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  for (size_t idx = 0; idx < pk_bytes.size(); ++idx) {
    char* gap = PrefixGapOf(pk_bytes[idx]);
    std::memcpy(gap, column_key_prefix.data(), kPrefixGap);
    rocksdb::Slice key{gap, kPrefixGap + pk_bytes[idx].size()};

    _value_buffer.clear();
    auto s = _txn ? _txn->Get(_read_options, _cf, key, &_value_buffer)
                  : _db->Get(_read_options, _cf, key, &_value_buffer);
    if (s.IsNotFound()) {
      SDB_THROW(ERROR_INTERNAL, "Missing row for PK in RocksDB");
    }
    SDB_ASSERT(s.ok(), "RocksDB Get failed: ", s.ToString());
    decode(idx, _value_buffer);
  }
}

void RocksDBIndexSource::EnsureSortedBatch(
  std::span<const std::string_view> pk_bytes) {
  if (!_new_batch) {
    return;
  }
  _read_idxs.resize(pk_bytes.size());
  std::iota(_read_idxs.begin(), _read_idxs.end(), size_t{0});
  std::sort(_read_idxs.begin(), _read_idxs.end(), [&](size_t lhs, size_t rhs) {
    return pk_bytes[lhs] < pk_bytes[rhs];
  });
  _new_batch = false;
}

void RocksDBIndexSource::MultiGetIterateColumnKeys(
  std::string_view column_key_prefix,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  EnsureSortedBatch(pk_bytes);
  SDB_ASSERT(column_key_prefix.size() == kPrefixGap);
  _key_slices.resize(pk_bytes.size());

  // Sorted slice array -- MultiGet wants keys sorted for sequential block
  // reads.
  for (size_t i = 0; i < _read_idxs.size(); ++i) {
    const auto idx = _read_idxs[i];
    char* gap = PrefixGapOf(pk_bytes[idx]);
    std::memcpy(gap, column_key_prefix.data(), kPrefixGap);
    _key_slices[i] = rocksdb::Slice{gap, kPrefixGap + pk_bytes[idx].size()};
  }

  size_t sorted_pos = 0;
  auto callback = [&](rocksdb::Slice, const rocksdb::PinnableSlice& value,
                      rocksdb::Status status) {
    if (status.IsNotFound()) {
      SDB_THROW(ERROR_INTERNAL, "Missing row for PK in RocksDB");
    }
    SDB_ASSERT(status.ok(), "RocksDB MultiGet failed: ", status.ToString());
    decode(_read_idxs[sorted_pos++], value.ToStringView());
  };
  if (_txn) {
    _multiget_ctx.MultiGet(*_txn, _key_slices, callback);
  } else {
    _multiget_ctx.MultiGet(*_db, _key_slices, callback);
  }
}

void RocksDBIndexSource::SeekIterateColumnKeys(
  std::string_view column_key_prefix, catalog::Column::Id column_id,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  EnsureSortedBatch(pk_bytes);
  SDB_ASSERT(column_key_prefix.size() == kPrefixGap);

  rocksdb::Iterator* column_iterator = nullptr;
  auto it = _iterators.find(column_id);
  if (it == _iterators.end()) {
    // async_io off: cached iterators may be resumed on a different thread,
    // and an outstanding async op would hang.
    auto it_options = _read_options;
    it_options.async_io = false;
    auto iter = std::unique_ptr<rocksdb::Iterator>(
      _txn ? _txn->GetIterator(it_options, _cf)
           : _db->NewIterator(it_options, _cf));
    column_iterator =
      _iterators.emplace(column_id, std::move(iter)).first->second.get();
  } else {
    column_iterator = it->second.get();
  }
  SDB_ASSERT(column_iterator);

  for (auto idx : _read_idxs) {
    char* gap = PrefixGapOf(pk_bytes[idx]);
    std::memcpy(gap, column_key_prefix.data(), kPrefixGap);
    const rocksdb::Slice key{gap, kPrefixGap + pk_bytes[idx].size()};
    column_iterator->Seek(key);
    if (!column_iterator->Valid() || column_iterator->key() != key) {
      SDB_THROW(ERROR_INTERNAL, "Missing row for PK in RocksDB (Seek)");
    }
    rocksutils::CheckIteratorStatus(*column_iterator);
    decode(idx, column_iterator->value().ToStringView());
  }
}

}  // namespace sdb::connector
