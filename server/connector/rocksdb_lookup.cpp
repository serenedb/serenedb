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

#include "connector/rocksdb_lookup.h"

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
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

// Batch-size threshold for picking the Seek strategy over MultiGet.
// Ported from origin/main rocksdb_materializer.cpp.
//
// TODO(mbkkt) benchmark and choose best threshold
constexpr size_t kSeekThreshold = 100;

// `[ObjectId][Column::Id]` prefix attached to every row key.
constexpr size_t kColumnKeySize =
  sizeof(ObjectId) + sizeof(catalog::Column::Id);

}  // namespace

RocksDBLookup::RocksDBLookup(
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

void RocksDBLookup::Lookup(std::span<const std::string_view> pk_bytes,
                           duckdb::DataChunk& output) {
  const auto num_rows = pk_bytes.size();
  if (num_rows == 0) {
    return;
  }

  // Mark each new call as a fresh batch; the sort + arena staging is done
  // lazily on first strategy that needs it.
  _new_batch = true;

  std::string table_key = key_utils::PrepareTableKey(_table_id);
  const auto table_prefix_size = table_key.size();

  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    const auto bind_col = _projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      // Virtual slot (rowid / tableoid / score / offsets) -- caller fills.
      continue;
    }

    const auto col_id = _bind_column_ids[bind_col];
    const auto& type = _projected_types[proj];

    // Build the column-key prefix: [ObjectId][ColumnId].
    basics::StrResize(table_key, table_prefix_size);
    key_utils::AppendColumnKey(table_key, col_id);
    SDB_ASSERT(table_key.size() == kColumnKeySize);

    auto& vec = output.data[proj];
    DispatchColumnRead(table_key, col_id, pk_bytes,
                       [&](size_t original_idx, std::string_view value) {
                         DeserializeValueIntoDuckDB(
                           value, vec, type,
                           static_cast<duckdb::idx_t>(original_idx));
                       });
  }
}

void RocksDBLookup::DispatchColumnRead(
  std::string_view column_key_prefix, catalog::Column::Id column_id,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  if (pk_bytes.size() > kSeekThreshold) {
    SeekIterateColumnKeys(column_key_prefix, column_id, pk_bytes, decode);
  } else if (pk_bytes.size() > MultiGetContext::kMultiGetThreshold) {
    MultiGetIterateColumnKeys(column_key_prefix, pk_bytes, decode);
  } else {
    IterateColumnKeys(column_key_prefix, pk_bytes, decode);
  }
}

void RocksDBLookup::IterateColumnKeys(
  std::string_view column_key_prefix,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  std::string buffer;
  buffer.reserve(column_key_prefix.size() + 64);
  for (size_t idx = 0; idx < pk_bytes.size(); ++idx) {
    buffer.assign(column_key_prefix);
    buffer.append(pk_bytes[idx].data(), pk_bytes[idx].size());
    _value_buffer.clear();
    auto s = _txn ? _txn->Get(_read_options, _cf, buffer, &_value_buffer)
                  : _db->Get(_read_options, _cf, buffer, &_value_buffer);
    if (s.IsNotFound()) {
      // Missing rows: mirror origin/main behaviour of raising; callers
      // never expect sparse index state in a consistent snapshot.
      SDB_THROW(ERROR_INTERNAL, "Missing row for PK in RocksDB");
    }
    SDB_ASSERT(s.ok(), "RocksDB Get failed: ", s.ToString());
    decode(idx, _value_buffer);
  }
}

void RocksDBLookup::PrepareSortedBatch(
  std::span<const std::string_view> pk_bytes) {
  if (!_new_batch) {
    return;
  }
  _read_idxs.resize(pk_bytes.size());
  std::iota(_read_idxs.begin(), _read_idxs.end(), size_t{0});
  std::sort(_read_idxs.begin(), _read_idxs.end(), [&](size_t lhs, size_t rhs) {
    return pk_bytes[lhs] < pk_bytes[rhs];
  });

  // Layout: per sorted pk we reserve `kColumnKeySize + pk.size()` bytes.
  // The column-key prefix is written at read-time (it may vary across
  // columns), so only the pk portion is staged here.
  size_t required_size = kColumnKeySize * pk_bytes.size();
  for (auto& pk : pk_bytes) {
    required_size += pk.size();
  }
  _multi_get_buffer.resize(required_size);
  char* data = _multi_get_buffer.data();

  size_t offset = kColumnKeySize;  // first pk goes right after first prefix
  for (auto idx : _read_idxs) {
    std::memcpy(data + offset, pk_bytes[idx].data(), pk_bytes[idx].size());
    offset += pk_bytes[idx].size() + kColumnKeySize;
  }
  _new_batch = false;
}

void RocksDBLookup::MultiGetIterateColumnKeys(
  std::string_view column_key_prefix,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  PrepareSortedBatch(pk_bytes);
  _key_slices.resize(pk_bytes.size());
  SDB_ASSERT(column_key_prefix.size() == kColumnKeySize);
  char* data = _multi_get_buffer.data();

  // Re-stamp the column-key prefix before every full key in the arena.
  size_t offset = 0;
  for (size_t i = 0; i < _read_idxs.size(); ++i) {
    std::memcpy(data + offset, column_key_prefix.data(), kColumnKeySize);
    const auto full_key_size = kColumnKeySize + pk_bytes[_read_idxs[i]].size();
    _key_slices[i] = {data + offset, full_key_size};
    offset += full_key_size;
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

void RocksDBLookup::SeekIterateColumnKeys(
  std::string_view column_key_prefix, catalog::Column::Id column_id,
  std::span<const std::string_view> pk_bytes, const DecodeFn& decode) {
  PrepareSortedBatch(pk_bytes);

  rocksdb::Iterator* column_iterator = nullptr;
  auto it = _iterators.find(column_id);
  if (it == _iterators.end()) {
    // Iterators are kept across batches, so disable async_io -- a batch
    // could be resumed on a different thread and an outstanding async
    // op would hang. (Same reasoning as in origin/main.)
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

  char* data = _multi_get_buffer.data();
  size_t offset = 0;
  for (auto idx : _read_idxs) {
    const auto pk_size = pk_bytes[idx].size();
    std::memcpy(data + offset, column_key_prefix.data(), kColumnKeySize);
    const rocksdb::Slice key{data + offset, kColumnKeySize + pk_size};
    column_iterator->Seek(key);
    if (!column_iterator->Valid() || column_iterator->key() != key) {
      SDB_THROW(ERROR_INTERNAL, "Missing row for PK in RocksDB (Seek)");
    }
    rocksutils::CheckIteratorStatus(*column_iterator);
    decode(idx, column_iterator->value().ToStringView());
    offset += kColumnKeySize + pk_size;
  }
}

}  // namespace sdb::connector
