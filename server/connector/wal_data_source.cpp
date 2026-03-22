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

#include "wal_data_source.hpp"

#include <absl/base/internal/endian.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_common.h"

namespace sdb::connector {

namespace {

constexpr size_t kTablePrefixSize = sizeof(ObjectId);
constexpr size_t kKeyPrefixSize =
  kTablePrefixSize + sizeof(catalog::Column::Id);

template<typename T>
void SetValue(std::string_view value, size_t idx,
              velox::FlatVector<T>& result) {
  if constexpr (std::is_same_v<T, velox::StringView>) {
    const size_t offset = value[0] == 0 ? 1 : 0;
    result.set(idx,
               velox::StringView(value.data() + offset, value.size() - offset));
  } else if constexpr (std::is_same_v<T, bool>) {
    SDB_ASSERT(value.size() == kTrueValue.size(),
               "WALDataSource: unexpected value size for bool column");
    result.set(idx, value == kTrueValue);
  } else {
    SDB_ASSERT(value.size() == sizeof(T),
               "WALDataSource: unexpected value size for scalar column");
    T tmp;
    memcpy(&tmp, value.data(), sizeof(T));
    result.set(idx, tmp);
  }
}

template<velox::TypeKind Kind>
velox::VectorPtr BuildColumnVector(
  const std::vector<std::pair<const std::string*, const RowAccumulator*>>&
    live_rows,
  size_t col_idx, velox::memory::MemoryPool& pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;

  const auto num_rows = static_cast<velox::vector_size_t>(live_rows.size());
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), num_rows, &pool);

  for (velox::vector_size_t row = 0; row < num_rows; ++row) {
    const auto& acc = *live_rows[row].second;
    if (col_idx < acc.column_present.size() && acc.column_present[col_idx] &&
        !acc.column_values[col_idx].empty()) {
      SetValue(std::string_view{acc.column_values[col_idx]}, row, *result);
    } else {
      result->setNull(row, true);
    }
  }

  return result;
}

}  // namespace

WALDataSource::WALDataSource(velox::memory::MemoryPool& memory_pool,
                             rocksdb::TransactionDB& db,
                             rocksdb::ColumnFamilyHandle& cf,
                             velox::RowTypePtr read_type, ObjectId object_key,
                             std::vector<catalog::Column::Id> column_ids,
                             WALRecoveryRange range)
  : _memory_pool{memory_pool},
    _db{db},
    _cf{cf},
    _read_type{std::move(read_type)},
    _object_key{object_key},
    _column_ids{std::move(column_ids)},
    _range{range},
    _default_cf_id{cf.GetID()} {
  SDB_ASSERT(_read_type);
  SDB_ASSERT(_object_key.isSet());
  SDB_ASSERT(!_column_ids.empty());
}

void WALDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "WALDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "WALDataSource: a split is already being processed");
  }
  _current_split = std::move(split);

  if (_range.IsEmpty()) {
    _finished = true;
    return;
  }

  // Initialize WAL iterator
  std::unique_ptr<rocksdb::TransactionLogIterator> iterator;
  auto s = _db.GetUpdatesSince(
    _range.start_sequence, &iterator,
    rocksdb::TransactionLogIterator::ReadOptions(true));

  if (!s.ok()) {
    SDB_THROW(ERROR_INTERNAL, "WALDataSource: failed to open WAL iterator: ",
              s.ToString());
  }

  _wal_iterator = std::move(iterator);
}

std::optional<velox::RowVectorPtr> WALDataSource::next(
  uint64_t size, velox::ContinueFuture& /*future*/) {
  if (_finished) {
    _current_split.reset();
    return std::nullopt;
  }

  // Drain WAL until we have enough rows or exhausted
  DrainWAL(size);

  auto batch = FlushAccumulator(size);
  if (!batch || batch->size() == 0) {
    _finished = true;
    _current_split.reset();
    return std::nullopt;
  }

  _produced += batch->size();
  return batch;
}

bool WALDataSource::IsOurKey(uint32_t column_family_id,
                             const rocksdb::Slice& key) const {
  if (column_family_id != _default_cf_id) {
    return false;
  }
  if (key.size() < kKeyPrefixSize) {
    return false;
  }
  ObjectId key_object_id;
  memcpy(&key_object_id, key.data(), sizeof(ObjectId));
  return key_object_id == _object_key;
}

catalog::Column::Id WALDataSource::ExtractColumnId(
  const rocksdb::Slice& key) const {
  SDB_ASSERT(key.size() >= kKeyPrefixSize);
  return absl::big_endian::Load<catalog::Column::Id>(key.data() +
                                                     kTablePrefixSize);
}

std::string WALDataSource::ExtractPK(const rocksdb::Slice& key) const {
  SDB_ASSERT(key.size() > kKeyPrefixSize);
  return std::string(key.data() + kKeyPrefixSize,
                     key.size() - kKeyPrefixSize);
}

std::optional<size_t> WALDataSource::FindColumnIndex(
  catalog::Column::Id col_id) const {
  for (size_t i = 0; i < _column_ids.size(); ++i) {
    if (_column_ids[i] == col_id) {
      return i;
    }
  }
  return std::nullopt;
}

void WALDataSource::HandlePut(const rocksdb::Slice& key,
                              const rocksdb::Slice& value) {
  auto col_id = ExtractColumnId(key);
  auto col_idx = FindColumnIndex(col_id);
  if (!col_idx) {
    // Column not in our read set, skip
    return;
  }

  auto pk = ExtractPK(key);
  auto& acc = _pending_rows[pk];

  const size_t num_cols = _column_ids.size();
  if (acc.column_values.size() < num_cols) {
    acc.column_values.resize(num_cols);
    acc.column_present.resize(num_cols, false);
  }

  acc.column_values[*col_idx] = value.ToString();
  acc.column_present[*col_idx] = true;
  // A Put after a Delete means the row was re-inserted/updated
  acc.deleted = false;
}

void WALDataSource::HandleDelete(const rocksdb::Slice& key) {
  auto col_id = ExtractColumnId(key);
  if (!FindColumnIndex(col_id)) {
    return;
  }

  auto pk = ExtractPK(key);
  auto& acc = _pending_rows[pk];
  acc.deleted = true;
}

velox::RowVectorPtr WALDataSource::FlushAccumulator(uint64_t max_rows) {
  // Collect live (non-deleted) rows
  std::vector<std::pair<const std::string*, const RowAccumulator*>> live_rows;
  live_rows.reserve(
    std::min(static_cast<size_t>(max_rows), _pending_rows.size()));

  auto it = _pending_rows.begin();
  while (it != _pending_rows.end() && live_rows.size() < max_rows) {
    if (!it->second.deleted) {
      live_rows.emplace_back(&it->first, &it->second);
    }
    ++it;
  }

  // Erase processed entries (both live and deleted)
  _pending_rows.erase(_pending_rows.begin(), it);

  if (live_rows.empty()) {
    // If WAL is exhausted and no pending rows remain, we're done
    if (_wal_exhausted && _pending_rows.empty()) {
      return nullptr;
    }
    // Return empty batch - will try again on next call
    return nullptr;
  }

  const auto num_rows = static_cast<velox::vector_size_t>(live_rows.size());
  const auto num_cols = _read_type->size();

  std::vector<velox::VectorPtr> columns;
  columns.reserve(num_cols);

  for (size_t col = 0; col < num_cols; ++col) {
    auto kind = _read_type->childAt(col)->kind();
    if (kind == velox::TypeKind::UNKNOWN) {
      columns.push_back(
        velox::BaseVector::createNullConstant(velox::UNKNOWN(), num_rows,
                                              &_memory_pool));
      continue;
    }
    columns.push_back(
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(BuildColumnVector, kind, live_rows,
                                         col, _memory_pool));
  }

  return std::make_shared<velox::RowVector>(&_memory_pool, _read_type,
                                            velox::BufferPtr{}, num_rows,
                                            std::move(columns));
}

bool WALDataSource::DrainWAL(uint64_t target_rows) {
  if (_wal_exhausted || !_wal_iterator) {
    return false;
  }

  WALBatchHandler handler(*this);

  // Count non-deleted rows in accumulator
  auto countLiveRows = [this]() -> size_t {
    size_t count = 0;
    for (const auto& [pk, acc] : _pending_rows) {
      if (!acc.deleted) {
        ++count;
      }
    }
    return count;
  };

  while (_wal_iterator->Valid() && countLiveRows() < target_rows) {
    auto s = _wal_iterator->status();
    if (!s.ok()) {
      SDB_THROW(ERROR_INTERNAL, "WALDataSource: WAL iterator error: ",
                s.ToString());
    }

    auto batch = _wal_iterator->GetBatch();

    // Stop if we've gone past our end sequence
    if (batch.sequence > _range.end_sequence) {
      _wal_exhausted = true;
      return true;
    }

    s = batch.writeBatchPtr->Iterate(&handler);
    if (!s.ok()) {
      SDB_THROW(ERROR_INTERNAL, "WALDataSource: failed to iterate batch: ",
                s.ToString());
    }

    _wal_iterator->Next();
  }

  if (!_wal_iterator->Valid()) {
    _wal_exhausted = true;
  }

  return true;
}

// WALBatchHandler implementation

rocksdb::Status WALDataSource::WALBatchHandler::PutCF(
  uint32_t column_family_id, const rocksdb::Slice& key,
  const rocksdb::Slice& value) {
  if (_source.IsOurKey(column_family_id, key)) {
    _source.HandlePut(key, value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WALDataSource::WALBatchHandler::DeleteCF(
  uint32_t column_family_id, const rocksdb::Slice& key) {
  if (_source.IsOurKey(column_family_id, key)) {
    _source.HandleDelete(key);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WALDataSource::WALBatchHandler::SingleDeleteCF(
  uint32_t column_family_id, const rocksdb::Slice& key) {
  if (_source.IsOurKey(column_family_id, key)) {
    _source.HandleDelete(key);
  }
  return rocksdb::Status::OK();
}

}  // namespace sdb::connector
