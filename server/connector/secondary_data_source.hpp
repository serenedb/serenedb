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

#include <velox/connectors/Connector.h>

#include "basics/fwd.h"
#include "connector/primary_key.hpp"
#include "connector/secondary_sink_writer.hpp"

namespace sdb::connector {

// Data source that reads from a secondary index via RocksDB iterator scan,
// extracts PKs, then materializes rows via the Materializer.
//
// Unique=false (non-unique index):
//   Key: <shard (8B)> <SK values> <PK bytes>   Value: empty
//   PK extracted from key suffix.
//
// Unique=true (unique index, range/full scan):
//   Key: <shard (8B)> <SK values>               Value: <PK bytes>
//   PK extracted from value.
//
// For unique point lookups (eq/IN), use UniqueSecondaryIndexPointDataSource
// which uses MultiGet instead of iteration.
template<typename Materializer, bool Unique>
class SecondaryIndexDataSource final : public velox::connector::DataSource {
 public:
  SecondaryIndexDataSource(velox::memory::MemoryPool& memory_pool,
                           Materializer materializer, rocksdb::DB& db,
                           rocksdb::ColumnFamilyHandle& cf,
                           const rocksdb::Snapshot* snapshot, ObjectId shard_id,
                           std::vector<velox::variant> values,
                           velox::TypePtr value_type)
    : _memory_pool{memory_pool},
      _materializer{std::move(materializer)},
      _db{db},
      _cf{cf},
      _snapshot{snapshot},
      _shard_id{shard_id},
      _values{std::move(values)},
      _row_type{velox::ROW({"v"}, {std::move(value_type)})} {}

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
    SDB_ENSURE(split, ERROR_INTERNAL,
               "SecondaryIndexDataSource: split is null");
    if (_current_split) {
      SDB_THROW(ERROR_INTERNAL,
                "SecondaryIndexDataSource: split already being processed");
    }
    _current_split = std::move(split);
    _iterator.reset();
    _current_value_idx = 0;
  }

  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& /*future*/) final {
    SDB_ASSERT(size);
    SDB_ASSERT(_current_split);

    primary_key::Keys row_keys{_memory_pool};
    row_keys.reserve(size);

    while (row_keys.size() < size && _current_value_idx < _values.size()) {
      if (!_iterator) {
        _current_scan_prefix = BuildScanPrefix(_values[_current_value_idx]);
        _value_key_size = _current_scan_prefix.size() - sizeof(ObjectId);
        _current_value_is_null = _values[_current_value_idx].isNull();

        rocksdb::ReadOptions ro;
        ro.snapshot = _snapshot;
        ro.total_order_seek = true;
        _upper_bound = _current_scan_prefix;
        IncrementPrefix(_upper_bound);
        _upper_bound_slice = rocksdb::Slice{_upper_bound};
        ro.iterate_upper_bound = &_upper_bound_slice;
        _iterator.reset(_db.NewIterator(ro, &_cf));
        _iterator->Seek(_current_scan_prefix);
      }

      while (row_keys.size() < size && _iterator->Valid()) {
        auto key = _iterator->key();
        std::string_view key_view{key.data(), key.size()};

        if (!key_view.starts_with(_current_scan_prefix)) {
          break;
        }

        if constexpr (Unique) {
          if (_current_value_is_null) {
            // NULL entries: PK in key suffix (like non-unique)
            auto pk_start = sizeof(ObjectId) + _value_key_size;
            if (key_view.size() > pk_start) {
              row_keys.emplace_back(key_view.substr(pk_start));
            }
          } else {
            // Non-NULL: PK stored as value
            auto val = _iterator->value();
            row_keys.emplace_back(val.data(), val.size());
          }
        } else {
          // PK stored in key suffix after [shard][SK values]
          auto pk_start = sizeof(ObjectId) + _value_key_size;
          if (key_view.size() > pk_start) {
            row_keys.emplace_back(key_view.substr(pk_start));
          }
        }

        _iterator->Next();
      }

      // If iterator exhausted for this value, move to next
      if (!_iterator->Valid() ||
          !std::string_view{_iterator->key().data(), _iterator->key().size()}
             .starts_with(_current_scan_prefix)) {
        _iterator.reset();
        ++_current_value_idx;
      }
    }

    if (row_keys.empty()) {
      _current_split.reset();
      _iterator.reset();
      return nullptr;
    }

    _produced += row_keys.size();
    return _materializer.ReadRows(row_keys, nullptr);
  }

  void addDynamicFilter(velox::column_index_t,
                        const std::shared_ptr<velox::common::Filter>&) final {
    VELOX_UNSUPPORTED();
  }

  uint64_t getCompletedBytes() final { return 0; }
  uint64_t getCompletedRows() final { return _produced; }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
    final {
    return {};
  }

  void cancel() final { _iterator.reset(); }

 private:
  std::string BuildScanPrefix(const velox::variant& value) const {
    std::string prefix;
    secondary_key::AppendShardPrefix(prefix, _shard_id);
    if (value.isNull()) {
      secondary_key::AppendNullMarker(prefix);
    } else {
      secondary_key::AppendNotNullMarker(prefix);
      std::array<velox::variant, 1> point{value};
      primary_key::Create(point, *_row_type, prefix);
    }
    return prefix;
  }

  static void IncrementPrefix(std::string& prefix) {
    for (auto it = prefix.rbegin(); it != prefix.rend(); ++it) {
      auto& c = *it;
      if (static_cast<unsigned char>(c) < 0xFF) {
        ++c;
        return;
      }
      c = 0;
    }
    prefix.push_back('\x00');
  }

  velox::memory::MemoryPool& _memory_pool;
  Materializer _materializer;
  rocksdb::DB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
  const rocksdb::Snapshot* _snapshot;
  ObjectId _shard_id;
  std::vector<velox::variant> _values;
  velox::RowTypePtr _row_type;
  std::string _current_scan_prefix;
  size_t _value_key_size = 0;
  std::string _upper_bound;
  rocksdb::Slice _upper_bound_slice;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  std::unique_ptr<rocksdb::Iterator> _iterator;
  size_t _current_value_idx = 0;
  bool _current_value_is_null = false;
  uint64_t _produced = 0;
};

// Data source for UNIQUE secondary index point lookups (eq/IN predicates).
// Uses MultiGet on index CF to map SK → PK, then materializes via Materializer.
//
// Index key: <shard (8B)> <SK values>   Value: <PK bytes>
template<typename Materializer>
class UniqueSecondaryIndexPointDataSource final
  : public velox::connector::DataSource {
 public:
  UniqueSecondaryIndexPointDataSource(
    velox::memory::MemoryPool& memory_pool, Materializer materializer,
    rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
    const rocksdb::Snapshot* snapshot, ObjectId shard_id,
    std::vector<velox::variant> values, velox::TypePtr value_type)
    : _memory_pool{memory_pool},
      _materializer{std::move(materializer)},
      _db{db},
      _cf{cf},
      _snapshot{snapshot},
      _shard_id{shard_id},
      _values{std::move(values)},
      _row_type{velox::ROW({"v"}, {std::move(value_type)})} {}

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
    SDB_ENSURE(split, ERROR_INTERNAL,
               "UniqueSecondaryIndexPointDataSource: split is null");
    if (_current_split) {
      SDB_THROW(
        ERROR_INTERNAL,
        "UniqueSecondaryIndexPointDataSource: split already being processed");
    }
    _current_split = std::move(split);
    _current_value_idx = 0;
  }

  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& /*future*/) final {
    SDB_ASSERT(size);
    if (!_current_split) {
      return nullptr;
    }

    size_t count =
      std::min(static_cast<size_t>(size), _values.size() - _current_value_idx);
    if (count == 0) {
      _current_split.reset();
      return nullptr;
    }

    // Build index keys: [shard][null_marker][encoded_SK_value]
    _index_keys.resize(count);
    _key_slices.resize(count);
    for (size_t i = 0; i < count; ++i) {
      _index_keys[i] = BuildIndexKey(_values[_current_value_idx + i]);
      _key_slices[i] = _index_keys[i];
    }

    // MultiGet on index CF
    _pinnable.resize(count);
    _statuses.resize(count);
    rocksdb::ReadOptions ro;
    ro.snapshot = _snapshot;
    for (auto& p : _pinnable) {
      p.Reset();
    }
    _db.MultiGet(ro, &_cf, count, _key_slices.data(), _pinnable.data(),
                 _statuses.data(), /*sorted_input=*/false);

    // Extract found PKs
    primary_key::Keys row_keys{_memory_pool};
    row_keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      if (_statuses[i].ok()) {
        row_keys.emplace_back(_pinnable[i].data(), _pinnable[i].size());
      }
    }
    _current_value_idx += count;

    if (row_keys.empty()) {
      if (_current_value_idx >= _values.size()) {
        _current_split.reset();
      }
      return nullptr;
    }

    _produced += row_keys.size();
    if (_current_value_idx >= _values.size()) {
      _current_split.reset();
    }
    return _materializer.ReadRows(row_keys, nullptr);
  }

  void addDynamicFilter(velox::column_index_t,
                        const std::shared_ptr<velox::common::Filter>&) final {
    VELOX_UNSUPPORTED();
  }

  uint64_t getCompletedBytes() final { return 0; }
  uint64_t getCompletedRows() final { return _produced; }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
    final {
    return {};
  }

  void cancel() final {}

 private:
  std::string BuildIndexKey(const velox::variant& value) const {
    std::string key;
    secondary_key::AppendShardPrefix(key, _shard_id);
    secondary_key::AppendNotNullMarker(key);
    std::array<velox::variant, 1> point{value};
    primary_key::Create(point, *_row_type, key);
    return key;
  }

  velox::memory::MemoryPool& _memory_pool;
  Materializer _materializer;
  rocksdb::DB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
  const rocksdb::Snapshot* _snapshot;
  ObjectId _shard_id;
  std::vector<velox::variant> _values;
  velox::RowTypePtr _row_type;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  size_t _current_value_idx = 0;
  uint64_t _produced = 0;
  // Reused across next() calls
  std::vector<std::string> _index_keys;
  std::vector<rocksdb::Slice> _key_slices;
  std::vector<rocksdb::PinnableSlice> _pinnable;
  std::vector<rocksdb::Status> _statuses;
};

}  // namespace sdb::connector
