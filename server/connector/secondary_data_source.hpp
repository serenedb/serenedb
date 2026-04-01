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

#include <absl/algorithm/container.h>
#include <velox/connectors/Connector.h>

#include "basics/fwd.h"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_filter.hpp"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector::detail {

inline std::unique_ptr<rocksdb::Iterator> CreateSecondaryIterator(
  rocksdb::DB& source, const rocksdb::ReadOptions& ro,
  rocksdb::ColumnFamilyHandle& cf) {
  return std::unique_ptr<rocksdb::Iterator>(source.NewIterator(ro, &cf));
}

inline std::unique_ptr<rocksdb::Iterator> CreateSecondaryIterator(
  rocksdb::Transaction& source, const rocksdb::ReadOptions& ro,
  rocksdb::ColumnFamilyHandle& cf) {
  return std::unique_ptr<rocksdb::Iterator>(source.GetIterator(ro, &cf));
}

}  // namespace sdb::connector::detail
namespace sdb::connector {

// TODO: replace with Misha's RocksdbRangeDataSource when it's ready
template<typename Materializer, bool Unique, typename Source>
class SecondaryIndexDataSource final : public velox::connector::DataSource {
 public:
  SecondaryIndexDataSource(velox::memory::MemoryPool& memory_pool,
                           Materializer materializer, Source& source,
                           rocksdb::ColumnFamilyHandle& cf,
                           const rocksdb::Snapshot* snapshot, ObjectId shard_id,
                           std::vector<SpecificPoint> values,
                           velox::RowTypePtr sk_type)
    : _memory_pool{memory_pool},
      _materializer{std::move(materializer)},
      _source{source},
      _cf{cf},
      _snapshot{snapshot},
      _shard_id{shard_id},
      _values{std::move(values)},
      _sk_type{std::move(sk_type)},
      _row_keys{memory_pool} {}

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

    _row_keys.clear();
    _row_keys.reserve(size);

    while (_row_keys.size() < size && _current_value_idx < _values.size()) {
      if (!_iterator) {
        BuildScanPrefix(_values[_current_value_idx]);

        rocksdb::ReadOptions ro;
        ro.snapshot = _snapshot;
        ro.total_order_seek = true;
        _upper_bound = _current_scan_prefix;
        IncrementPrefix(_upper_bound);
        _upper_bound_slice = rocksdb::Slice{_upper_bound};
        ro.iterate_upper_bound = &_upper_bound_slice;
        _iterator = detail::CreateSecondaryIterator(_source, ro, _cf);
        _iterator->Seek(_current_scan_prefix);
      }

      const auto pk_start = sizeof(ObjectId) + _value_key_size;

      if constexpr (Unique) {
        bool has_null =
          absl::c_any_of(_values[_current_value_idx],
                         [](const velox::variant& v) { return v.isNull(); });
        if (has_null) {
          CollectPKsFromKey(size, pk_start);
        } else {
          CollectPKsFromValue(size);
        }
      } else {
        CollectPKsFromKey(size, pk_start);
      }

      if (!_iterator->Valid()) {
        _iterator.reset();
        ++_current_value_idx;
      }
    }

    if (_row_keys.empty()) {
      _current_split.reset();
      _iterator.reset();
      return nullptr;
    }

    _produced += _row_keys.size();
    return _materializer.ReadRows(_row_keys, nullptr);
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
  void CollectPKsFromKey(uint64_t size, size_t pk_start) {
    while (_row_keys.size() < size && _iterator->Valid()) {
      auto key = _iterator->key();
      std::string_view key_view{key.data(), key.size()};
      if (!key_view.starts_with(_current_scan_prefix)) {
        break;
      }
      if (key_view.size() > pk_start) {
        _row_keys.emplace_back(key_view.substr(pk_start));
      }
      _iterator->Next();
    }
  }

  void CollectPKsFromValue(uint64_t size) {
    while (_row_keys.size() < size && _iterator->Valid()) {
      auto key = _iterator->key();
      std::string_view key_view{key.data(), key.size()};
      if (!key_view.starts_with(_current_scan_prefix)) {
        break;
      }
      auto val = _iterator->value();
      _row_keys.emplace_back(val.data(), val.size());
      _iterator->Next();
    }
  }

  void BuildScanPrefix(const SpecificPoint& point) {
    _current_scan_prefix.clear();
    secondary_key::AppendShardPrefix(_current_scan_prefix, _shard_id);
    secondary_key::AppendDummyColumnId(_current_scan_prefix);
    for (size_t i = 0; i < point.size(); ++i) {
      if (point[i].isNull()) {
        secondary_key::AppendNullMarker(_current_scan_prefix);
      } else {
        secondary_key::AppendNotNullMarker(_current_scan_prefix);
        primary_key::AppendVariantValue(_current_scan_prefix, point[i],
                                        _sk_type->childAt(i));
      }
    }
    _value_key_size = _current_scan_prefix.size() - sizeof(ObjectId);
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
  Source& _source;
  rocksdb::ColumnFamilyHandle& _cf;
  const rocksdb::Snapshot* _snapshot;
  ObjectId _shard_id;
  std::vector<SpecificPoint> _values;
  velox::RowTypePtr _sk_type;
  primary_key::Keys _row_keys;
  std::string _current_scan_prefix;
  size_t _value_key_size = 0;
  std::string _upper_bound;
  rocksdb::Slice _upper_bound_slice;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  std::unique_ptr<rocksdb::Iterator> _iterator;
  size_t _current_value_idx = 0;
  uint64_t _produced = 0;
};

}  // namespace sdb::connector
