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

#include <array>
#include <span>

#include "rocksdb/db.h"

namespace sdb::connector {

class MultiGetContext {
 public:
  static constexpr size_t kBatchSize = 32;  // copied from rocksdb
  // TODO(mbkkt) benchmark and choose best threshold
  static constexpr size_t kMultiGetThreshold = 1;

  MultiGetContext(rocksdb::ColumnFamilyHandle& cf,
                  const rocksdb::ReadOptions& read_options)
    : _cf{cf}, _read_options{read_options} {}

  // keys must be sorted by the same order as the rocksdb comparator produces
  template<typename DataSource, typename ValueProcessor>
  void MultiGet(DataSource& data_source, std::span<const rocksdb::Slice> keys,
                ValueProcessor&& value_processor);

 private:
  std::array<rocksdb::Status, kBatchSize> _statuses;
  std::array<rocksdb::PinnableSlice, kBatchSize> _values;
  rocksdb::ColumnFamilyHandle& _cf;
  rocksdb::ReadOptions _read_options;
};

template<typename DataSource, typename ValueProcessor>
void MultiGetContext::MultiGet(DataSource& data_source,
                               std::span<const rocksdb::Slice> keys,
                               ValueProcessor&& value_processor) {
  const auto n = keys.size();
  for (size_t start = 0; start < n; start += kBatchSize) {
    const auto batch = std::min(kBatchSize, n - start);
    if (batch <= kMultiGetThreshold) {
      for (size_t i = 0; i < batch; ++i) {
        _statuses[i] =
          data_source.Get(_read_options, &_cf, keys[start + i], &_values[i]);
      }
    } else {
      data_source.MultiGet(_read_options, &_cf, batch, keys.data() + start,
                           _values.data(), _statuses.data(), true);
    }
    for (size_t c = 0; c < batch; ++c) {
      value_processor(keys[start + c], _values[c], _statuses[c]);
      _values[c].Reset();
    }
  }
}

}  // namespace sdb::connector
