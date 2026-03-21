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

#include "search_count_data_source.hpp"

#include <velox/vector/BaseVector.h>
#include <velox/vector/ComplexVector.h>

#include "basics/assert.h"

namespace sdb::connector {

SearchCountDataSource::SearchCountDataSource(
  velox::memory::MemoryPool& memory_pool, velox::RowTypePtr row_type,
  const irs::IndexReader& reader, const irs::Filter::Query& query)
  : _memory_pool{memory_pool},
    _row_type{std::move(row_type)},
    _reader{reader},
    _query{query} {
  SDB_ASSERT(_row_type->size() == 0);
}

void SearchCountDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "SearchCountDataSource: split is null");
  SDB_ENSURE(!_current_split, ERROR_INTERNAL,
             "SearchCountDataSource: a split is already being processed");
  _current_split = std::move(split);
}

// TODO(mbkkt) Finer-grained preemption: currently we check cancellation between
// segments (atomic_bool with relaxed ordering). To improve, adjust iresearch
// count to accept a cancellation check -- this would allow interruption within
// a segment.
// TODO(mbkkt) Add cancellation tests -- right now we don't have good coverage
// for cancel() behavior.
std::optional<velox::RowVectorPtr> SearchCountDataSource::next(
  uint64_t /*size*/, velox::ContinueFuture& /*future*/) {
  if (!_current_split) {
    return nullptr;
  }
  _current_split.reset();

  uint64_t count = 0;
  for (size_t i = 0; i < _reader.size(); ++i) {
    if (_cancelled.load(std::memory_order_relaxed)) {
      break;
    }
    auto& segment = _reader[i];
    auto doc = segment.mask(_query.execute({.segment = segment}));
    count += doc->count();
  }

  _produced += count;
  return velox::BaseVector::create<velox::RowVector>(_row_type, count,
                                                     &_memory_pool);
}

void SearchCountDataSource::addDynamicFilter(
  velox::column_index_t /*output_channel*/,
  const std::shared_ptr<velox::common::Filter>& /*filter*/) {
  VELOX_UNSUPPORTED();
}

uint64_t SearchCountDataSource::getCompletedBytes() { return 0; }

uint64_t SearchCountDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
SearchCountDataSource::getRuntimeStats() {
  return {};
}

void SearchCountDataSource::cancel() {
  _cancelled.store(true, std::memory_order_relaxed);
}

}  // namespace sdb::connector
