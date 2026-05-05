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

#include <memory>
#include <string>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "connector/index_source.h"
#include "rocksdb/iterator.h"

namespace sdb::connector {

struct SKRangeScanGlobalState : public CommonScanGlobalState {
  // Per-range lower/upper bounds (empty upper = use shard_upper_bound).
  // Reserved upfront; spans held by sk_iterator must remain stable.
  std::vector<std::string> lower_keys;
  std::vector<std::string> upper_keys;

  // Shard-level fallback upper bound (stable storage for the Slice).
  std::string shard_upper_bound;
  rocksdb::Slice shard_upper_bound_slice;

  // RocksDBPrefixRangeColumnIterator wrapping a single underlying RocksDB
  // iterator over all ranges. Stored as rocksdb::Iterator* because
  // RocksDBPrefixRangeColumnIterator extends rocksdb::Iterator.
  std::unique_ptr<rocksdb::Iterator> sk_iterator;

  // Reused PK batch across batches; std::monostate until first call.
  PrimaryKeyBatch pk_batch;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SKRangeScanFunction(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output);

}  // namespace sdb::connector
