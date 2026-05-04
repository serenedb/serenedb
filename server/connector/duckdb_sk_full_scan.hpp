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
#include <memory>
#include <string>

#include "connector/duckdb_scan_base.hpp"
#include "connector/index_source.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

namespace sdb::connector {

// Global state for SKFullScan, SKPointLookup (fallback), and SKRangeScan
// (fallback). The SK iterator is created lazily on the first scan call.
struct SKFullScanGlobalState : public CommonScanGlobalState {
  std::unique_ptr<rocksdb::Iterator> sk_iterator;
  std::string sk_upper_bound;
  rocksdb::Slice sk_upper_bound_slice;
  // Reused PK batch across batches. Default state is std::monostate;
  // switched on first call to index_source->CreatePkBatch() (rocksdb
  // table: PrimaryKeysBytes; file-backed view: PrimaryKeyI64 /
  // PrimaryKeyI64I64).
  PrimaryKeyBatch pk_batch;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SKFullScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output);

}  // namespace sdb::connector
