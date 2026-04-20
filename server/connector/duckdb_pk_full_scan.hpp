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
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

namespace sdb::connector {

// Global state for PKFullScan (full prefix table scan).
// Members are ordered so that iterators (which reference upper_bound_slices)
// are destroyed before upper_bound_data -- C++ destructs in reverse declaration
// order, so iterators must be declared last.
struct PKFullScanGlobalState : public CommonScanGlobalState {
  // upper_bound_data / upper_bound_slices must outlive iterators.
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
  // Destroyed first (declared last): RocksDB iterators reference the slices.
  std::vector<std::unique_ptr<rocksdb::Iterator>> iterators;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void PKFullScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output);

}  // namespace sdb::connector
