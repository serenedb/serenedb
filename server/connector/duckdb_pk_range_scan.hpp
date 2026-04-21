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
#include "rocksdb/iterator.h"

namespace sdb::connector {

struct PKRangeScanGlobalState : public PKScanGlobalState {
  // Per-(column x range) key storage; flat layout [col*N_ranges + range_idx].
  // Spans into these vectors are held by iterators -- no reallocation after
  // iterators are created.
  std::vector<std::string> split_prefix_keys;
  std::vector<std::string> split_upper_bound_keys;
  // Declared last -> destroyed first; iterators hold Slice refs into
  // upper_bound_slices (base) and spans into split_*_keys above.
  std::vector<std::unique_ptr<rocksdb::Iterator>> iterators;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void PKRangeScanFunction(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output);

}  // namespace sdb::connector
