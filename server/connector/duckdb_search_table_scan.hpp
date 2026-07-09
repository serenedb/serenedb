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

#pragma once

#include <cstdint>
#include <duckdb/function/table_function.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <memory>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "iresearch/index/hit_batcher.hpp"

namespace sdb::connector {

struct SearchTableScanGlobalState : public CommonScanGlobalState {
  std::shared_ptr<irs::DirectoryReader> reader;
  irs::DocIterator::ptr live_docs;
  std::unique_ptr<HitBatcher> hit_batcher;
  bool count_only = false;
  uint64_t count_remaining = 0;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchTableScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchTableScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output);

}  // namespace sdb::connector
