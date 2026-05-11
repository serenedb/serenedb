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

#include "connector/duckdb_scan_base.hpp"
#include "connector/index_source.h"

namespace sdb::connector {

struct SKPointLookupGlobalState : public CommonScanGlobalState {
  size_t point_offset = 0;  // next index into SkPointScan::points to probe
  // Reused PK batch across batches; std::monostate until first call.
  PrimaryKeyBatch pk_batch;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SKPointLookupInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SKPointLookupFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

}  // namespace sdb::connector
