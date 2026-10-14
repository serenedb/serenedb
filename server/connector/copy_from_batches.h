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

#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>

#include "pg/copy_in_batches.h"

namespace sdb::connector {

bool BindCopyFrom(duckdb::ClientContext& context,
                  duckdb::CopyFromFunctionBindInput& input, bool binary);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitCopyFromGlobal(
  duckdb::ClientContext& context, const std::string& path,
  pg::CopyInTarget target, bool ordered);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitCopyFromLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global);

void ScanCopyFrom(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input, duckdb::DataChunk& output);

}  // namespace sdb::connector
