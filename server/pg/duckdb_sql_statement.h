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
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <memory>
#include <string>
#include <vector>

#include "pg/serialize.h"

namespace sdb::pg {

// DuckDB-based SQL statement -- replaces the old Velox-based SqlStatement.
// Used by both simple and extended protocol.
struct DuckDBStatement {
  void Reset() noexcept {
    prepared.reset();
    extracted.clear();
    current_stmt_idx = 0;
    param_oids.clear();
    resolved_types.clear();
    resolved_names.clear();
  }

  duckdb::unique_ptr<duckdb::PreparedStatement> prepared;
  // For simple protocol multi-statement support
  duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> extracted;
  uint32_t current_stmt_idx = 0;
  // Parameter type OIDs from Parse message
  std::vector<int32_t> param_oids;

  std::vector<duckdb::LogicalType> resolved_types;
  std::vector<std::string> resolved_names;
};

struct DuckDBBindInfo {
  std::vector<VarFormat> output_formats;
  duckdb::vector<duckdb::Value> param_values;
};

}  // namespace sdb::pg
