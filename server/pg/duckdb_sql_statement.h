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
#include <vector>

#include "pg/serialize.h"

namespace sdb::pg {

// DuckDB-based SQL statement -- replaces the old Velox-based SqlStatement.
// Used by both simple and extended protocol.
struct DuckDBStatement {
  void Reset() noexcept {
    prepared.reset();
    deferred_copy.reset();
    empty = false;
  }

  duckdb::unique_ptr<duckdb::PreparedStatement> prepared;
  // COPY ... FROM STDIN cannot be prepared at Parse time (the CSV sniff opens
  // /dev/stdin before the CopyData feeder exists), so the unbound, un-sniffed
  // statement is stashed here and bound at Execute. Non-null => deferred COPY,
  // and `prepared` stays null.
  duckdb::unique_ptr<duckdb::SQLStatement> deferred_copy;
  // Parse with an empty query string is legal in PG: Bind succeeds and
  // Execute replies EmptyQueryResponse. `prepared` stays null.
  bool empty = false;
};

struct DuckDBBindInfo {
  std::vector<VarFormat> output_formats;
  duckdb::vector<duckdb::Value> param_values;
};

}  // namespace sdb::pg
