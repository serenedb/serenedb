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

#include <cstdint>
#include <string>

#include <duckdb/common/enums/statement_type.hpp>

namespace duckdb {
class PreparedStatement;
}

namespace sdb::pg {

struct CommandTag {
  std::string tag;
  // The "effective" statement type -- same as `prepared.data->statement_type`
  // except for EXECUTE, where it's the underlying prepared's type. Drives the
  // `INSERT 0 N` / `UPDATE N` / etc. row-count formatting.
  duckdb::StatementType effective_type;
};

// PG-compatible CommandComplete verb (e.g. "SELECT", "CREATE TABLE",
// "DROP INDEX", "INSERT"). Not all DuckDB statement types exist in PG; those
// fall back to DuckDB's string representation.
CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared);

// The full CommandComplete tag including the trailing row count per PG rules:
// CHANGED_ROWS -> "INSERT 0 N" / "UPDATE N"; QUERY_RESULT -> "SELECT N";
// NOTHING -> no count (e.g. "CREATE TABLE", "BEGIN").
std::string FormatCommandTag(const duckdb::PreparedStatement& prepared,
                             duckdb::StatementReturnType return_type,
                             uint64_t rows);

}  // namespace sdb::pg
