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
#include <duckdb/common/enums/statement_type.hpp>
#include <string_view>

namespace duckdb {

class PreparedStatement;
}

namespace sdb::pg {

struct CommandTag {
  // Static-lifetime verb: a string literal or a StatementTypeToString() view,
  // so the tag never allocates.
  std::string_view tag;
  // The "effective" statement type -- same as `prepared.data->statement_type`
  // except for EXECUTE, where it's the underlying prepared's type. Selects the
  // `INSERT 0 N` form (INSERT) from the plain `<TAG> N` form.
  duckdb::StatementType effective_type;
  // PG's display_rowcount: whether CommandComplete appends an affected/returned
  // row count. True for SELECT / INSERT / UPDATE / DELETE / MERGE / COPY (and
  // CREATE TABLE AS, which completes as "SELECT N"); false for everything else,
  // including TRUNCATE ("TRUNCATE TABLE", no count), DDL, BEGIN, SET, ...
  bool rowcount = false;
};

// PG-compatible CommandComplete verb (e.g. "SELECT", "CREATE TABLE",
// "DROP INDEX", "INSERT") plus the rowcount flag. Not all DuckDB statement
// types exist in PG; those fall back to DuckDB's string representation. The
// trailing row count is written straight into the send buffer by
// WriteCommandComplete (no intermediate string).
CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared);

}  // namespace sdb::pg
