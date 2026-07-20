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
#include <duckdb/common/types.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/fwd.h"
#include "replication/pgoutput.h"

namespace duckdb {

class DatabaseInstance;
class SQLStatement;

}  // namespace duckdb
namespace sdb::replication {

class ReplStream;

// One remote relation mapped to a local table, columns in the remote order.
struct RelColumn {
  std::string name;
  duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
  bool is_key = false;
};
struct RelInfo {
  bool mapped = false;
  std::string schema;
  std::string table;
  std::vector<RelColumn> columns;
};

// The batch a subscriber is currently applying, published to repl_src()'s scan
// via ConnectionContext::SetReplBatch. The scan pulls rows from `stream` and
// keeps consuming while they match (op, relation, key/col selection); a row
// that differs is the batch boundary (left for the control loop). Column order:
// INSERT = cols; DELETE = keys; UPDATE = keys then cols.
struct ReplBatch {
  ReplStream* stream = nullptr;
  char op = 0;  // 'I' / 'U' / 'D'
  uint32_t relid = 0;
  const RelInfo* rel = nullptr;
  std::vector<size_t> keys;  // key column indices (WHERE)
  std::vector<size_t>
    cols;  // set/insert column indices (present-non-unchanged)
  // REPLICA IDENTITY FULL: no key index, the old tuple carries every column, so
  // the WHERE matches on all of `keys` with IS NOT DISTINCT FROM (NULL-safe).
  bool full = false;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

// The relation id of a row-change message, or nullopt for anything else.
std::optional<uint32_t> RowRelId(const PgOutputMessage& msg);

// Classify a row against its relation: op ('I'/'U'/'D'), key columns (WHERE)
// and set columns (all present-non-unchanged, keys included -- so a key change
// rewrites the key). Under REPLICA IDENTITY FULL (old tuple sent as 'O', no key
// flagged) `full` is set and keys are all present old-tuple columns, matched
// NULL-safely. `cells` returns the parsed primary tuple (new for I/U, old for
// D). Returns false for a non-row or malformed message.
bool RowShape(const PgOutputMessage& msg, const RelInfo& rel, char& op,
              std::vector<size_t>& keys, std::vector<size_t>& cols,
              std::vector<PgColumn>& cells, bool& full);

// Build the apply DML for one (op, relation, column set), sourced from
// repl_src(): INSERT INTO / DELETE ... USING / UPDATE ... FROM. Values never
// appear in the statement -- they stream through repl_src at execution.
duckdb::unique_ptr<duckdb::SQLStatement> BuildReplStatement(
  char op, std::string_view schema, std::string_view table,
  const std::vector<std::string>& key_names,
  const std::vector<std::string>& col_names, bool full);

// TRUNCATE schema.table -- applied for a pgoutput Truncate message.
duckdb::unique_ptr<duckdb::SQLStatement> BuildTruncate(std::string_view schema,
                                                       std::string_view table);

// COPY schema.table (cols) FROM STDIN -- the local sink for one table's initial
// snapshot. Built as an AST (never SQL text), so the apply path parses nothing;
// the subscriber feeds the publisher's COPY-out bytes to it via a CopyInBridge.
duckdb::unique_ptr<duckdb::SQLStatement> BuildCopyFromStdin(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& columns, bool binary);

// Register repl_src('schema','table','col,col,...'): the table function a
// logical-replication apply uses as the SOURCE of its batched DELETE/UPDATE/
// INSERT. Its scan pulls the current batch's decoded pgoutput rows from the
// subscriber's ReplStream and deserializes them straight into the output
// vectors -- deserialization inside execution, mirroring COPY FROM STDIN's
// scan. The subscriber publishes the batch via ConnectionContext::SetReplBatch.
void RegisterReplicationSourceFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::replication
