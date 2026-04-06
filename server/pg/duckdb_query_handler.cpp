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

#include "pg/duckdb_query_handler.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/numbers.h>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>

#include "connector/duckdb_client_state.h"
#include "query/duckdb_engine.h"

namespace sdb::pg {

std::string DuckDBQueryHandler::ExecuteQuery(std::string_view sql) {
  // Strip trailing null bytes from PG wire protocol
  while (!sql.empty() && sql.back() == '\0') {
    sql.remove_suffix(1);
  }
  if (sql.empty()) {
    return {};
  }

  // Use the persistent per-session DuckDB connection
  auto& conn = _duckdb_conn;

  // Extract individual statements for multi-statement support
  auto statements = conn.ExtractStatements(std::string{sql});
  if (statements.empty()) {
    return {};
  }

  for (auto& stmt : statements) {
    auto error = ExecuteSingleStatement(conn, stmt->query);
    if (!error.empty()) {
      return error;
    }
  }
  return {};
}

std::string DuckDBQueryHandler::ExecuteSingleStatement(
  duckdb::Connection& conn, const std::string& sql) {
  auto result = conn.Query(sql);

  if (result->HasError()) {
    return result->GetError();
  }

  auto stmt_type = result->statement_type;

  bool is_dml = (stmt_type == duckdb::StatementType::INSERT_STATEMENT ||
                 stmt_type == duckdb::StatementType::UPDATE_STATEMENT ||
                 stmt_type == duckdb::StatementType::DELETE_STATEMENT);

  // DDL: no RowDescription, just CommandComplete
  if (result->types.empty()) {
    SendCommandComplete(stmt_type, 0);
    return {};
  }

  // DML: extract affected row count, don't send DataRows
  if (is_dml) {
    uint64_t affected = 0;
    while (true) {
      auto chunk = result->Fetch();
      if (!chunk || chunk->size() == 0) break;
      if (chunk->ColumnCount() > 0 && chunk->size() > 0) {
        chunk->Flatten();
        for (duckdb::idx_t i = 0; i < chunk->size(); ++i) {
          affected += duckdb::FlatVector::GetData<int64_t>(chunk->data[0])[i];
        }
      }
    }
    SendCommandComplete(stmt_type, affected);
    return {};
  }

  // SELECT/etc: RowDescription + DataRows + CommandComplete
  SendRowDescription(*result);

  uint64_t total_rows = 0;
  while (true) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    total_rows += chunk->size();
    SendDataRows(*chunk, result->types);
  }

  SendCommandComplete(stmt_type, total_rows);
  return {};
}

std::string_view DuckDBQueryHandler::StatementTypeToTag(
  duckdb::StatementType type) {
  switch (type) {
    case duckdb::StatementType::SELECT_STATEMENT:
      return "SELECT";
    case duckdb::StatementType::INSERT_STATEMENT:
      return "INSERT 0";
    case duckdb::StatementType::UPDATE_STATEMENT:
      return "UPDATE";
    case duckdb::StatementType::DELETE_STATEMENT:
      return "DELETE";
    case duckdb::StatementType::CREATE_STATEMENT:
      return "CREATE TABLE";
    case duckdb::StatementType::DROP_STATEMENT:
      return "DROP TABLE";
    case duckdb::StatementType::ALTER_STATEMENT:
      return "ALTER TABLE";
    case duckdb::StatementType::TRANSACTION_STATEMENT:
      return "TRANSACTION";
    case duckdb::StatementType::COPY_STATEMENT:
      return "COPY";
    case duckdb::StatementType::EXPLAIN_STATEMENT:
      return "EXPLAIN";
    case duckdb::StatementType::VACUUM_STATEMENT:
      return "VACUUM";
    case duckdb::StatementType::SET_STATEMENT:
    case duckdb::StatementType::VARIABLE_SET_STATEMENT:
      return "SET";
    default:
      return "OK";
  }
}

void DuckDBQueryHandler::SendRowDescription(
  const duckdb::QueryResult& result) {
  const auto num_fields = static_cast<uint16_t>(result.types.size());
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(7);

  for (uint16_t i = 0; i < num_fields; ++i) {
    const auto& name = result.names[i];
    _send.WriteUncommitted({name.data(), name.size()});
    _send.WriteUncommitted({"\0", 1});

    absl::big_endian::Store32(_send.GetContiguousData(4), 0);  // table OID
    absl::big_endian::Store16(_send.GetContiguousData(2), 0);  // attr number
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              DuckDBTypeToOid(result.types[i]));
    absl::big_endian::Store16(_send.GetContiguousData(2),
                              static_cast<uint16_t>(-1));  // type size
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              static_cast<uint32_t>(-1));  // type modifier
    absl::big_endian::Store16(_send.GetContiguousData(2), 0);  // format text
  }

  prefix_data[0] = PQ_MSG_ROW_DESCRIPTION;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  absl::big_endian::Store16(prefix_data + 5, num_fields);
  _send.Commit(false);
}

void DuckDBQueryHandler::SendDataRows(
  duckdb::DataChunk& chunk,
  const duckdb::vector<duckdb::LogicalType>& types) {
  const auto num_cols = static_cast<uint16_t>(chunk.ColumnCount());
  const auto num_rows = chunk.size();

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    const auto uncommitted_size = _send.GetUncommittedSize();
    auto* prefix_data = _send.GetContiguousData(7);

    for (uint16_t col = 0; col < num_cols; ++col) {
      auto value = chunk.GetValue(col, row);
      SerializeValue(value, types[col]);
    }

    prefix_data[0] = PQ_MSG_DATA_ROW;
    absl::big_endian::Store32(
      prefix_data + 1,
      _send.GetUncommittedSize() - uncommitted_size - 1);
    absl::big_endian::Store16(prefix_data + 5, num_cols);
    _send.Commit(false);
  }
}

void DuckDBQueryHandler::SendCommandComplete(duckdb::StatementType type,
                                             uint64_t rows) {
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);

  auto tag = StatementTypeToTag(type);
  _send.WriteUncommitted(tag);
  _send.WriteUncommitted({" ", 1});
  auto rows_str = std::to_string(rows);
  _send.WriteUncommitted({rows_str.data(), rows_str.size()});
  _send.WriteUncommitted({"\0", 1});

  prefix_data[0] = PQ_MSG_COMMAND_COMPLETE;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  _send.Commit(false);
}

void DuckDBQueryHandler::SerializeValue(const duckdb::Value& value,
                                        const duckdb::LogicalType& type) {
  if (value.IsNull()) {
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              static_cast<uint32_t>(-1));
    return;
  }

  auto str = value.ToString();
  absl::big_endian::Store32(_send.GetContiguousData(4),
                            static_cast<uint32_t>(str.size()));
  _send.WriteUncommitted({str.data(), str.size()});
}

int32_t DuckDBQueryHandler::DuckDBTypeToOid(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return 16;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
      return 21;
    case duckdb::LogicalTypeId::INTEGER:
      return 23;
    case duckdb::LogicalTypeId::BIGINT:
      return 20;
    case duckdb::LogicalTypeId::FLOAT:
      return 700;
    case duckdb::LogicalTypeId::DOUBLE:
      return 701;
    case duckdb::LogicalTypeId::DECIMAL:
      return 1700;
    case duckdb::LogicalTypeId::VARCHAR:
      return 25;
    case duckdb::LogicalTypeId::BLOB:
      return 17;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return 1114;
    case duckdb::LogicalTypeId::DATE:
      return 1082;
    case duckdb::LogicalTypeId::TIME:
    case duckdb::LogicalTypeId::TIME_TZ:
      return 1083;
    case duckdb::LogicalTypeId::INTERVAL:
      return 1186;
    case duckdb::LogicalTypeId::UUID:
      return 2950;
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::ARRAY:
      return 2277;
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::MAP:
      return 2249;
    default:
      return 25;
  }
}

}  // namespace sdb::pg
