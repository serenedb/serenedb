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

#include <iostream>

#include "query/duckdb_engine.h"

namespace sdb::pg {

bool DuckDBQueryHandler::ExecuteQuery(std::string_view sql) {
  // Strip trailing null bytes from PG wire protocol
  while (!sql.empty() && sql.back() == '\0') {
    sql.remove_suffix(1);
  }

  auto conn = query::DuckDBEngine::Instance().CreateConnection();
  auto result = conn->Query(std::string{sql});

  if (result->HasError()) {
    std::cerr << "DuckDB error for [" << sql << "]: " << result->GetError()
              << std::endl;
    return false;
  }

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

  SendCommandComplete("SELECT", total_rows);
  return true;
}

void DuckDBQueryHandler::SendRowDescription(
  const duckdb::QueryResult& result) {
  const auto num_fields = static_cast<uint16_t>(result.types.size());
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(7);

  for (uint16_t i = 0; i < num_fields; ++i) {
    // Column name
    const auto& name = result.names[i];
    _send.WriteUncommitted({name.data(), name.size()});
    _send.WriteUncommitted({"\0", 1});

    // table OID (0 = not from a table)
    absl::big_endian::Store32(_send.GetContiguousData(4), 0);
    // attribute number (0)
    absl::big_endian::Store16(_send.GetContiguousData(2), 0);
    // type OID
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              DuckDBTypeToOid(result.types[i]));
    // type size (-1 = variable)
    absl::big_endian::Store16(_send.GetContiguousData(2),
                              static_cast<uint16_t>(-1));
    // type modifier (-1 = none)
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              static_cast<uint32_t>(-1));
    // format code (0 = text)
    absl::big_endian::Store16(_send.GetContiguousData(2), 0);
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

void DuckDBQueryHandler::SendCommandComplete(std::string_view tag,
                                             uint64_t rows) {
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);

  // "SELECT N\0"
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
    // NULL is represented as -1 length
    absl::big_endian::Store32(_send.GetContiguousData(4),
                              static_cast<uint32_t>(-1));
    return;
  }

  // For the prototype, use text format via ToString() for all types.
  // This is correct but not optimized — will be replaced with direct
  // vector access in the production version.
  auto str = value.ToString();
  absl::big_endian::Store32(_send.GetContiguousData(4),
                            static_cast<uint32_t>(str.size()));
  _send.WriteUncommitted({str.data(), str.size()});
}

int32_t DuckDBQueryHandler::DuckDBTypeToOid(const duckdb::LogicalType& type) {
  // Map DuckDB types to PostgreSQL type OIDs
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return 16;  // BOOLOID
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
      return 21;  // INT2OID
    case duckdb::LogicalTypeId::INTEGER:
      return 23;  // INT4OID
    case duckdb::LogicalTypeId::BIGINT:
      return 20;  // INT8OID
    case duckdb::LogicalTypeId::FLOAT:
      return 700;  // FLOAT4OID
    case duckdb::LogicalTypeId::DOUBLE:
      return 701;  // FLOAT8OID
    case duckdb::LogicalTypeId::DECIMAL:
      return 1700;  // NUMERICOID
    case duckdb::LogicalTypeId::VARCHAR:
      return 25;  // TEXTOID
    case duckdb::LogicalTypeId::BLOB:
      return 17;  // BYTEAOID
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return 1114;  // TIMESTAMPOID
    case duckdb::LogicalTypeId::DATE:
      return 1082;  // DATEOID
    case duckdb::LogicalTypeId::TIME:
    case duckdb::LogicalTypeId::TIME_TZ:
      return 1083;  // TIMEOID
    case duckdb::LogicalTypeId::INTERVAL:
      return 1186;  // INTERVALOID
    case duckdb::LogicalTypeId::UUID:
      return 2950;  // UUIDOID
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::ARRAY:
      return 2277;  // ANYARRAYOID
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::MAP:
      return 2249;  // RECORDOID
    default:
      return 25;  // TEXTOID fallback
  }
}

}  // namespace sdb::pg
