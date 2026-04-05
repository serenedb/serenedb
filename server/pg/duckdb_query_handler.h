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

#include <string_view>

#include "basics/message_buffer.h"
#include "pg/protocol.h"

namespace sdb::pg {

// Standalone DuckDB query execution and PG wire serialization.
// Bypasses the existing Query/Executor/Cursor pipeline for prototyping.
class DuckDBQueryHandler {
 public:
  explicit DuckDBQueryHandler(message::Buffer& send_buffer)
    : _send{send_buffer} {}

  // Execute a SQL query via DuckDB and write results to PG wire buffer.
  // Returns true if query was handled, false if it should fall through
  // to the existing Velox path.
  bool ExecuteQuery(std::string_view sql);

 private:
  void SendRowDescription(const duckdb::QueryResult& result);
  void SendDataRows(duckdb::DataChunk& chunk,
                    const duckdb::vector<duckdb::LogicalType>& types);
  void SendCommandComplete(std::string_view tag, uint64_t rows);

  static int32_t DuckDBTypeToOid(const duckdb::LogicalType& type);
  void SerializeValue(const duckdb::Value& value,
                      const duckdb::LogicalType& type);

  message::Buffer& _send;
};

}  // namespace sdb::pg
