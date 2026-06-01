////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/sdb_catalog/sdb_log.h"

#include "basics/duckdb_engine.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&SdbLog::id),
  GetIndex(&SdbLog::timestamp),
  GetIndex(&SdbLog::topic),
  GetIndex(&SdbLog::level),
  GetIndex(&SdbLog::message),
});

}  // namespace

// Backed by DuckDB's LogManager via the duckdb_logs() table function.
// LogBufferFeature is gone; SDB_* writes flow through LogManager and we surface
// them here so user-facing SQL queries against `sdb_log` keep working. The id
// column is a synthetic row number (LogManager has no stable id); topic/level
// mirror duckdb_logs.type / log_level; timestamp is the epoch-microseconds form
// of duckdb_logs.timestamp.
template<>
catalog::MaterializedData SystemTableSnapshot<SdbLog>::GetTableData() {
  auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
  auto result = conn->Query(
    "SELECT timestamp, type, log_level, message FROM duckdb_logs()");

  // Strings must outlive WriteData (DuckDB Vectors store string_t blobs that
  // get copied via StringVector::AddString -- which itself just takes a
  // pointer/size pair, so the source bytes need to be alive during that call).
  std::vector<std::string> topics;
  std::vector<std::string> levels;
  std::vector<std::string> messages;
  std::vector<SdbLog> values;
  if (result && !result->HasError()) {
    auto count = result->RowCount();
    topics.reserve(count);
    levels.reserve(count);
    messages.reserve(count);
    values.reserve(count);
    for (size_t row = 0; row < count; ++row) {
      auto ts_val = result->GetValue(0, row);
      auto type_val = result->GetValue(1, row);
      auto level_val = result->GetValue(2, row);
      auto msg_val = result->GetValue(3, row);

      topics.push_back(type_val.IsNull() ? std::string{} : type_val.ToString());
      levels.push_back(level_val.IsNull() ? std::string{}
                                          : level_val.ToString());
      messages.push_back(msg_val.IsNull() ? std::string{} : msg_val.ToString());

      values.push_back({
        .id = static_cast<uint64_t>(row),
        .timestamp = ts_val.IsNull()
                       ? uint64_t{0}
                       : static_cast<uint64_t>(ts_val.GetValue<int64_t>()),
        .topic = topics.back(),
        .level = levels.back(),
        .message = messages.back(),
      });
    }
  }

  auto columns = CreateColumns<SdbLog>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(columns, values[row], kNullMask, row);
  }
  return {std::move(columns), values.size()};
}

}  // namespace sdb::pg
