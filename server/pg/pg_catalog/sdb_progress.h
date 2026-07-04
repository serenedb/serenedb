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

#include "pg/system_table.h"

namespace sdb::pg {

// All live connections with their current statement, DuckDB query progress
// (percent + estimate-normalized rows) and the command-specific counters
// written by the execution paths. pg_stat_activity and the
// pg_stat_progress_* views derive from this table.
// NOLINTBEGIN
struct SdbProgress {
  static constexpr uint64_t kId = 999997;
  static constexpr std::string_view kName = "sdb_progress";

  int32_t pid;
  Oid datid;
  std::string usename;
  std::string datname;
  std::string_view state;
  std::string query;
  int64_t backend_start_us;
  int64_t query_start_us;
  double percent;
  int64_t rows_processed;
  int64_t rows_total;
  std::string_view command;
  std::string_view io_type;
  Oid relid;
  Oid current_relid;
  std::string_view phase;
  int64_t bytes_processed;
  int64_t bytes_total;
  int64_t tuples_processed;
  int64_t tuples_total;
  int64_t stage;
  int64_t stages_total;
  int64_t step;
  int64_t steps_total;
  int64_t items_processed;
  int64_t items_total;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<SdbProgress>::GetTableData();

}  // namespace sdb::pg
