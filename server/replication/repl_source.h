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
#include <functional>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "replication/pgoutput.h"

namespace duckdb {

class DatabaseInstance;
class SQLStatement;

}  // namespace duckdb
namespace sdb::replication {

class ReplStream;

struct RelColumn {
  std::string name;
  duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
  bool is_key = false;
};
struct RelInfo {
  bool mapped = false;
  bool ready = false;
  uint64_t sync_lsn = 0;
  duckdb::idx_t table_oid = 0;
  duckdb::idx_t owner = 0;
  bool foreign_keys = false;
  std::string schema;
  std::string table;
  std::vector<RelColumn> columns;
  std::vector<std::string> missing_columns;
  std::vector<std::string> generated_columns;
};

struct ReplBatch {
  ReplStream* stream = nullptr;
  char op = 0;
  uint32_t relid = 0;
  const RelInfo* rel = nullptr;
  std::vector<size_t> keys;
  std::vector<size_t> cols;
  bool full = false;
  uint64_t rows = 0;
  irs::containers::FlatHashSet<std::string> touched;
  std::function<bool(const PgOutputMessage&)> pass_through;
};

std::optional<uint32_t> RowRelId(const PgOutputMessage& msg);

bool RowShape(const PgOutputMessage& msg, const RelInfo& rel, char& op,
              std::vector<size_t>& keys, std::vector<size_t>& cols,
              std::vector<PgColumn>& cells, bool& full);

duckdb::unique_ptr<duckdb::SQLStatement> BuildReplStatement(
  char op, std::string_view schema, std::string_view table,
  const std::vector<std::string>& key_names,
  const std::vector<std::string>& col_names, bool full);

duckdb::unique_ptr<duckdb::SQLStatement> BuildTruncate(std::string_view schema,
                                                       std::string_view table);

duckdb::unique_ptr<duckdb::SQLStatement> BuildCopyFromStdin(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& columns, bool binary);

void RegisterReplicationSourceFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::replication
