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

#include "sql_query_view.h"

#include <duckdb/parser/parsed_data/create_view_info.hpp>

#include "basics/errors.h"
#include "basics/result.h"

namespace sdb {

std::shared_ptr<SqlQueryViewImpl::State> SqlQueryViewImpl::Create() {
  return std::make_shared<State>();
}

Result SqlQueryViewImpl::Parse(State& state, ObjectId database_id,
                               std::string_view query) {
  // Parse the SQL view query through DuckDB
  auto info = duckdb::CreateViewInfo::ParseSelect(std::string{query});
  if (!info) {
    return {ERROR_BAD_PARAMETER, "failed to parse view query"};
  }
  state.view_info = duckdb::make_uniq<duckdb::CreateViewInfo>();
  state.view_info->query = std::move(info);
  return {};
}

Result SqlQueryViewImpl::Check(ObjectId database, std::string_view name,
                               const State& state, const Config& config) {
  // DuckDB handles view validation during binding
  (void)database;
  (void)name;
  (void)config;
  return {};
}

}  // namespace sdb
