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

#include <duckdb/main/query_parameters.hpp>
#include <duckdb/main/query_result.hpp>

namespace sdb::query {

inline duckdb::QueryParameters DrivenQuery() {
  duckdb::QueryParameters parameters;
  parameters.result_eagerness = duckdb::ResultEagerness::FORCED;
  parameters.caller_drives = true;
  return parameters;
}

inline duckdb::unique_ptr<duckdb::QueryResult> TakeDrivenResult(
  duckdb::QueryResult& pending) {
  auto result = pending.TakeCollectorResult();
  if (!result) {
    pending.Complete();
  }
  return result;
}

}  // namespace sdb::query
