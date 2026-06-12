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

namespace duckdb {

class ErrorData;

}  // namespace duckdb
namespace sdb::pg {

// Native constraint violations from store tables surface as duckdb
// ConstraintExceptions carrying structured extra_info (table_name,
// column_name, key_columns/key_values, constraint_kind). Translates them to
// PG-shaped SqlExceptions (SQLSTATE 23xxx + Postgres wording); returns
// without effect for any other error.
void ThrowTranslatedConstraintError(const duckdb::ErrorData& error);

}  // namespace sdb::pg
