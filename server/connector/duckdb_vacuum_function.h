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

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::connector {

// Register serenedb_vacuum() table function with DuckDB. The VACUUM grammar
// lowers VACUUM (REFRESH_*|COMPACT_*) <object>
// into `PRAGMA serenedb_vacuum(option, name, [schema], [catalog])`.
void RegisterVacuumFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
