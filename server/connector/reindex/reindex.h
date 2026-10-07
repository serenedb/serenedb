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

class DatabaseInstance;

}  // namespace duckdb
namespace sdb::connector {

// serenedb_reindex('index'[, schema[, catalog]]) -- REFRESH of a view-backed
// inverted index: bring it to the current source state and publish the
// result atomically. Registered as both a table function and a PRAGMA (the
// REINDEX INDEX grammar lowers to the PRAGMA form).
void RegisterReindexFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
