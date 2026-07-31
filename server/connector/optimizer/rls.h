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

#pragma once

namespace duckdb {
class DatabaseInstance;
}  // namespace duckdb

namespace sdb {
class ObjectId;
}
namespace sdb::catalog {
struct Snapshot;
class Table;
}  // namespace sdb::catalog

namespace sdb::connector {

// Installs the Row-Level Security enforcement pass: a pre-optimizer plan rewrite
// that filters every governed scan and attaches the WITH CHECK constraints of
// every write. Runs before the built-in optimizers, so the filters it emits are
// still pushed into the scans.
void RegisterRlsEnforcement(duckdb::DatabaseInstance& db);

// Refuses TRUNCATE on a table whose policies apply to `role`. TRUNCATE cannot be
// row-filtered, so allowing it would bypass row-level security outright.
void RlsGuardTruncate(const catalog::Snapshot& snapshot,
                      const catalog::Table& table, ObjectId role);

}  // namespace sdb::connector
