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
#include <memory>

namespace duckdb {

class DatabaseInstance;
}

namespace sdb {

class ConnectionContext;

namespace catalog {

struct Snapshot;
class Table;

}  // namespace catalog
}  // namespace sdb
namespace sdb::connector {

// Register serenedb_truncate() PRAGMA + table function with DuckDB.
// Multi-table TRUNCATE lowers to a CALL of this function. Single-table
// TRUNCATE lowers to a DeleteStatement with `is_truncate=true`, which
// SereneDBCatalog::PlanDelete dispatches to SereneDBPhysicalTruncate
// (which calls TruncateResolvedTable below).
void RegisterTruncateFunction(duckdb::DatabaseInstance& db);

// Wipes one already-resolved table: range-deletes its row range + every
// secondary/inverted index shard's range under one rocksdb WriteBatch,
// then calls IndexWriter::Clear() on each inverted shard with the
// post-write seq. The caller is responsible for validating that the
// table is truncatable (not a view, not a file table).
void TruncateResolvedTable(
  ConnectionContext& conn_ctx,
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const std::shared_ptr<catalog::Table>& table);

}  // namespace sdb::connector
