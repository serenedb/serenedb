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
#include <string>
#include <string_view>

#include "basics/result.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

class Table;

}  // namespace sdb::catalog
namespace sdb::query {

// Name of the internal DuckDB-attached database that holds per-shard cache
// tables for kSearch tables. Uses the reserved `$` suffix so it cannot
// collide with a user-attached database. See search_table_shard_native.md
// §1.4 for the design rationale.
inline constexpr std::string_view kSearchCacheDatabase = "sdb_cache$";

// Cache table name format -- "cache_<id>" in schema `main` of
// `sdb_cache$`. The id is the `cache_table_id` field stored on the
// search-table catalog entry.
std::string SearchCacheTableName(ObjectId cache_table_id);

// Drops the cache table identified by `cache_table_id` from the internal
// `sdb_cache$` database. Idempotent: returns ok if the table doesn't
// exist (so recovery / retry is safe). Caller need not hold any
// transaction -- this opens a fresh DuckDB connection internally.
Result DropSearchCacheTable(ObjectId cache_table_id) noexcept;

// Creates the per-shard cache table for a kSearch table in `sdb_cache$`.
// Schema follows search_table_shard_native.md §1.3: `sdb_op$` first, then
// `sdb_pk$` for generated-PK tables (when `table.PKColumns()` is empty),
// then user columns. PK columns get NOT NULL; other user columns are
// nullable so op=DELETE tombstone rows can leave them NULL. Throws on
// failure -- caller is responsible for any catalog-side rollback.
void CreateSearchCacheTable(ObjectId cache_table_id,
                            const catalog::Table& table);

class DuckDBEngine {
 public:
  static DuckDBEngine& Instance();

  void Initialize();
  void Shutdown();

  duckdb::DuckDB& GetDB() { return *_db; }

  duckdb::unique_ptr<duckdb::Connection> CreateConnection();

 private:
  DuckDBEngine() = default;
  std::unique_ptr<duckdb::DuckDB> _db;
};

}  // namespace sdb::query
