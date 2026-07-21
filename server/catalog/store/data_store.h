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

#include <absl/status/status.h>

#include <atomic>
#include <duckdb/main/connection.hpp>
#include <span>
#include <string_view>

#include "catalog/store/store.h"

namespace sdb::catalog {

// The data side of the catalog/data split: owns the DuckDB file database
// (<datadir>/engine_duckdb/data.db, attached as __sdb_data) that holds the
// store tables and their indexes. Attached only after InitCatalog, so the
// data WAL replays with the catalog live. Store DDL executes here, ordered
// around the catalog-WAL append by CatalogStore::Write; every call runs
// under CatalogStore's write mutex (plus the single-threaded boot), so the
// connection needs no lock of its own.
class DataStore {
 public:
  inline static DataStore* gInstance = nullptr;
  static DataStore& instance() noexcept { return *gInstance; }

  // Gate for work that touches the data DB from background tasks
  // (DropTask): false until Initialize finished reconciling.
  static bool IsReady() noexcept {
    return gInstance && gInstance->_ready.load(std::memory_order_acquire);
  }

  DataStore();
  ~DataStore();

  // Attaches data.db (replaying its WAL -- the catalog must already be
  // initialized), resolves a pending order-sensitive batch, reconciles the
  // store state against the catalog, and opens the gate. Fatal on failure.
  void Initialize(std::string_view database_directory);
  void Shutdown();

  // Executes the batch's store ops in order inside one data transaction.
  absl::Status ApplyStoreOps(std::span<const CatalogStore::Entry> entries);

  // Boot-time sanity: the store table exists and its column names/types
  // match. Assert-only (no-op in release builds).
  void ValidateStoreTable(const StoreTableDef& def);

 private:
  absl::Status ExecuteEntry(const CatalogStore::Entry& entry);
  absl::Status ExecuteCreateStoreTable(const StoreTableDef& def);
  absl::Status ExecuteCreateStoreTableImpl(const StoreTableDef& def,
                                           bool with_checks);
  void ResolvePendingAlter();
  void Reconcile();
  void ReconcileTable(const Table& table);
  void SweepOrphans();
  void SweepSearchDirs();

  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::string _database_directory;
  std::atomic<bool> _ready = false;
};

DataStore& GetDataStore();

}  // namespace sdb::catalog
