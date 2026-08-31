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
#include <duckdb/storage/storage_extension.hpp>
#include <string_view>

namespace sdb::connector {

class SereneDBStorageExtension : public duckdb::StorageExtension {
 public:
  SereneDBStorageExtension();
};

// Attaches the database `id` names, under `name`, with its catalog alone: the
// data file stays closed and its WAL unreplayed. Boot does this as it reads
// each database record, so everything the log says afterwards lands in a real
// CatalogSet -- and opens the storage only once the catalog is whole, which is
// what lets the WAL replay into inverted indexes that are already there.
void AttachDatabaseCatalog(duckdb::idx_t id, std::string_view name);

// Takes such an attachment back out without dropping anything, which a DETACH
// would: boot read the database's drop record after having attached it, and
// the record has already removed the database itself.
void DiscardDatabaseAttachment(std::string_view name);

// Opens the data file behind the attachment `name` and replays its WAL, which
// AttachDatabaseCatalog left for later. Throws what the load throws.
void LoadDatabaseStorage(std::string_view name);

// Register the storage extension with a DuckDB config (before DB creation).
void RegisterSereneDBStorage(duckdb::DBConfig& config);

// Register SereneDB optimizer extensions with a live DuckDB instance.
void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
