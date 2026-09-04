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

#include <duckdb/common/optional_ptr.hpp>
#include <duckdb/common/types.hpp>
#include <string>
#include <string_view>

namespace duckdb {
class AttachedDatabase;
struct DBConfig;
}  // namespace duckdb

namespace sdb::catalog {

void RegisterClusterStorage(duckdb::DBConfig& config);

// The datadir layout of PLAN.md: engine_catalog/ holds the cluster catalog,
// engine_duckdb/ one file per database.
std::string ClusterFilePath(std::string_view directory);
std::string DatabaseFilePath(std::string_view name);

void InitCatalog(std::string_view directory);

void ShutdownCatalog();

// Attaches the serenedb database `name`, opening its file under the datadir.
void AttachDatabase(std::string_view name);

// The attached serenedb database with this id, or null. AttachedDatabase is a
// CatalogEntry, so the id is its oid.
duckdb::optional_ptr<duckdb::AttachedDatabase> FindAttachedDatabase(
  duckdb::idx_t id);

// Records the database in the cluster catalog on `context`'s transaction, so a
// rolled back CREATE DATABASE leaves no record. A database is two things: the
// attachment duckdb routes queries to, and this -- connection setup resolves
// the name against the record, so an attachment alone is unreachable.
void RegisterDatabaseIn(duckdb::ClientContext& context, std::string_view name);

}  // namespace sdb::catalog
