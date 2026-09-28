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

#include <cstdint>
#include <duckdb/common/projection_index.hpp>

namespace duckdb {

class Binder;
class DatabaseInstance;
class LogicalGet;
class LogicalProjection;

}  // namespace duckdb
namespace sdb::connector {

struct SereneDBCreateIndexInfo;

// serenedb_reindex('index'[, schema[, catalog]]) -- REFRESH of a view-backed
// inverted index: bring it to the current source state and publish the
// result atomically. Registered as both a table function and a PRAGMA (the
// REINDEX INDEX grammar lowers to the PRAGMA form).
void RegisterReindexFunction(duckdb::DatabaseInstance& db);

// Narrow a delta pass's leaf to the delta files: the driver stamped each
// one's manifest id as `delta_file_base + its listing ordinal`, so the
// ordinals come off the statement's manifest; each is verified against THIS
// bind's listing (a moved listing aborts the pass, the next tick re-diffs),
// then pushed as a `file_index IN (...)` table filter -- the leaf keeps its
// FULL file list (iceberg keeps its delete state) and the reader skips
// every other file pre-open.
void NarrowScanToDelta(duckdb::LogicalGet& leaf,
                       const SereneDBCreateIndexInfo& info,
                       duckdb::ProjectionIndex file_index_slot);

// The delta pass's pk file element: docs are born with their manifest ids,
// which are `file_index + delta_file_base` by construction -- patch the
// backfill projection's file_index slot with the add.
void AddDeltaFileBase(duckdb::Binder& binder, duckdb::LogicalProjection& proj,
                      duckdb::ProjectionIndex file_index_slot,
                      uint64_t delta_file_base);

}  // namespace sdb::connector
