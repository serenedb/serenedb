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

#include <duckdb/main/database.hpp>

namespace sdb::connector {

// Debug views over the catalog's persistent form, definitions rendered as
// JSON text: sdb_catalog_wal() walks the on-disk wal frame by frame,
// sdb_catalog_snapshot() dumps the resident record maps (what a compaction
// would write).
void RegisterCatalogIntrospectFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
