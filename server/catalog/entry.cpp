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

#include "entry.h"

#include <duckdb/main/database_manager.hpp>

#include "basics/duckdb_engine.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

duckdb::DatabaseManager& IdAllocator() {
  return duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
}

ObjectId NextId() { return ObjectId{IdAllocator().NextOid()}; }

ObjectId NextNIds(uint64_t n) { return ObjectId{IdAllocator().NextOids(n)}; }

void RestoreId(uint64_t id) { IdAllocator().RestoreOid(id); }

}  // namespace sdb::catalog
