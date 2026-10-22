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
#include <string_view>
#include <vector>

namespace sdb::catalog {

struct DataDirectory;

}  // namespace sdb::catalog
namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::connector {

std::vector<std::shared_ptr<search::InvertedIndexStorage>> BoundStorages(
  duckdb::AttachedDatabase& db);

// Register the storage extension with a DuckDB config (before DB creation).
void RegisterSereneDBStorage(duckdb::DBConfig& config,
                             duckdb::shared_ptr<catalog::DataDirectory> layout);

// Register SereneDB optimizer extensions with a live DuckDB instance.
void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
