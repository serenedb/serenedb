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
#include <duckdb/storage/database_size.hpp>
#include <string_view>

namespace duckdb {

class Catalog;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

// The size the size functions report for one database, populated from the
// store file's blocks, the search tables' segments and the inverted indexes.
duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         duckdb::Catalog& catalog,
                                         std::string_view only_schema);

}  // namespace sdb::catalog
namespace sdb::connector {

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
