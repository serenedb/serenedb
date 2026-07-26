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

#include <duckdb/catalog/default/default_generator.hpp>
#include <string_view>

namespace duckdb {

class Catalog;
class SchemaCatalogEntry;

}  // namespace duckdb
namespace sdb::connector {

// pg_catalog and information_schema. Their content -- the system tables, the
// system views and the builtin macros -- is fixed when the process starts and
// is the same in every database, so it is not a projection of anything.
bool IsStaticSchema(std::string_view schema_name) noexcept;

// The relation namespace of a static schema: its system tables and its system
// views. Null for any other schema.
duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticRelationGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema);

// The builtin macros of a static schema, split the way duckdb splits them: a
// scalar macro and a table macro are one SereneDB kind and two duckdb ones, so
// each set generates the half it is keyed on. Null for any other schema.
duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticFunctionGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  bool table_functions);

}  // namespace sdb::connector
