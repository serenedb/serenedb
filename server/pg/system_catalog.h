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

#pragma once

#include <absl/functional/function_ref.h>

#include <duckdb/parser/parser.hpp>
#include <string_view>

#include "catalog/fwd.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

// Parse and cache all system views and functions. Call once at startup.
void InitSystemViews(duckdb::Parser& parser);
void InitSystemFunctions(duckdb::Parser& parser);

const catalog::VirtualTable* GetSystemTable(std::string_view schema,
                                            std::string_view name);
const catalog::VirtualTable* GetTable(std::string_view name);

void VisitSystemTables(
  absl::FunctionRef<void(const catalog::VirtualTable&, Oid)> visitor);
void VisitSystemViews(
  absl::FunctionRef<void(const catalog::PgSqlView&, Oid)> visitor);

// Schema-specific visitors for ScanEntries
void VisitPgCatalogTables(
  absl::FunctionRef<void(const catalog::VirtualTable&)> visitor);
void VisitPgCatalogViews(
  absl::FunctionRef<void(const catalog::PgSqlView&)> visitor);
void VisitPgCatalogFunctions(
  absl::FunctionRef<void(const catalog::PgSqlFunction&)> visitor);
void VisitInfoSchemaTables(
  absl::FunctionRef<void(const catalog::VirtualTable&)> visitor);
void VisitInfoSchemaViews(
  absl::FunctionRef<void(const catalog::PgSqlView&)> visitor);
void VisitInfoSchemaFunctions(
  absl::FunctionRef<void(const catalog::PgSqlFunction&)> visitor);

// Returns the unified PgSqlFunction for `name` (with all scalar and table
// overloads in its macros vector), or nullptr if absent.
std::shared_ptr<catalog::PgSqlFunction> GetPgCatalogFunction(
  std::string_view name);
std::shared_ptr<catalog::PgSqlFunction> GetInfoSchemaFunction(
  std::string_view name);

std::shared_ptr<catalog::PgSqlView> GetView(std::string_view name);
std::shared_ptr<catalog::PgSqlView> GetInfoSchemaView(std::string_view name);

}  // namespace sdb::pg
