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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <string_view>

#include "catalog1/permissions.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/virtual_table.h"

namespace sdb::pg {

// A built-in pg_catalog / information_schema function as the static schema
// serves it: there is no catalog entry behind these, so the definition and the
// permissions travel together.
using StaticView = std::pair<std::shared_ptr<const duckdb::CreateViewInfo>,
                             catalog::Permissions>;
using StaticFunction = std::pair<std::shared_ptr<const duckdb::CreateMacroInfo>,
                                 catalog::Permissions>;

// Parse and cache all system views and functions. Call once at startup.
void InitSystemViews(duckdb::Parser& parser);
void InitSystemFunctions(duckdb::Parser& parser);

const VirtualTable* GetSystemTable(std::string_view schema,
                                   std::string_view name);
const VirtualTable* GetTable(std::string_view name);

void VisitSystemTables(
  absl::FunctionRef<void(const VirtualTable&, Oid)> visitor);
// The builtin views and functions carry the owner and ACL their entries get:
// there is no catalog record behind them, so the two travel beside the
// definition here exactly as they do on an entry.
void VisitSystemViews(absl::FunctionRef<void(const StaticView&, Oid)> visitor);

// Schema-specific visitors for ScanEntries
void VisitPgCatalogTables(absl::FunctionRef<void(const VirtualTable&)> visitor);
void VisitPgCatalogViews(absl::FunctionRef<void(const StaticView&)> visitor);
void VisitPgCatalogFunctions(
  absl::FunctionRef<void(const StaticFunction&)> visitor);
void VisitInfoSchemaTables(
  absl::FunctionRef<void(const VirtualTable&)> visitor);
void VisitInfoSchemaViews(absl::FunctionRef<void(const StaticView&)> visitor);
void VisitInfoSchemaFunctions(
  absl::FunctionRef<void(const StaticFunction&)> visitor);

// Returns the unified definition for `name` (with all scalar and table
// overloads in its macros vector), or nullptr if absent.
StaticFunction GetPgCatalogFunction(std::string_view name);
StaticFunction GetInfoSchemaFunction(std::string_view name);

StaticView GetView(std::string_view name);
StaticView GetInfoSchemaView(std::string_view name);

}  // namespace sdb::pg
