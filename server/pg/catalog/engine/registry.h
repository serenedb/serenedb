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

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <span>
#include <string_view>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/oids.h"

namespace sdb::pg {

struct StaticView {
  std::string_view schema;
  std::string_view name;
  duckdb::unique_ptr<const duckdb::CreateViewInfo> info;
  duckdb::Permissions permissions;
  duckdb::idx_t oid = 0;
  std::string_view options;
  mutable duckdb::shared_ptr<duckdb::ViewColumnInfo> columns;
};

inline constexpr duckdb::AclItem kPublicSelect{
  .grantee = kPublicGrantee,
  .grantor = kRootUser,
  .privs = duckdb::AclMode::Select};

std::span<const duckdb::DefaultType> ExternalTypes();

void InitSystemTables();
void InitSystemViews(duckdb::Parser& parser);
void InitSystemFunctions(duckdb::DatabaseInstance& db, duckdb::Parser& parser);

const SystemTable* GetSystemTable(std::string_view schema,
                                  std::string_view name);
const SystemTable* FindSystemTable(duckdb::idx_t oid);
std::span<SystemTable* const> SystemTableFunctions();
const StaticView* GetSystemView(std::string_view schema, std::string_view name);
const StaticView* FindSystemView(duckdb::idx_t oid);
duckdb::idx_t SystemRelationOid(std::string_view schema, std::string_view name);
const duckdb::CreateMacroInfo* GetSystemFunction(std::string_view schema,
                                                 std::string_view name,
                                                 duckdb::MacroType kind);

const duckdb::Permissions& SystemPermissions(bool superuser_only);
const duckdb::Permissions& SchemaPermissions();

void VisitSystemTables(std::string_view schema,
                       absl::FunctionRef<void(const SystemTable&)> visitor);
void VisitSystemViews(std::string_view schema,
                      absl::FunctionRef<void(const StaticView&)> visitor);
void VisitSystemFunctions(
  std::string_view schema, duckdb::MacroType kind,
  absl::FunctionRef<void(std::string_view, const duckdb::CreateMacroInfo&)>
    visitor);

}  // namespace sdb::pg
