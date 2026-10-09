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
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <memory>
#include <span>
#include <string_view>

namespace duckdb {

struct DefaultType;

}  // namespace duckdb
namespace sdb::pg {

struct ViewBinding {
  duckdb::shared_ptr<duckdb::ViewColumnInfo> columns;
};

class SystemTable;

struct StaticView {
  std::string_view schema;
  std::string_view name;
  std::shared_ptr<const duckdb::CreateViewInfo> info;
  duckdb::Permissions permissions;
  duckdb::idx_t oid = 0;
  std::shared_ptr<ViewBinding> binding;
};
using StaticFunction = std::shared_ptr<const duckdb::CreateMacroInfo>;

std::span<const duckdb::DefaultType> ExternalTypes();

void InitSystemTables();
void InitSystemViews(duckdb::Parser& parser);
void InitSystemFunctions(duckdb::Parser& parser);

const SystemTable* GetSystemTable(std::string_view schema,
                                  std::string_view name);
const SystemTable* FindSystemTable(duckdb::idx_t oid);
const StaticView* GetSystemView(std::string_view schema, std::string_view name);
const StaticView* FindSystemView(duckdb::idx_t oid);
bool IsSystemRelation(std::string_view schema, std::string_view name);
StaticFunction GetSystemFunction(std::string_view schema, std::string_view name,
                                 duckdb::MacroType kind);

duckdb::Permissions SystemPermissions(bool superuser_only);
duckdb::Permissions SchemaPermissions();

void VisitSystemTables(std::string_view schema,
                       absl::FunctionRef<void(const SystemTable&)> visitor);
void VisitSystemViews(std::string_view schema,
                      absl::FunctionRef<void(const StaticView&)> visitor);
void VisitSystemFunctions(
  std::string_view schema, duckdb::MacroType kind,
  absl::FunctionRef<void(std::string_view, const duckdb::CreateMacroInfo&)>
    visitor);

}  // namespace sdb::pg
