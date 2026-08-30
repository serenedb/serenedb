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

#include "pg/pg_catalog/builtin_functions.h"

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <vector>

namespace sdb::pg {
namespace {

template<typename Entry>
void EmitSignatures(const Entry& entry, BuiltinFunction& row,
                    absl::FunctionRef<void(const BuiltinFunction&)> visitor,
                    uint64_t& next_oid) {
  for (duckdb::idx_t offset = 0; offset < entry.functions.functions.size();
       ++offset) {
    const auto& function = entry.functions.GetFunctionByOffset(offset);
    const auto& signature = function.GetSignature();

    row.oid = ObjectId{next_oid++};
    row.return_type = function.GetReturnType();
    row.has_varargs = function.HasVarArgs();
    row.parameter_types.clear();
    row.parameter_types.reserve(signature.GetParameterCount());
    for (duckdb::idx_t i = 0; i < signature.GetParameterCount(); ++i) {
      row.parameter_types.push_back(signature.GetParameter(i).GetType());
    }
    visitor(row);
  }
}

template<typename Entry>
void EmitArguments(const Entry& entry, BuiltinFunction& row,
                   absl::FunctionRef<void(const BuiltinFunction&)> visitor,
                   uint64_t& next_oid) {
  for (duckdb::idx_t offset = 0; offset < entry.functions.functions.size();
       ++offset) {
    const auto& function = entry.functions.GetFunctionByOffset(offset);

    row.oid = ObjectId{next_oid++};
    row.return_type = duckdb::LogicalType::INVALID;
    row.has_varargs = function.HasVarArgs();
    row.parameter_types = function.GetArguments();
    visitor(row);
  }
}

void EmitMacros(const duckdb::MacroCatalogEntry& entry, BuiltinFunction& row,
                absl::FunctionRef<void(const BuiltinFunction&)> visitor,
                uint64_t& next_oid) {
  for (const auto& macro : entry.macros) {
    row.oid = ObjectId{next_oid++};
    row.return_type = macro->return_types.empty() ? duckdb::LogicalType::INVALID
                                                  : macro->return_types[0];
    row.has_varargs = false;
    row.returns_set = macro->type == duckdb::MacroType::TABLE_MACRO;
    row.parameter_types = macro->types;
    visitor(row);
  }
}

}  // namespace

void VisitBuiltinFunctions(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(const BuiltinFunction&)> visitor) {
  auto& system_catalog = duckdb::Catalog::GetSystemCatalog(context);

  std::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  const auto collect = [&entries](duckdb::CatalogEntry& entry) {
    entries.emplace_back(entry);
  };
  for (const auto& schema_name : {duckdb::Identifier::DefaultSchema(),
                                  duckdb::Identifier{"pg_catalog"}}) {
    auto schema = system_catalog.GetSchema(
      context, schema_name, duckdb::OnEntryNotFound::RETURN_NULL);
    if (!schema) {
      continue;
    }
    schema->Scan(context, duckdb::CatalogType::SCALAR_FUNCTION_ENTRY,
                 [&](duckdb::CatalogEntry& entry) {
                   if (entry.type != duckdb::CatalogType::TABLE_MACRO_ENTRY) {
                     collect(entry);
                   }
                 });
    schema->Scan(context, duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
                 [&](duckdb::CatalogEntry& entry) {
                   if (entry.type != duckdb::CatalogType::MACRO_ENTRY) {
                     collect(entry);
                   }
                 });
    schema->Scan(context, duckdb::CatalogType::PRAGMA_FUNCTION_ENTRY, collect);
  }

  std::ranges::sort(entries, [](const duckdb::CatalogEntry& lhs,
                                const duckdb::CatalogEntry& rhs) {
    const auto left_schema = lhs.ParentSchema().name.GetIdentifierName();
    const auto right_schema = rhs.ParentSchema().name.GetIdentifierName();
    if (left_schema != right_schema) {
      return left_schema < right_schema;
    }
    const auto left = lhs.name.GetIdentifierName();
    const auto right = rhs.name.GetIdentifierName();
    return left != right ? left < right : lhs.type < rhs.type;
  });

  uint64_t next_oid = id::kFirstBuiltinFunction.id();
  for (auto ref : entries) {
    auto& entry = ref.get();
    BuiltinFunction row;
    row.name = entry.name.GetIdentifierName();
    row.kind = entry.type;

    switch (entry.type) {
      case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
        EmitSignatures(entry.Cast<duckdb::ScalarFunctionCatalogEntry>(), row,
                       visitor, next_oid);
        break;
      case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
        EmitSignatures(entry.Cast<duckdb::AggregateFunctionCatalogEntry>(), row,
                       visitor, next_oid);
        break;
      case duckdb::CatalogType::WINDOW_FUNCTION_ENTRY:
        EmitSignatures(entry.Cast<duckdb::WindowFunctionCatalogEntry>(), row,
                       visitor, next_oid);
        break;
      case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
        row.returns_set = true;
        EmitArguments(entry.Cast<duckdb::TableFunctionCatalogEntry>(), row,
                      visitor, next_oid);
        break;
      case duckdb::CatalogType::PRAGMA_FUNCTION_ENTRY:
        EmitArguments(entry.Cast<duckdb::PragmaFunctionCatalogEntry>(), row,
                      visitor, next_oid);
        break;
      case duckdb::CatalogType::MACRO_ENTRY:
      case duckdb::CatalogType::TABLE_MACRO_ENTRY:
        EmitMacros(entry.Cast<duckdb::MacroCatalogEntry>(), row, visitor,
                   next_oid);
        break;
      default:
        break;
    }
  }
}

}  // namespace sdb::pg
