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

#include "connector/functions/duckdb_aliases.h"

#include <absl/strings/str_cat.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/default/default_table_functions.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/parser_options.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "basics/assert.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kSourcePrefix = "duckdb_";
constexpr std::string_view kAliasPrefix = "sdb_";

constexpr std::array kTableFunctions{
  std::string_view{"duckdb_approx_database_count"},
  std::string_view{"duckdb_available_metrics"},
  std::string_view{"duckdb_columns"},
  std::string_view{"duckdb_connection_count"},
  std::string_view{"duckdb_constraints"},
  std::string_view{"duckdb_coordinate_systems"},
  std::string_view{"duckdb_databases"},
  std::string_view{"duckdb_dependencies"},
  std::string_view{"duckdb_eviction_queues"},
  std::string_view{"duckdb_extensions"},
  std::string_view{"duckdb_external_file_cache"},
  std::string_view{"duckdb_functions"},
  std::string_view{"duckdb_indexes"},
  std::string_view{"duckdb_keywords"},
  std::string_view{"duckdb_log_contexts"},
  std::string_view{"duckdb_logs"},
  std::string_view{"duckdb_memory"},
  std::string_view{"duckdb_optimizers"},
  std::string_view{"duckdb_prepared_statements"},
  std::string_view{"duckdb_schemas"},
  std::string_view{"duckdb_secret_types"},
  std::string_view{"duckdb_secrets"},
  std::string_view{"duckdb_sequences"},
  std::string_view{"duckdb_settings"},
  std::string_view{"duckdb_table_sample"},
  std::string_view{"duckdb_tables"},
  std::string_view{"duckdb_temporary_files"},
  std::string_view{"duckdb_triggers"},
  std::string_view{"duckdb_types"},
  std::string_view{"duckdb_variables"},
  std::string_view{"duckdb_views"},
  std::string_view{"truncate_duckdb_logs"},
};

constexpr std::array kScalarFunctions{std::string_view{"duckdb_format_sql"}};

constexpr std::array kViews{
  std::string_view{"duckdb_columns"},   std::string_view{"duckdb_constraints"},
  std::string_view{"duckdb_databases"}, std::string_view{"duckdb_indexes"},
  std::string_view{"duckdb_logs"},      std::string_view{"duckdb_schemas"},
  std::string_view{"duckdb_tables"},    std::string_view{"duckdb_types"},
  std::string_view{"duckdb_views"},
};

struct MacroAlias {
  std::string_view source;
  std::string_view parameter;
};

constexpr std::array kTableMacros{
  MacroAlias{"duckdb_logs_parsed", "log_type"},
  MacroAlias{"duckdb_profiling_settings", {}},
};

std::string AliasNameFor(std::string_view source) {
  const auto pos = source.find(kSourcePrefix);
  SDB_ASSERT(pos != std::string_view::npos);
  return absl::StrCat(source.substr(0, pos), kAliasPrefix,
                      source.substr(pos + kSourcePrefix.size()));
}

duckdb::Identifier AliasFor(std::string_view source) {
  return duckdb::Identifier{AliasNameFor(source)};
}

std::string QualifiedSource(std::string_view source) {
  return absl::StrCat(SYSTEM_CATALOG, ".", DEFAULT_SCHEMA, ".", source);
}

void AliasTableFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto name : kTableFunctions) {
    auto entry = loader.TryGetTableFunction(duckdb::Identifier{name});
    if (!entry) {
      continue;
    }
    auto& source = entry->Cast<duckdb::TableFunctionCatalogEntry>();
    auto functions = source.functions;
    functions.SetName(AliasFor(name));

    duckdb::CreateTableFunctionInfo info{std::move(functions)};
    info.descriptions = source.descriptions;
    info.alias_of = source.name;
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }
}

void AliasScalarFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto name : kScalarFunctions) {
    auto entry = loader.TryGetFunction(duckdb::Identifier{name});
    if (!entry) {
      continue;
    }
    auto& source = entry->Cast<duckdb::ScalarFunctionCatalogEntry>();
    auto functions = source.functions;
    functions.SetName(AliasFor(name));

    duckdb::CreateScalarFunctionInfo info{std::move(functions)};
    info.descriptions = source.descriptions;
    info.alias_of = source.name;
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }
}

void AliasTableMacros(duckdb::ExtensionLoader& loader,
                      duckdb::ParserOptions options) {
  for (const auto& macro : kTableMacros) {
    const std::string name = AliasNameFor(macro.source);
    const std::string parameter{macro.parameter};
    const std::string body = absl::StrCat(
      "SELECT * FROM ", QualifiedSource(macro.source), "(", parameter, ")");

    duckdb::DefaultTableMacro definition{
      DEFAULT_SCHEMA, name.c_str(), {}, {}, body.c_str()};
    if (!parameter.empty()) {
      definition.parameters[0] = parameter.c_str();
    }

    auto info = duckdb::DefaultTableFunctionGenerator::CreateTableMacroInfo(
      definition, options);
    info->on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    loader.RegisterFunction(*info);
  }
}

void AliasViews(duckdb::DatabaseInstance& db, duckdb::Parser& parser) {
  auto& system_catalog = duckdb::Catalog::GetSystemCatalog(db);
  auto transaction = duckdb::CatalogTransaction::GetSystemTransaction(db);
  for (const auto name : kViews) {
    auto info = duckdb::make_uniq<duckdb::CreateViewInfo>();
    info->SetSchema(duckdb::Identifier::DefaultSchema());
    info->SetViewName(AliasFor(name));
    info->sql = absl::StrCat("SELECT * FROM ", QualifiedSource(name));
    info->temporary = true;
    info->internal = true;
    info->on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

    parser.statements.clear();
    parser.ParseQuery(info->sql);
    SDB_ASSERT(parser.statements.size() == 1 &&
               parser.statements[0]->type ==
                 duckdb::StatementType::SELECT_STATEMENT);
    info->query =
      duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(
        std::move(parser.statements[0]));

    system_catalog.CreateView(transaction, *info);
  }
}

}  // namespace

void RegisterDuckDBAliases(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  AliasTableFunctions(loader);
  AliasScalarFunctions(loader);

  duckdb::ParserOptions parser_options;
  parser_options.parser_cache = &db.GetParserCache();
  duckdb::Parser parser{parser_options};
  AliasTableMacros(loader, parser_options);
  AliasViews(db, parser);
}

}  // namespace sdb::connector
