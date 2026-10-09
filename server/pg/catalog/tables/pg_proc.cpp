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

#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/function/macro_function.hpp>
#include <ranges>

#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/tables/tables.h"
#include "pg/sql_utils.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {
  duckdb::CatalogType::MACRO_ENTRY,
  duckdb::CatalogType::TABLE_MACRO_ENTRY,
};

constexpr SystemKindTypes kProkinds[] = {
  {'f', kTypes},
  {'p', kTypes},
};

constexpr SystemIndex kProcIndexes[] = {
  {kPgProcSql["oid"], SystemLookup::Object},
  {kPgProcSql["proname"], SystemLookup::Object},
  {kPgProcSql["pronamespace"], SystemLookup::Namespace},
  {kPgProcSql["prokind"], SystemLookup::Kind, kProkinds},
};

using Builtins = SystemRows<BuiltinFunction, BuiltinFunctions>;

Builtins LoadBuiltins(SystemScan& scan) {
  auto functions = GetBuiltinFunctions(scan.Context());
  return {functions->All(), std::move(functions)};
}

void FindNamed(const Builtins& rows, const SystemFilter& filter,
               std::vector<const BuiltinFunction*>& picked) {
  for (const auto& name : *filter.texts.keys) {
    for (const auto i : rows.Owner().Named(name)) {
      picked.emplace_back(&rows.Rows()[i]);
    }
  }
}

constexpr auto kByOid =
  SortedBy<BuiltinFunction, &BuiltinFunction::oid, BuiltinFunctions>;

constexpr ArrayKey<BuiltinFunction, BuiltinFunctions> kBuiltinKeys[] = {
  {kPgProcSql["oid"], kByOid},
  {kPgProcSql["proname"], FindNamed},
};

constexpr ArrayKey<BuiltinFunction, BuiltinFunctions> kAggregateKeys[] = {
  {kPgAggregateSql["aggfnoid"], kByOid},
};

bool ReturnsSet(const duckdb::MacroFunction& macro) {
  return !macro.is_procedure && macro.type == duckdb::MacroType::TABLE_MACRO;
}

struct Macro {
  const duckdb::MacroCatalogEntry& entry;
  const duckdb::MacroFunction& macro;
};

class PgProc final : public SystemTableScan<kPgProcSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinFunction, BuiltinFunctions>{&LoadBuiltins, kBuiltinKeys},
    CatalogSource{kTypes, SystemSchemas::Skip, kProcIndexes}};

  static constexpr auto kBuiltin = Shape<kSql, const BuiltinFunction>(
    Col<"oid">(&BuiltinFunction::oid), Col<"proname">(&BuiltinFunction::name),
    Col<"pronamespace">(&BuiltinFunction::nsp),
    Col<"proowner">([](const auto&) { return kRootUser; }),
    Col<"prolang">(&BuiltinFunction::lang),
    Col<"prorows">(
      [](const auto& function) { return function.retset ? 1000.0F : 0.0F; }),
    Col<"prokind">(&BuiltinFunction::kind),
    Col<"proisstrict">(&BuiltinFunction::strict),
    Col<"proretset">(&BuiltinFunction::retset),
    Col<"provolatile">(&BuiltinFunction::volatility),
    Col<"pronargs">(
      [](const auto& function) { return function.argtypes.size(); }),
    Col<"prorettype">(&BuiltinFunction::rettype),
    Col<"proargtypes">(&BuiltinFunction::argtypes),
    Col<"prosrc">(&BuiltinFunction::src));

  static constexpr auto kMacro = Shape<kSql, const Macro>(
    Col<"oid">([](const auto& row) { return row.entry.oid; }),
    Col<"proname">(
      [](const auto& row) -> const auto& { return row.entry.name; }),
    Col<"pronamespace">(
      [](const auto& row) { return row.entry.ParentSchemaOid(); }),
    Col<"proowner">(
      [](const auto& row) { return row.entry.permissions.owner; }),
    Col<"prolang">([](const auto&) { return kPgSqlLanguage; }),
    Col<"procost">([](const auto&) { return 100.0F; }),
    Col<"prokind">(
      [](const auto& row) { return row.macro.is_procedure ? 'p' : 'f'; }),
    Col<"proisstrict">([](const auto&) { return false; }),
    Col<"proretset">([](const auto& row) { return ReturnsSet(row.macro); }),
    Col<"provolatile">([](const auto&) { return 'v'; }),
    Col<"proparallel">([](const auto&) { return 'u'; }),
    Col<"pronargs">([](const auto& row) { return row.macro.types.size(); }),
    Col<"prorettype">([](const auto& row) -> duckdb::idx_t {
      if (row.macro.is_procedure) {
        return kVoid;
      }
      if (row.macro.return_types.empty()) {
        return kInvalidOid;
      }
      return ReturnsSet(row.macro) && !row.macro.return_names.empty()
               ? kRecord
               : Type2Oid(row.macro.return_types[0]);
    }),
    Col<"proargtypes">([](const auto& row) {
      return row.macro.types |
             std::views::transform([](const duckdb::LogicalType& type) {
               return type.id() == duckdb::LogicalTypeId::UNKNOWN
                        ? 0
                        : Type2Oid(type);
             });
    }),
    Col<"proargnames">(
      [](const auto& row) -> std::optional<std::vector<std::string>> {
        std::vector<std::string> names;
        for (duckdb::idx_t i = 0; i < row.macro.parameters.size(); ++i) {
          names.emplace_back(MacroParameterName(row.macro, i));
        }
        if (absl::c_all_of(names,
                           [](const auto& name) { return name.empty(); })) {
          return std::nullopt;
        }
        return names;
      }),
    Col<"prosrc">([](const auto& row) { return MacroBody(row.macro); }),
    Col<"proacl">([](const auto& row) -> const auto& {
      return row.entry.permissions.acl;
    }));

  void Row(const BuiltinFunction& function) { Emit<kBuiltin>(function); }

  void Row(const duckdb::MacroCatalogEntry& entry) {
    for (const auto& macro : entry.macros) {
      Emit<kMacro>({entry, *macro});
    }
  }
};

class PgAggregate final : public SystemTableScan<kPgAggregateSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinFunction, BuiltinFunctions>{&LoadBuiltins,
                                                   kAggregateKeys}};

  static constexpr auto kAggregate = Shape<kSql, const BuiltinFunction>(
    Col<"aggfnoid">(&BuiltinFunction::oid),
    Col<"aggtranstype">([](const auto&) { return kInternal; }));

  void Row(const BuiltinFunction& function) {
    if (function.kind == 'a') {
      Emit<kAggregate>(function);
    }
  }
};

}  // namespace

SystemTable gPgProc = SystemTableOf<PgProc>();

SystemTable gPgAggregate = SystemTableOf<PgAggregate>();

}  // namespace sdb::pg
