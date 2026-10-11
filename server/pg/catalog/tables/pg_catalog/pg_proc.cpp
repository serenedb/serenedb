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
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/lookup.h"
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

void FindNamed(const Builtins& rows, const SystemFilter& filter,
               std::vector<const BuiltinFunction*>& picked) {
  for (const auto& name : *filter.texts.keys) {
    for (const auto i : rows.Owner().Named(name)) {
      picked.emplace_back(&rows.Rows()[i]);
    }
  }
}

constexpr ArrayKey<BuiltinFunction, BuiltinFunctions> kBuiltinKeys[] = {
  {kPgProcSql["oid"], kBuiltinsByOid},
  {kPgProcSql["proname"], FindNamed},
};

constexpr SystemFact kBuiltinFacts[] = {
  {kPgProcSql["pronamespace"], kPgCatalogSchema, kPgCatalogSchema},
  {kPgProcSql["pronamespace"], kPgInformationSchema, kPgInformationSchema},
  {kPgProcSql["oid"], 0, kMaxSystem - 1},
};

bool ReturnsSet(const duckdb::MacroFunction& macro) {
  return !macro.is_procedure && macro.type == duckdb::MacroType::TABLE_MACRO;
}

bool ReturnsTable(const duckdb::MacroFunction& macro) {
  return ReturnsSet(macro) && !macro.return_names.empty();
}

duckdb::idx_t ArgumentOid(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::UNKNOWN ? 0 : Type2Oid(type);
}

auto AllArgTypes(const duckdb::MacroFunction& macro) {
  const auto inputs = macro.types.size();
  return std::views::iota(size_t{0}, inputs + macro.return_types.size()) |
         std::views::transform([&macro, inputs](size_t i) -> duckdb::idx_t {
           return i < inputs ? ArgumentOid(macro.types[i])
                             : Type2Oid(macro.return_types[i - inputs]);
         });
}

auto ArgModes(const duckdb::MacroFunction& macro) {
  const auto inputs = macro.types.size();
  return std::views::iota(size_t{0}, inputs + macro.return_types.size()) |
         std::views::transform(
           [inputs](size_t i) { return i < inputs ? 'i' : 't'; });
}

auto ArgNames(const duckdb::MacroFunction& macro) {
  const auto inputs = macro.parameters.size();
  const auto outputs = ReturnsTable(macro) ? macro.return_names.size() : 0;
  return std::views::iota(size_t{0}, inputs + outputs) |
         std::views::transform([&macro, inputs](size_t i) -> std::string_view {
           return i < inputs ? MacroParameterName(macro, i)
                             : std::string_view{macro.return_names[i - inputs]};
         });
}

struct Macro {
  const duckdb::MacroCatalogEntry& entry;
  const duckdb::MacroFunction& macro;
};

class PgProc final : public SystemTableScan<kPgProcSql> {
 public:
  PgProc(duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input)
    : SystemTableScan{context, input} {
    if (VisibleOnly()) {
      _session = MakeSession(&context);
    }
  }

  static constexpr std::tuple kSources{
    ArraySource<BuiltinFunction, BuiltinFunctions>{&LoadBuiltins, kBuiltinKeys,
                                                   kBuiltinFacts},
    CatalogSource{kTypes, SystemSchemas::Skip, kProcIndexes}};

  static constexpr SystemVisibility kVisibility{kSql["oid"],
                                                "pg_function_is_visible"};

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
      return ReturnsTable(row.macro) ? kRecord
                                     : Type2Oid(row.macro.return_types[0]);
    }),
    Col<"proargtypes">([](const auto& row) {
      return row.macro.types | std::views::transform(ArgumentOid);
    }),
    Col<"proallargtypes">([](const auto& row) {
      return ReturnsTable(row.macro) ? std::optional{AllArgTypes(row.macro)}
                                     : std::nullopt;
    }),
    Col<"proargmodes">([](const auto& row) {
      return ReturnsTable(row.macro) ? std::optional{ArgModes(row.macro)}
                                     : std::nullopt;
    }),
    Col<"proargnames">([](const auto& row) {
      auto names = ArgNames(row.macro);
      return absl::c_all_of(names,
                            [](std::string_view name) { return name.empty(); })
               ? std::nullopt
               : std::optional{names};
    }),
    Col<"prosrc">([](const auto& row) { return MacroBody(row.macro); }),
    Col<"proacl">([](const auto& row) -> const auto& {
      return row.entry.permissions.acl;
    }));

  void Row(const BuiltinFunction& function) {
    Emit<kBuiltin>(function, [&] { return Visible(function.oid); });
  }

  void Row(const duckdb::MacroCatalogEntry& entry) {
    for (const auto& macro : entry.macros) {
      Emit<kMacro>({entry, *macro}, [&] { return Visible(entry.oid); });
    }
  }

 private:
  bool Visible(duckdb::idx_t oid) const {
    return FunctionIsVisible(*_session, oid).value_or(false);
  }

  std::optional<Session> _session;
};

}  // namespace

SystemTable gPgProc = SystemTableOf<PgProc>();

}  // namespace sdb::pg
