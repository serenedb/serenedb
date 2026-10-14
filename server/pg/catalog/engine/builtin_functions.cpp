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

#include "pg/catalog/engine/builtin_functions.h"

#include <absl/algorithm/container.h>
#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <ranges>
#include <tuple>
#include <vector>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/oids.h"
#include "pg/types.h"
#include "utils/file_utils.h"

ABSL_FLAG(std::string, dump_builtin_oids, "",
          "Write the identity of every builtin function and collation to "
          "this file and exit, without starting a listener. "
          "scripts/generate_pg_catalog.py --serened reads it to freeze their "
          "OIDs; see CONTRIBUTING.md.");

namespace sdb::pg {
namespace {

static_assert(kMaxSystem == duckdb::DatabaseManager::FIRST_OID);
static_assert(kFirstUnfrozenFunction <
              duckdb::DatabaseManager::FIRST_SYSTEM_OID);

constexpr FrozenOid kFrozenFunctions[] = {
#include "pg/catalog/generated/builtin_function_oids.gen.inc"
};

static_assert(absl::c_is_sorted(kFrozenFunctions,
                                [](const auto& lhs, const auto& rhs) {
                                  return lhs.key < rhs.key;
                                }));
static_assert(absl::c_all_of(kFrozenFunctions, [](const FrozenOid& row) {
  return row.oid < kMinSystem ||
         (row.oid >= kFirstBuiltinFunction && row.oid < kFirstUnfrozenFunction);
}));

duckdb::idx_t TypeOid(const duckdb::LogicalType& type) {
  return type.IsComplete() ? Type2Oid(type) : kUnknown;
}

std::vector<duckdb::idx_t> ArgTypes(
  const duckdb::FunctionSignature& signature) {
  return signature.GetParameters() |
         std::views::filter(
           [](const auto& parameter) { return !parameter.IsVariadic(); }) |
         std::views::transform(
           [](const auto& parameter) { return TypeOid(parameter.GetType()); }) |
         std::ranges::to<std::vector>();
}

char Volatility(duckdb::FunctionStability stability) {
  switch (stability) {
    case duckdb::FunctionStability::CONSISTENT:
      return 'i';
    case duckdb::FunctionStability::VOLATILE:
      return 'v';
    case duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY:
      return 's';
  }
}

bool Exact(const duckdb::LogicalType& type) {
  const auto oid = TypeOid(type);
  const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
  return oid != kUnknown && builtin && BuiltinLogicalType(*builtin) == type;
}

std::string Parameters(const duckdb::FunctionSignature& signature) {
  return absl::StrJoin(signature.GetParameters(), ", ",
                       [](std::string* out, const auto& parameter) {
                         absl::StrAppend(out, parameter.GetType().ToString(),
                                         parameter.IsVariadic() ? "..." : "");
                       });
}

bool Exact(const duckdb::FunctionSignature& signature) {
  return absl::c_all_of(signature.GetParameters(), [](const auto& parameter) {
    return !parameter.IsVariadic() && Exact(parameter.GetType());
  });
}

std::string Parameters(const duckdb::MacroFunction& macro) {
  std::string out;
  for (size_t i = 0; i < macro.parameters.size(); ++i) {
    absl::StrAppend(
      &out, i == 0 ? "" : ", ",
      i < macro.types.size()
        ? macro.types[i].ToString()
        : duckdb::LogicalType{duckdb::LogicalTypeId::UNKNOWN}.ToString());
  }
  return out;
}

bool Exact(const duckdb::MacroFunction& macro) {
  return macro.types.size() == macro.parameters.size() &&
         absl::c_all_of(macro.types, [](const duckdb::LogicalType& type) {
           return Exact(type);
         });
}

struct BuiltinSource {
  std::string_view schema;
  std::string_view name;
  duckdb::CatalogType type;
  const duckdb::CatalogEntry* entry;
  const duckdb::CreateMacroInfo* macro;
};

struct BuiltinOverload {
  std::string key;
  bool exact;
  BuiltinFunction function;
};

template<typename Visit>
void VisitBuiltinOverloads(duckdb::ClientContext& context, Visit&& visit) {
  std::vector<BuiltinSource> sources;
  duckdb::Catalog::GetSystemCatalog(context).ScanSchemas(
    context, [&](duckdb::SchemaCatalogEntry& schema) {
      const auto collect = [&](duckdb::CatalogEntry& entry) {
        sources.emplace_back(
          BuiltinSource{.schema = schema.name.GetIdentifierName(),
                        .name = entry.name.GetIdentifierName(),
                        .type = entry.type,
                        .entry = &entry,
                        .macro = nullptr});
      };
      schema.Scan(context, duckdb::CatalogType::SCALAR_FUNCTION_ENTRY, collect);
      schema.Scan(context, duckdb::CatalogType::TABLE_FUNCTION_ENTRY, collect);
      schema.Scan(context, duckdb::CatalogType::PRAGMA_FUNCTION_ENTRY, collect);
    });
  for (const auto& schema : kSystemNamespaces) {
    for (const auto kind :
         {duckdb::MacroType::SCALAR_MACRO, duckdb::MacroType::TABLE_MACRO}) {
      VisitSystemFunctions(
        schema.name, kind,
        [&](std::string_view name, const duckdb::CreateMacroInfo& info) {
          sources.emplace_back(
            BuiltinSource{.schema = schema.name,
                          .name = name,
                          .type = kind == duckdb::MacroType::SCALAR_MACRO
                                    ? duckdb::CatalogType::MACRO_ENTRY
                                    : duckdb::CatalogType::TABLE_MACRO_ENTRY,
                          .entry = nullptr,
                          .macro = &info});
        });
    }
  }
  absl::c_sort(sources, [](const BuiltinSource& lhs, const BuiltinSource& rhs) {
    return std::tie(lhs.schema, lhs.name, lhs.type) <
           std::tie(rhs.schema, rhs.name, rhs.type);
  });

  for (const auto& source : sources) {
    const auto emit = [&](std::string_view kind_name, std::string parameters,
                          bool exact, char kind, duckdb::idx_t rettype,
                          std::vector<duckdb::idx_t> argtypes, bool retset,
                          bool strict, char volatility) {
      visit(BuiltinOverload{
        .key =
          absl::StrCat(kind_name, " ", source.entry ? "system." : "",
                       source.schema, ".", source.name, "(", parameters, ")"),
        .exact = exact,
        .function = BuiltinFunction{
          .oid = kInvalidOid,
          .name = std::string{source.name},
          .nsp = source.schema == irs::StaticStrings::kInformationSchema
                   ? kPgInformationSchema
                   : kPgCatalogSchema,
          .kind = kind,
          .lang = kPgInternalLanguage,
          .rettype = retset ? kRecord : rettype,
          .argtypes = std::move(argtypes),
          .retset = retset,
          .strict = strict,
          .volatility = volatility,
          .src = std::string{source.name}}});
    };
    const auto emit_macros =
      [&](std::string_view kind_name,
          const duckdb::vector<duckdb::unique_ptr<duckdb::MacroFunction>>&
            macros) {
        for (const auto& macro : macros) {
          emit(kind_name, Parameters(*macro), Exact(*macro), 'f',
               macro->return_types.empty() ? kUnknown
                                           : TypeOid(macro->return_types[0]),
               macro->types | std::views::transform(TypeOid) |
                 std::ranges::to<std::vector>(),
               macro->type == duckdb::MacroType::TABLE_MACRO, false, 'i');
        }
      };
    const auto emit_signatures = [&](std::string_view kind_name, char kind,
                                     const auto& entry) {
      for (const auto& function : entry.functions.functions) {
        const auto& signature = function->GetSignature();
        emit(
          kind_name, Parameters(signature), Exact(signature), kind,
          TypeOid(function->GetReturnType()), ArgTypes(signature), false,
          kind == 'f' && function->GetNullHandling() ==
                           duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
          Volatility(function->GetStability()));
      }
    };
    const auto emit_arguments = [&](std::string_view kind_name, bool retset,
                                    const auto& entry) {
      for (const auto& function : entry.functions.functions) {
        const auto& signature = function->GetSignature();
        emit(kind_name, Parameters(signature), Exact(signature), 'f', kUnknown,
             ArgTypes(signature), retset, false, 'i');
      }
    };
    using enum duckdb::CatalogType;
    const auto macro_kind =
      source.type == MACRO_ENTRY ? "macro" : "table_macro";
    if (source.macro) {
      emit_macros(macro_kind, source.macro->macros);
      continue;
    }
    const auto& entry = *source.entry;
    if (entry.type == SCALAR_FUNCTION_ENTRY) {
      emit_signatures("scalar", 'f',
                      entry.Cast<duckdb::ScalarFunctionCatalogEntry>());
    } else if (entry.type == AGGREGATE_FUNCTION_ENTRY) {
      emit_signatures("aggregate", 'a',
                      entry.Cast<duckdb::AggregateFunctionCatalogEntry>());
    } else if (entry.type == WINDOW_FUNCTION_ENTRY) {
      emit_signatures("window", 'w',
                      entry.Cast<duckdb::WindowFunctionCatalogEntry>());
    } else if (entry.type == TABLE_FUNCTION_ENTRY) {
      emit_arguments("table", true,
                     entry.Cast<duckdb::TableFunctionCatalogEntry>());
    } else if (entry.type == PRAGMA_FUNCTION_ENTRY) {
      emit_arguments("pragma", false,
                     entry.Cast<duckdb::PragmaFunctionCatalogEntry>());
    } else if (entry.type == MACRO_ENTRY || entry.type == TABLE_MACRO_ENTRY) {
      emit_macros(macro_kind, entry.Cast<duckdb::MacroCatalogEntry>().macros);
    }
  }
}

std::vector<BuiltinFunction> CollectBuiltinFunctions(
  duckdb::ClientContext& context) {
  std::vector<BuiltinFunction> functions;
  for (const auto& proc : BuiltinProcs()) {
    functions.emplace_back(BuiltinFunction{
      .oid = static_cast<duckdb::idx_t>(proc.oid),
      .name = std::string{proc.name},
      .nsp = kPgCatalogSchema,
      .kind = proc.kind,
      .lang = static_cast<duckdb::idx_t>(proc.lang),
      .rettype = static_cast<duckdb::idx_t>(proc.rettype),
      .argtypes = {proc.argtypes.begin(), proc.argtypes.begin() + proc.nargs},
      .retset = proc.retset,
      .strict = proc.strict,
      .volatility = proc.volatility,
      .src = std::string{proc.src}});
  }
  auto unfrozen = kFirstUnfrozenFunction;
  VisitBuiltinOverloads(context, [&](BuiltinOverload&& overload) {
    const auto frozen = FindFrozenOid(kFrozenFunctions, overload.key);
    overload.function.oid = frozen != kInvalidOid ? frozen : unfrozen++;
    functions.emplace_back(std::move(overload.function));
  });
  SDB_ASSERT(unfrozen <= duckdb::DatabaseManager::FIRST_SYSTEM_OID);
  absl::c_sort(functions,
               [](const BuiltinFunction& lhs, const BuiltinFunction& rhs) {
                 return lhs.oid < rhs.oid;
               });
  return functions;
}

absl::Mutex gBuiltinsLock;
std::shared_ptr<const BuiltinFunctions> gBuiltins
  ABSL_GUARDED_BY(gBuiltinsLock);

}  // namespace

std::shared_ptr<const BuiltinFunctions> GetBuiltinFunctions(
  duckdb::ClientContext& context) {
  const auto system_version =
    duckdb::Catalog::GetSystemCatalog(context).GetCatalogVersion(context);
  const auto version =
    system_version.IsValid() ? system_version.GetIndex() : duckdb::idx_t{0};
  {
    absl::ReaderMutexLock guard{&gBuiltinsLock};
    if (gBuiltins && gBuiltins->_version == version) {
      return gBuiltins;
    }
  }
  auto built = std::make_shared<BuiltinFunctions>();
  built->_version = version;
  built->_functions = CollectBuiltinFunctions(context);
  for (uint32_t i = 0; i < built->_functions.size(); ++i) {
    built->_by_name[built->_functions[i].name].emplace_back(i);
  }
  absl::MutexLock guard{&gBuiltinsLock};
  gBuiltins = std::move(built);
  return gBuiltins;
}

std::optional<int> RunBuiltinOidsDump() {
  const auto path = absl::GetFlag(FLAGS_dump_builtin_oids);
  if (path.empty()) {
    return std::nullopt;
  }
  std::string out;
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.RunFunctionInTransaction([&] {
    VisitBuiltinOverloads(context, [&](const BuiltinOverload& overload) {
      const auto& function = overload.function;
      absl::StrAppend(&out, "function\t", overload.key, "\t", function.nsp,
                      "\t", function.name, "\t",
                      std::string_view{&function.kind, 1}, "\t",
                      absl::StrJoin(function.argtypes, " "), "\t",
                      overload.exact ? "t" : "f", "\n");
    });
  });
  for (const auto& collation : BuiltinCollations()) {
    if (collation.oid >= kFirstCollation) {
      absl::StrAppend(&out, "collation\t", collation.name, "\n");
    }
  }
  try {
    utils::file_utils::Spit(path, out);
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot write '", path, "': ", e.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

}  // namespace sdb::pg
