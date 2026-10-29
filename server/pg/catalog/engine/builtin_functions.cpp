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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>

#include <bitset>
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
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <tuple>
#include <vector>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/oids.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

static_assert(kMaxSystem == duckdb::DatabaseManager::FIRST_OID);
static_assert(kFirstBuiltinFunction <
              duckdb::DatabaseManager::FIRST_SYSTEM_OID);

struct PgProcIdentity {
  duckdb::idx_t nsp;
  std::string_view name;
  char kind;
  std::string_view argtypes;
  duckdb::idx_t oid;
};

constexpr PgProcIdentity kPgProcIdentities[] = {
#include "pg/catalog/generated/pg_proc_identities.gen.inc"
};

constexpr auto Identity(const PgProcIdentity& row) {
  return std::tie(row.nsp, row.name, row.kind, row.argtypes);
}

static_assert(absl::c_is_sorted(kPgProcIdentities,
                                [](const auto& lhs, const auto& rhs) {
                                  return Identity(lhs) < Identity(rhs);
                                }));
static_assert(absl::c_all_of(kPgProcIdentities, [](const auto& row) {
  return row.oid < kMinSystem;
}));

duckdb::idx_t PgProcOid(const BuiltinFunction& function) {
  const auto argtypes = absl::StrJoin(function.argtypes, " ");
  const auto identity =
    std::tuple{function.nsp, std::string_view{function.name}, function.kind,
               std::string_view{argtypes}};
  const auto it =
    absl::c_lower_bound(kPgProcIdentities, identity,
                        [](const PgProcIdentity& row, const auto& identity) {
                          return Identity(row) < identity;
                        });
  return it != std::end(kPgProcIdentities) && Identity(*it) == identity
           ? it->oid
           : kInvalidOid;
}

duckdb::idx_t TypeOid(const duckdb::LogicalType& type) {
  return type.IsComplete() ? CatalogTypeOid(type) : kUnknown;
}

std::vector<duckdb::idx_t> ArgTypes(
  const duckdb::FunctionSignature& signature) {
  std::vector<duckdb::idx_t> types;
  for (const auto& parameter : signature.GetParameters()) {
    if (!parameter.IsVariadic()) {
      types.emplace_back(TypeOid(parameter.GetType()));
    }
  }
  return types;
}

std::vector<duckdb::idx_t> ArgTypes(const duckdb::MacroFunction& macro) {
  std::vector<duckdb::idx_t> types;
  types.reserve(macro.types.size());
  for (const auto& type : macro.types) {
    types.emplace_back(TypeOid(type));
  }
  return types;
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
  if (type.id() == duckdb::LogicalTypeId::LIST &&
      duckdb::ListType::GetChildType(type).id() ==
        duckdb::LogicalTypeId::LIST) {
    return Exact(duckdb::ListType::GetChildType(type));
  }
  const auto oid = TypeOid(type);
  const auto* builtin = FindBuiltinType(oid);
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

struct BuiltinOverload {
  std::string key;
  bool exact;
  BuiltinFunction function;
};

template<typename Visit>
void VisitBuiltinOverloads(duckdb::ClientContext& context, Visit&& visit) {
  const auto emit_source = [&](std::string_view schema, std::string_view name,
                               const duckdb::CatalogEntry* system_entry,
                               const duckdb::CreateMacroInfo* macro,
                               std::string_view macro_kind) {
    const auto emit = [&](std::string_view kind_name, std::string parameters,
                          bool exact, char kind, duckdb::idx_t rettype,
                          std::vector<duckdb::idx_t> argtypes, bool retset,
                          bool strict, char volatility) {
      visit(BuiltinOverload{
        .key = absl::StrCat(kind_name, " ", system_entry ? "system." : "",
                            schema, ".", name, "(", parameters, ")"),
        .exact = exact,
        .function = BuiltinFunction{
          .oid = kInvalidOid,
          .name = std::string{name},
          .nsp = schema == irs::StaticStrings::kInformationSchema
                   ? kPgInformationSchema
                   : kPgCatalogSchema,
          .kind = kind,
          .lang = kPgInternalLanguage,
          .rettype = retset ? kRecord : rettype,
          .argtypes = std::move(argtypes),
          .retset = retset,
          .strict = strict,
          .volatility = volatility}});
    };
    const auto emit_macros =
      [&](std::string_view kind_name,
          const duckdb::vector<duckdb::unique_ptr<duckdb::MacroFunction>>&
            macros) {
        for (const auto& macro : macros) {
          emit(kind_name, Parameters(*macro), Exact(*macro), 'f',
               macro->return_types.empty() ? kUnknown
                                           : TypeOid(macro->return_types[0]),
               ArgTypes(*macro), macro->type == duckdb::MacroType::TABLE_MACRO,
               false, 'i');
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
    if (macro) {
      emit_macros(macro_kind, macro->macros);
      return;
    }
    using enum duckdb::CatalogType;
    const auto& entry = *system_entry;
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
      emit_macros(entry.type == MACRO_ENTRY ? "macro" : "table_macro",
                  entry.Cast<duckdb::MacroCatalogEntry>().macros);
    }
  };
  duckdb::Catalog::GetSystemCatalog(context).ScanSchemas(
    context, [&](duckdb::SchemaCatalogEntry& schema) {
      const auto collect = [&](duckdb::CatalogEntry& entry) {
        emit_source(schema.name.GetIdentifierName(),
                    entry.name.GetIdentifierName(), &entry, nullptr, {});
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
          emit_source(
            schema.name, name, nullptr, &info,
            kind == duckdb::MacroType::SCALAR_MACRO ? "macro" : "table_macro");
        });
    }
  }
}

std::vector<BuiltinFunction> CollectBuiltinFunctions(
  duckdb::ClientContext& context) {
  std::vector<BuiltinFunction> functions;
  for (const auto& proc : BuiltinProcs()) {
    functions.emplace_back(
      static_cast<duckdb::idx_t>(proc.oid), std::string{proc.name},
      kPgCatalogSchema, proc.kind, static_cast<duckdb::idx_t>(proc.lang),
      static_cast<duckdb::idx_t>(proc.rettype),
      std::vector<duckdb::idx_t>{proc.argtypes.begin(),
                                 proc.argtypes.begin() + proc.nargs},
      proc.retset, proc.strict, proc.volatility, proc.src);
  }
  std::vector<BuiltinOverload> overloads;
  VisitBuiltinOverloads(context, [&](BuiltinOverload&& overload) {
    overloads.emplace_back(std::move(overload));
  });
  absl::c_stable_sort(
    overloads, [](const BuiltinOverload& lhs, const BuiltinOverload& rhs) {
      return lhs.key < rhs.key;
    });
  std::bitset<kMinSystem> matched;
  auto next = kFirstBuiltinFunction;
  for (auto& overload : overloads) {
    const auto pg = overload.exact ? PgProcOid(overload.function) : kInvalidOid;
    if (pg != kInvalidOid && !matched.test(pg)) {
      matched.set(pg);
      overload.function.oid = pg;
    } else {
      overload.function.oid = next++;
    }
    functions.emplace_back(std::move(overload.function));
  }
  SDB_ASSERT(next <= duckdb::DatabaseManager::FIRST_SYSTEM_OID);
  absl::c_sort(functions, kByOid);
  SDB_ASSERT(absl::c_adjacent_find(functions, [](const BuiltinFunction& lhs,
                                                 const BuiltinFunction& rhs) {
               return lhs.oid == rhs.oid;
             }) == functions.end());
  return functions;
}

absl::Mutex gBuiltinsLock;
std::shared_ptr<const BuiltinFunctions> gBuiltins
  ABSL_GUARDED_BY(gBuiltinsLock);

}  // namespace

BuiltinFunctions::BuiltinFunctions(duckdb::idx_t version,
                                   std::vector<BuiltinFunction> functions)
  : _version{version}, _functions{std::move(functions)} {
  for (uint32_t i = 0; i < _functions.size(); ++i) {
    _by_name[_functions[i].name].emplace_back(i);
  }
}

std::shared_ptr<const BuiltinFunctions> GetBuiltinFunctions(
  duckdb::ClientContext& context) {
  const auto version = duckdb::Catalog::GetSystemCatalog(context)
                         .GetCatalogVersion(context)
                         .GetIndex();
  {
    absl::ReaderMutexLock guard{&gBuiltinsLock};
    if (gBuiltins && gBuiltins->Version() == version) {
      return gBuiltins;
    }
  }
  auto built = std::make_shared<const BuiltinFunctions>(
    version, CollectBuiltinFunctions(context));
  absl::MutexLock guard{&gBuiltinsLock};
  gBuiltins = std::move(built);
  return gBuiltins;
}

}  // namespace sdb::pg
