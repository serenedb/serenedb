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
#include <absl/synchronization/mutex.h>

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
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <ranges>
#include <tuple>
#include <vector>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/oids.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

static_assert(kMaxSystem == duckdb::DatabaseManager::FIRST_OID);

bool TypeIsComplete(const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  const auto id = type.id();
  if (id == LIST) {
    return type.HasParameters() &&
           TypeIsComplete(duckdb::ListType::GetChildType(type));
  }
  if (id == ARRAY) {
    return type.HasParameters() &&
           TypeIsComplete(duckdb::ArrayType::GetChildType(type));
  }
  if (id == DECIMAL || id == STRUCT || id == TUPLE || id == MAP ||
      id == UNION || id == ENUM) {
    return type.HasParameters();
  }
  return true;
}

duckdb::idx_t TypeOid(const duckdb::LogicalType& type) {
  return TypeIsComplete(type) ? Type2Oid(type) : kUnknown;
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

char Kind(duckdb::CatalogType type) {
  if (type == duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY) {
    return 'a';
  }
  if (type == duckdb::CatalogType::WINDOW_FUNCTION_ENTRY) {
    return 'w';
  }
  return 'f';
}

struct BuiltinSource {
  std::string schema;
  std::string_view name;
  duckdb::CatalogType type;
  const duckdb::CatalogEntry* entry;
  const duckdb::CreateMacroInfo* macro;
};

std::vector<BuiltinFunction> CollectBuiltinFunctions(
  duckdb::ClientContext& context) {
  std::vector<BuiltinSource> sources;
  const auto collect = [&sources](duckdb::CatalogEntry& entry) {
    sources.emplace_back(
      BuiltinSource{.schema = entry.ParentSchemaName().GetIdentifierName(),
                    .name = entry.name.GetIdentifierName(),
                    .type = entry.type,
                    .entry = &entry,
                    .macro = nullptr});
  };
  duckdb::Catalog::GetSystemCatalog(context).ScanSchemas(
    context, [&](duckdb::SchemaCatalogEntry& schema) {
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
            BuiltinSource{.schema = std::string{schema.name},
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
  const auto first = functions.size();
  for (const auto& source : sources) {
    const auto emit = [&](char kind, duckdb::idx_t rettype,
                          std::vector<duckdb::idx_t> argtypes, bool retset,
                          bool strict, char volatility) {
      functions.emplace_back(BuiltinFunction{
        .oid = kFirstBuiltinFunction + functions.size() - first,
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
        .src = std::string{source.name}});
    };
    const auto emit_macros =
      [&](const duckdb::vector<duckdb::unique_ptr<duckdb::MacroFunction>>&
            macros) {
        for (const auto& macro : macros) {
          emit('f',
               macro->return_types.empty() ? kUnknown
                                           : TypeOid(macro->return_types[0]),
               macro->types | std::views::transform(TypeOid) |
                 std::ranges::to<std::vector>(),
               macro->type == duckdb::MacroType::TABLE_MACRO, false, 'i');
        }
      };
    const auto emit_signatures = [&](const auto& entry) {
      for (const auto& function : entry.functions.functions) {
        emit(Kind(entry.type), TypeOid(function->GetReturnType()),
             ArgTypes(function->GetSignature()), false,
             entry.type == duckdb::CatalogType::SCALAR_FUNCTION_ENTRY &&
               function->GetNullHandling() ==
                 duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
             Volatility(function->GetStability()));
      }
    };
    const auto emit_arguments = [&](const auto& entry) {
      for (const auto& function : entry.functions.functions) {
        emit('f', kUnknown, ArgTypes(function->GetSignature()),
             entry.type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY, false,
             'i');
      }
    };
    if (source.macro) {
      emit_macros(source.macro->macros);
      continue;
    }
    const auto& entry = *source.entry;
    using enum duckdb::CatalogType;
    if (entry.type == SCALAR_FUNCTION_ENTRY) {
      emit_signatures(entry.Cast<duckdb::ScalarFunctionCatalogEntry>());
    } else if (entry.type == AGGREGATE_FUNCTION_ENTRY) {
      emit_signatures(entry.Cast<duckdb::AggregateFunctionCatalogEntry>());
    } else if (entry.type == WINDOW_FUNCTION_ENTRY) {
      emit_signatures(entry.Cast<duckdb::WindowFunctionCatalogEntry>());
    } else if (entry.type == TABLE_FUNCTION_ENTRY) {
      emit_arguments(entry.Cast<duckdb::TableFunctionCatalogEntry>());
    } else if (entry.type == PRAGMA_FUNCTION_ENTRY) {
      emit_arguments(entry.Cast<duckdb::PragmaFunctionCatalogEntry>());
    } else if (entry.type == MACRO_ENTRY || entry.type == TABLE_MACRO_ENTRY) {
      emit_macros(entry.Cast<duckdb::MacroCatalogEntry>().macros);
    }
  }
  SDB_ASSERT(kFirstBuiltinFunction + functions.size() - first <= kMaxSystem);
  return functions;
}

absl::Mutex gBuiltinsLock;
std::shared_ptr<const BuiltinFunctions> gBuiltins
  ABSL_GUARDED_BY(gBuiltinsLock);

}  // namespace

const BuiltinFunction* BuiltinFunctions::Find(duckdb::idx_t oid) const {
  const auto it = absl::c_lower_bound(
    _functions, oid, [](const BuiltinFunction& function, duckdb::idx_t key) {
      return function.oid < key;
    });
  return it != _functions.end() && it->oid == oid ? &*it : nullptr;
}

std::span<const uint32_t> BuiltinFunctions::Named(std::string_view name) const {
  const auto it = _by_name.find(name);
  return it == _by_name.end() ? std::span<const uint32_t>{} : it->second;
}

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

}  // namespace sdb::pg
