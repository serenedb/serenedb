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

#include "pg/pg_catalog/pg_proc.h"

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <string>
#include <vector>

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "catalog1/entry/role.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgProc::proallargtypes),
  GetIndex(&PgProc::proargmodes),
  GetIndex(&PgProc::proargnames),
  GetIndex(&PgProc::proargdefaults),
  GetIndex(&PgProc::protrftypes),
  GetIndex(&PgProc::prosqlbody),
  GetIndex(&PgProc::proconfig),
});

constexpr Oid kLangSql = 14;

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgProc>::GetTableData() {
  std::vector<PgProc> values;
  std::vector<std::vector<Oid>> argtypes_storage;
  auto& context = _context;

  const auto emit = [&](const duckdb::MacroCatalogEntry& func) {
    const auto& perm = func.permissions;
    for (const auto& macro : func.macros) {
      PgProc::Prokind prokind = macro->is_procedure ? PgProc::Prokind::Procedure
                                                    : PgProc::Prokind::Function;

      // proretset: true if the function returns a set (TABLE_MACRO).
      bool proretset = macro->type == duckdb::MacroType::TABLE_MACRO;

      // prorettype: first return type (or 0 if not specified).
      Oid rettype = 0;
      if (!macro->return_types.empty()) {
        rettype = Type2Oid(macro->return_types[0], &context);
      }

      // Build argument types from macro->types (one per parameter).
      std::vector<Oid> argtypes;
      argtypes.reserve(macro->types.size());
      for (const auto& param_type : macro->types) {
        if (param_type.id() == duckdb::LogicalTypeId::UNKNOWN) {
          argtypes.push_back(0);
        } else {
          argtypes.push_back(Type2Oid(param_type, &context));
        }
      }

      auto pronargs = static_cast<int16_t>(argtypes.size());
      argtypes_storage.push_back(std::move(argtypes));
      values.push_back(PgProc{
        .oid = func.oid,
        .proname = func.name.GetIdentifierName(),
        .pronamespace = func.ParentSchema().oid,
        .proowner = perm.owner,
        .prolang = kLangSql,
        .procost = 0.0f,
        .prorows = 0.0f,
        .provariadic = 0,
        .prosupport = 0,
        .prokind = prokind,
        .prosecdef = false,
        .proleakproof = false,
        .proisstrict = false,
        .proretset = proretset,
        .provolatile = PgProc::Provolatile::Volatile,
        .proparallel = PgProc::Proparallel::Unsafe,
        .pronargs = pronargs,
        .pronargdefaults = 0,
        .prorettype = rettype,
        .proargtypes = argtypes_storage.back(),
        .prosrc = func.name.GetIdentifierName(),
        .proacl = {catalog::AclView{perm.acl}},
      });
    }
  };
  // A scalar macro and a table macro are one SereneDB kind and two duckdb
  // sets, so both are walked.
  VisitEntries<duckdb::ScalarMacroCatalogEntry>(context, GetDatabase(), emit);
  VisitEntries<duckdb::TableMacroCatalogEntry>(context, GetDatabase(), emit);

  auto result = CreateColumns<PgProc>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
