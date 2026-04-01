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

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/schema.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"
#include "rest_server/serened.h"

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
  GetIndex(&PgProc::proacl),
});

constexpr Oid kLangInternal = 12;
constexpr Oid kLangSql = 14;

}  // namespace

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgProc>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgProc> values;
  std::vector<std::vector<Oid>> argtypes_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    auto functions = catalog->GetFunctions(GetDatabaseId(), schema->GetName());
    for (const auto& func : functions) {
      auto& sig = func->Signature();
      auto& opts = func->Options();

      // Map language
      Oid prolang = kLangInternal;
      if (opts.language == catalog::FunctionLanguage::SQL) {
        prolang = kLangSql;
      }

      // Map kind
      PgProc::Prokind prokind = PgProc::Prokind::Function;
      if (sig.IsProcedure()) {
        prokind = PgProc::Prokind::Procedure;
      } else if (opts.kind == catalog::FunctionKind::Aggregate) {
        prokind = PgProc::Prokind::Aggregate;
      } else if (opts.kind == catalog::FunctionKind::Window) {
        prokind = PgProc::Prokind::Window;
      }

      // Map return type
      Oid rettype = 0;
      if (sig.return_type) {
        rettype = Type2Oid(sig.return_type);
      }

      // Build argument types
      std::vector<Oid> argtypes;
      int16_t pronargs = 0;
      for (const auto& param : sig.parameters) {
        if (param.mode == catalog::FunctionParameter::Mode::In ||
            param.mode == catalog::FunctionParameter::Mode::InOut ||
            param.mode == catalog::FunctionParameter::Mode::Variadic) {
          if (param.type) {
            argtypes.push_back(Type2Oid(param.type));
          } else {
            argtypes.push_back(0);
          }
          ++pronargs;
        }
      }

      argtypes_storage.push_back(std::move(argtypes));
      auto owner = func->GetOwnerId().id();
      if (!owner) {
        owner = id::kRootUser.id();
      }
      values.push_back({
        .oid = func->GetId().id(),
        .proname = func->GetName(),
        .pronamespace = schema->GetId().id(),
        .proowner = owner,
        .prolang = prolang,
        .procost = static_cast<float>(opts.cost),
        .prorows = static_cast<float>(opts.rows),
        .provariadic = 0,
        .prosupport = 0,
        .prokind = prokind,
        .prosecdef = opts.security,
        .proleakproof = false,
        .proisstrict = opts.strict,
        .proretset = opts.table,
        .provolatile = opts.state == catalog::FunctionState::Immutable
                         ? PgProc::Provolatile::Immutable
                       : opts.state == catalog::FunctionState::Stable
                         ? PgProc::Provolatile::Stable
                         : PgProc::Provolatile::Volatile,
        .proparallel = opts.parallel == catalog::FunctionParallel::Safe
                         ? PgProc::Proparallel::Safe
                       : opts.parallel == catalog::FunctionParallel::Restricted
                         ? PgProc::Proparallel::Restricted
                         : PgProc::Proparallel::Unsafe,
        .pronargs = pronargs,
        .pronargdefaults = 0,
        .prorettype = rettype,
        .proargtypes = argtypes_storage.back(),
        .prosrc = func->GetName(),
      });
    }
  }

  auto result = CreateColumns<PgProc>(values, &pool);

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
