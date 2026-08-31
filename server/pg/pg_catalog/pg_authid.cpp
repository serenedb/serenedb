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

#include "pg/pg_catalog/pg_authid.h"

#include <duckdb/common/types/timestamp.hpp>

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog1/cluster.h"
#include "catalog1/entry/role.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAuthid::rolpassword),
});

Timestamptz ValidUntilOf(const catalog::RoleCatalogEntry& role) {
  if (!role.HasValidUntil()) {
    return {};
  }
  return Timestamptz{.micros = role.ValidUntil(), .is_null = false};
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgAuthid>::GetTableData() {
  std::vector<PgAuthid> values;
  auto& context = _config.GetClientContext();
  auto& cluster = catalog::ClusterOf(context);
  cluster.ScanRoles(
    cluster.GetCatalogTransaction(context), [&](duckdb::CatalogEntry& entry) {
      using catalog::RoleOption;
      const auto& role = entry.Cast<catalog::RoleCatalogEntry>();
      const auto options = role.Options();
      values.push_back(PgAuthid{
        .oid = role.oid,
        .rolname = role.name.GetIdentifierName(),
        .rolsuper = HasOption(options, RoleOption::Superuser),
        .rolinherit = HasOption(options, RoleOption::Inherit),
        .rolcreaterole = HasOption(options, RoleOption::CreateRole),
        .rolcreatedb = HasOption(options, RoleOption::CreateDb),
        .rolcanlogin = role.CanLogin(),
        .rolreplication = HasOption(options, RoleOption::Replication),
        .rolbypassrls = HasOption(options, RoleOption::BypassRls),
        .rolconnlimit = role.ConnLimit(),
        .rolvaliduntil = ValidUntilOf(role),
      });
    });

  auto result = CreateColumns<PgAuthid>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
