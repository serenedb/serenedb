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
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAuthid::rolpassword),
});

Timestamptz ValidUntilOf(const catalog::Role& role) {
  if (!role.HasValidUntil()) {
    return {};
  }
  return Timestamptz{.micros = role.ValidUntil(), .is_null = false};
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAuthid>::GetTableData() {
  std::vector<PgAuthid> values;
  auto catalog = _config.CatalogSnapshot();
  for (const auto& role : catalog->GetRoles()) {
    using catalog::RoleOption;
    PgAuthid row{
      .oid = role->GetId().id(),
      .rolname = role->GetName(),
      .rolsuper = role->Has(RoleOption::Superuser),
      .rolinherit = role->Has(RoleOption::Inherit),
      .rolcreaterole = role->Has(RoleOption::CreateRole),
      .rolcreatedb = role->Has(RoleOption::CreateDb),
      .rolcanlogin = role->CanLogin(),
      .rolreplication = role->Has(RoleOption::Replication),
      .rolbypassrls = role->Has(RoleOption::BypassRls),
      .rolconnlimit = role->ConnLimit(),
      .rolvaliduntil = ValidUntilOf(*role),
    };
    values.push_back(std::move(row));
  }

  auto result = CreateColumns<PgAuthid>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
