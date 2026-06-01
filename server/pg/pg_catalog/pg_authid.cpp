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

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "pg/pg_catalog/fwd.h"
#include "rest_server/serened.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAuthid::rolpassword),
  GetIndex(&PgAuthid::rolvaliduntil),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAuthid>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgAuthid> values;
  for (const auto& role : catalog->GetRoles()) {
    PgAuthid row{
      .oid = role->GetId().id(),
      .rolname = role->GetName(),
      .rolsuper = true,  // No RBAC yet, all roles are superusers
      .rolinherit = true,
      .rolcreaterole = true,
      .rolcreatedb = true,
      .rolcanlogin = role->isActive(),
      .rolreplication = true,
      .rolbypassrls = true,
      .rolconnlimit = -1,
    };
    values.push_back(std::move(row));
  }

  auto result = CreateColumns<PgAuthid>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
