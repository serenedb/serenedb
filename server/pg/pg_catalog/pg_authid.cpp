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
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAuthid::rolpassword),
  GetIndex(&PgAuthid::rolvaliduntil),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAuthid>::GetTableData() {
  // RBAC/roles were removed; pg_authid (and pg_roles / \du layered on it) keeps
  // working off one static superuser row for the default login role.
  std::vector<PgAuthid> values;
  values.push_back(PgAuthid{
    .oid = id::kRootUser.id(),
    .rolname = StaticStrings::kDefaultUser,
    .rolsuper = true,
    .rolinherit = true,
    .rolcreaterole = true,
    .rolcreatedb = true,
    .rolcanlogin = true,
    .rolreplication = true,
    .rolbypassrls = true,
    .rolconnlimit = -1,
  });

  auto result = CreateColumns<PgAuthid>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
