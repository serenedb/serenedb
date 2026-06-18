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

#include "pg/pg_catalog/pg_default_acl.h"

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/role.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgDefaultAcl>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  auto roles = catalog->GetRoles();
  std::vector<PgDefaultAcl> values;
  uint64_t oid = 1;
  for (const auto& role : roles) {
    for (const auto& entry : role->DefaultAcls()) {
      // defaclnamespace 0 == all schemas (the schema-less form).
      const uint64_t ns = entry.schema.isSet() ? entry.schema.id() : 0;
      values.push_back(PgDefaultAcl{
        .oid = oid++,
        .defaclrole = role->GetId().id(),
        .defaclnamespace = ns,
        .defaclobjtype =
          static_cast<PgDefaultAcl::Defaclobjtype>(entry.objtype),
        .defaclacl = {entry.acl},
      });
    }
  }

  auto result = CreateColumns<PgDefaultAcl>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
