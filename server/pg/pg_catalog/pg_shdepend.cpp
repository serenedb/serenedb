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

#include "pg/pg_catalog/pg_shdepend.h"

#include <vector>

#include "pg/pg_catalog/fwd.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/role_dependencies.h"

namespace sdb::pg {

template<>
MaterializedData SystemTableSnapshot<PgShdepend>::GetTableData() {
  std::vector<PgShdepend> values;
  VisitRoleDependencies(_context, [&](const RoleDependency& dependency) {
    values.push_back(PgShdepend{
      .dbid = dependency.database,
      .classid = dependency.classid,
      .objid = dependency.objid,
      .objsubid = dependency.objsubid,
      .refclassid = PgAuthid::kId,
      .refobjid = dependency.role,
      .deptype = static_cast<PgShdepend::Deptype>(dependency.deptype),
    });
  });

  auto result = CreateColumns<PgShdepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
