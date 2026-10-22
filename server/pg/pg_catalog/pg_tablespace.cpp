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

#include "pg/pg_catalog/pg_tablespace.h"

#include <span>
#include <vector>

#include "catalog/cluster.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgTablespace::spcoptions),
});

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgTablespace>::GetTableData() {
  std::vector<PgTablespace> values;
  auto& cluster = catalog::ClusterOf(_context);
  cluster.GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
    .Scan(cluster.GetCatalogTransaction(_context),
          [&](duckdb::CatalogEntry& tablespace) {
            values.push_back(PgTablespace{
              .oid = tablespace.oid,
              .spcname = tablespace.name.GetIdentifierName(),
              .spcowner = tablespace.permissions.owner,
              .spcacl = {std::span<const duckdb::AclItem>{
                tablespace.permissions.acl}},
            });
          });
  auto result = CreateColumns<PgTablespace>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
