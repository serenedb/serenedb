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

#include "pg/pg_catalog/pg_database.h"

#include <deque>
#include <string>
#include <vector>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog1/cluster.h"
#include "catalog1/entry/database.h"
#include "catalog1/entry/role.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

// datacl excluded -> data-driven: WriteField renders an empty Array<Aclitem> as
// SQL NULL (PG's default-privileges representation) and a non-empty one as the
// aclitem[] array.
constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgDatabase::datlocale),
  GetIndex(&PgDatabase::daticurules),
  GetIndex(&PgDatabase::datcollversion),
});

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgDatabase>::GetTableData() {
  std::vector<PgDatabase> values;
  // The name and the ACL of every row are views into the entry the walk read
  // them off, which the rows written after it still point at: an entry version
  // stays in its set's chain for as long as a transaction can see it.
  auto& context = _context;
  auto& cluster = catalog::ClusterOf(context);
  cluster.ScanDatabases(
    cluster.GetCatalogTransaction(context), [&](duckdb::CatalogEntry& db) {
      values.push_back(PgDatabase{
        .oid = db.oid,
        .datname = db.name.GetIdentifierName(),
        .datdba = db.permissions.owner,
        .encoding = 6,  // UTF8
        .datlocprovider = PgDatabase::Datlocprovider::Libc,
        .datistemplate = false,
        .datallowconn = true,
        .dathasloginevt = false,
        .datconnlimit = -1,
        .datfrozenxid = 0,
        .datminmxid = 0,
        // pg_default; always 1663 (no CREATE TABLESPACE) and must be a real
        // pg_tablespace oid -- \l inner-joins pg_tablespace and would otherwise
        // drop the row. TODO: derive from a real tablespace once CREATE
        // TABLESPACE exists.
        .dattablespace = 1663,
        .datcollate = "C.UTF-8",
        .datctype = "C.UTF-8",
        .datacl = {catalog::AclView{db.permissions.acl}},
      });
    });

  auto result = CreateColumns<PgDatabase>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
