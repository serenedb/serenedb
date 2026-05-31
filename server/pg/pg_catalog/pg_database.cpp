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

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgDatabase::datlocale),
  GetIndex(&PgDatabase::daticurules),
  GetIndex(&PgDatabase::datcollversion),
  GetIndex(&PgDatabase::datacl),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgDatabase>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgDatabase> values;
  for (const auto& db : catalog->GetDatabases()) {
    PgDatabase row{
      .oid = db->GetId().id(),
      .datname = db->GetName(),
      .datdba = db->GetParentId().id(),
      .encoding = 6,  // UTF8
      .datlocprovider = PgDatabase::Datlocprovider::Libc,
      .datistemplate = false,
      .datallowconn = true,
      .dathasloginevt = false,
      .datconnlimit = -1,
      .datfrozenxid = 0,
      .datminmxid = 0,
      .dattablespace = 0,
      .datcollate = "C.UTF-8",
      .datctype = "C.UTF-8",
    };
    values.push_back(std::move(row));
  }

  auto result = CreateColumns<PgDatabase>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
