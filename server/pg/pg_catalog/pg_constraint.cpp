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

#include "pg/pg_catalog/pg_constraint.h"

#include <deque>

#include "catalog/catalog.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgConstraint::conbin),
  GetIndex(&PgConstraint::confkey),
  GetIndex(&PgConstraint::conpfeqop),
  GetIndex(&PgConstraint::conppeqop),
  GetIndex(&PgConstraint::conffeqop),
  GetIndex(&PgConstraint::confdelsetcols),
  GetIndex(&PgConstraint::conexclop),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgConstraint>::GetTableData() {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgConstraint> values;
  std::deque<std::string> conname_storage;
  std::vector<std::vector<int16_t>> conkey_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      auto& pk_columns = table->PKColumns();
      auto& columns = table->Columns();

      // Primary key constraint
      if (!pk_columns.empty()) {
        std::vector<int16_t> conkey;
        conkey.reserve(pk_columns.size());
        for (auto pk_id : pk_columns) {
          for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].GetId() == pk_id) {
              conkey.push_back(static_cast<int16_t>(i + 1));
              break;
            }
          }
        }

        conname_storage.push_back(std::string{table->GetName()} + "_pkey");
        conkey_storage.push_back(std::move(conkey));
        values.push_back({
          .oid = table->GetId().id(),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = PgConstraint::Contype::PrimaryKey,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
          .contypid = 0,
          // Synthetic PK index OID (see PkIndexOid in fwd.h and pg_index.cpp).
          .conindid = PkIndexOid(table->GetId().id()),
          .conparentid = 0,
          .confrelid = 0,
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
          .conkey = conkey_storage.back(),
        });
      }

      // Check constraints
      for (const auto& check : table->CheckConstraints()) {
        conname_storage.emplace_back(check.GetName());
        conkey_storage.emplace_back();
        values.push_back({
          .oid = check.GetId().id(),
          .conname = conname_storage.back(),
          .connamespace = schema->GetId().id(),
          .contype = PgConstraint::Contype::Check,
          .condeferrable = false,
          .condeferred = false,
          .conenforced = true,
          .convalidated = true,
          .conrelid = table->GetId().id(),
          .contypid = 0,
          .conindid = 0,
          .conparentid = 0,
          .confrelid = 0,
          .confupdtype = PgConstraint::Confchgtype::NoAction,
          .confdeltype = PgConstraint::Confchgtype::NoAction,
          .confmatchtype = PgConstraint::Confmatchtype::Simple,
          .conislocal = true,
          .coninhcount = 0,
          .connoinherit = false,
          .conperiod = false,
          .conkey = conkey_storage.back(),
        });
      }
    }
  }

  auto result = CreateColumns<PgConstraint>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
