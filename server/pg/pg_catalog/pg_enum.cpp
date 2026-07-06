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

#include "pg/pg_catalog/pg_enum.h"

#include "catalog/catalog.h"
#include "catalog/user_type.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgEnum::oid),
  GetIndex(&PgEnum::enumtypid),
  GetIndex(&PgEnum::enumsortorder),
  GetIndex(&PgEnum::enumlabel),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgEnum>::GetTableData() {
  auto snapshot = _config.CatalogSnapshot();
  auto database_id = GetDatabaseId();

  std::vector<PgEnum> rows;
  // Keep labels alive: enum labels come from duckdb::EnumType storage which
  // outlives this call, but string_t::GetData() on a non-inlined label points
  // into that storage -- we keep Name wrapping a string_view into it.
  for (const auto& schema : snapshot->GetSchemas(database_id)) {
    for (const auto& type :
         snapshot->GetTypes(database_id, schema->GetName())) {
      const auto& info = type->GetInfo();
      if (info.type.id() != duckdb::LogicalTypeId::ENUM) {
        continue;
      }
      const auto type_oid = type->GetId().id();
      const auto size = duckdb::EnumType::GetSize(info.type);
      for (duckdb::idx_t i = 0; i < size; ++i) {
        auto label = duckdb::EnumType::GetString(info.type, i);
        rows.push_back({
          .oid = type_oid * 10000 + i + 1,
          .enumtypid = type_oid,
          .enumsortorder = static_cast<float>(i + 1),
          .enumlabel = std::string_view{label.GetData(), label.GetSize()},
        });
      }
    }
  }

  auto result = CreateColumns<PgEnum>(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    WriteData(result, rows[i], kNullMask, i, *_config.GetCatalogSnapshot());
  }
  return {std::move(result), rows.size()};
}

}  // namespace sdb::pg
