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

#include <deque>
#include <string>

#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
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
  auto database_id = GetDatabaseId();

  std::vector<PgEnum> rows;
  // The labels are owned here, not borrowed: EnumType::GetString hands back a
  // string_t by value, and a short label lives inside that temporary -- a view
  // into it reads whatever the next label reused the bytes for. A deque because
  // the rows point at these and must not move.
  std::deque<std::string> labels;
  catalog::Visit<catalog::SereneDBTypeEntry>(
    &_config.GetClientContext(), database_id,
    [&](const duckdb::TypeCatalogEntry& type) {
      if (type.user_type.id() != duckdb::LogicalTypeId::ENUM) {
        return;
      }
      const auto type_oid = type.oid;
      const auto size = duckdb::EnumType::GetSize(type.user_type);
      for (duckdb::idx_t i = 0; i < size; ++i) {
        const auto label = duckdb::EnumType::GetString(type.user_type, i);
        labels.emplace_back(label.GetData(), label.GetSize());
        rows.push_back({
          .oid = type_oid * 10000 + i + 1,
          .enumtypid = type_oid,
          .enumsortorder = static_cast<float>(i + 1),
          .enumlabel = std::string_view{labels.back()},
        });
      }
    });

  auto result = CreateColumns<PgEnum>(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    WriteData(result, rows[i], kNullMask, i, Roles());
  }
  return {std::move(result), rows.size()};
}

}  // namespace sdb::pg
