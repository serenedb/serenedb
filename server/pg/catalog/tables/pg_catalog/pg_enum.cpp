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

#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>

#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypeTypes[] = {duckdb::CatalogType::TYPE_ENTRY};

constexpr SystemIndex kEnumIndexes[] = {
  {kPgEnumSql["enumtypid"], SystemLookup::Object},
};

struct EnumLabel {
  const duckdb::TypeCatalogEntry& entry;
  duckdb::idx_t index;
  duckdb::string_t label;
};

class PgEnum final : public SystemTableScan<kPgEnumSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypeTypes, SystemSchemas::Skip, kEnumIndexes}};

  static constexpr auto kLabel = Shape<kSql, const EnumLabel>(
    Col<"oid">(
      [](const auto& row) { return row.entry.oid * 10000 + row.index + 1; }),
    Col<"enumtypid">([](const auto& row) { return row.entry.oid; }),
    Col<"enumsortorder">([](const auto& row) { return row.index + 1; }),
    Col<"enumlabel">([](const auto& row) {
      return std::string_view{row.label.GetData(), row.label.GetSize()};
    }));

  void Row(const duckdb::TypeCatalogEntry& entry) {
    const auto& type = entry.user_type;
    if (type.id() != duckdb::LogicalTypeId::ENUM) {
      return;
    }
    const auto size = duckdb::EnumType::GetSize(type);
    for (duckdb::idx_t i = 0; i < size; ++i) {
      Emit<kLabel>({entry, i, duckdb::EnumType::GetString(type, i)});
    }
  }
};

}  // namespace

SystemTable gPgEnum = SystemTableOf<PgEnum>();

}  // namespace sdb::pg
