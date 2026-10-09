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

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <ranges>

#include "auth/role_closure.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/format_type.h"
#include "pg/catalog/functions/reg_types.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kInfoColumnsSql["table_name"], SystemLookup::Object},
  {kInfoColumnsSql["table_schema"], SystemLookup::Namespace},
};

constexpr auto kAnyColumnPrivilege =
  duckdb::AclMode::Select | duckdb::AclMode::Insert | duckdb::AclMode::Update |
  duckdb::AclMode::References;

struct ColumnTypeInfo {
  ColumnType described;
  const BuiltinType* builtin;
  const BuiltinType* base;
  const BuiltinCollation* collation;
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  bool array;
};

struct InfoColumn {
  const SystemScan& scan;
  std::string_view database;
  const Session& session;
  ArrayTypeNames& arrays;
  std::string_view schema;
  std::string_view relation;
  std::string_view name;
  size_t position;
  const duckdb::LogicalType& type;
  const duckdb::ColumnDefinition* column;
  bool not_null;
  bool internal;
  mutable std::optional<ColumnTypeInfo> info;
  mutable duckdb::Identifier udt_schema;
  mutable std::string udt_name;

  const ColumnTypeInfo& Info() const {
    if (info) {
      return *info;
    }
    const auto described = DescribeColumnType(type);
    const auto* builtin =
      described.oid < kMaxSystem
        ? FindBuiltinType(static_cast<int32_t>(described.oid))
        : nullptr;
    if (!builtin) {
      auto entry = scan.FindById(described.oid);
      const auto array = !entry;
      if (array) {
        entry = scan.FindById(described.oid + 1);
      }
      return info.emplace(ColumnTypeInfo{.described = described,
                                         .builtin = nullptr,
                                         .base = nullptr,
                                         .collation = nullptr,
                                         .entry = entry,
                                         .array = array});
    }
    const auto* base = builtin;
    if (builtin->type == 'd') {
      if (const auto* found = FindBuiltinType(builtin->basetype)) {
        base = found;
      }
    }
    const auto* collation = FindBuiltinCollation(described.collation);
    return info.emplace(ColumnTypeInfo{
      .described = described,
      .builtin = builtin,
      .base = base,
      .collation =
        collation && collation->provider != 'd' ? collation : nullptr,
      .entry = nullptr,
      .array = false});
  }
};

bool IsDomain(const ColumnTypeInfo& info) {
  return info.builtin && info.builtin->type == 'd';
}

template<auto Measure>
constexpr auto kBuiltinMeasure =
  [](const auto& row) -> decltype(Measure(int64_t{}, int32_t{})) {
  const auto& info = row.Info();
  if (!info.builtin) {
    return std::nullopt;
  }
  return Measure(info.base->oid, info.described.typmod != -1
                                   ? info.described.typmod
                                   : info.builtin->typmod);
};

constexpr std::tuple kRelationColumns{
  Col<"table_catalog">(&InfoColumn::database),
  Col<"table_schema">(&InfoColumn::schema),
  Col<"table_name">(&InfoColumn::relation),
  Col<"column_name">(&InfoColumn::name),
  Col<"ordinal_position">(&InfoColumn::position),
  Col<"dtd_identifier">(
    [](const auto& row) { return absl::StrCat(row.position); })};

constexpr std::tuple kTypeColumns{
  Col<"udt_catalog">(&InfoColumn::database),
  Col<"data_type">([](const auto& row) -> std::string {
    const auto& info = row.Info();
    if (!info.builtin) {
      return info.array ? "ARRAY" : "USER-DEFINED";
    }
    if (info.base->elem != 0 && info.base->len == -1) {
      return "ARRAY";
    }
    return FormatTypeOut(row.session, static_cast<uint64_t>(info.base->oid),
                         std::nullopt);
  }),
  Col<"udt_schema">([](const auto& row) -> std::optional<std::string_view> {
    const auto& info = row.Info();
    if (info.builtin) {
      return FindSystemNamespace(info.base->nsp)->name;
    }
    if (!info.entry) {
      return std::nullopt;
    }
    row.udt_schema = info.entry->ParentSchemaName();
    return std::string_view{row.udt_schema.GetIdentifierName()};
  }),
  Col<"udt_name">([](const auto& row) -> std::optional<std::string_view> {
    const auto& info = row.Info();
    if (info.builtin) {
      return info.base->name;
    }
    if (!info.entry) {
      return std::nullopt;
    }
    if (!info.array) {
      return std::string_view{info.entry->name.GetIdentifierName()};
    }
    row.udt_name = row.arrays.Of(*info.entry);
    return std::string_view{row.udt_name};
  }),
  Col<"domain_catalog">([](const auto& row) -> std::optional<std::string_view> {
    if (!IsDomain(row.Info())) {
      return std::nullopt;
    }
    return row.database;
  }),
  Col<"domain_schema">([](const auto& row) -> std::optional<std::string_view> {
    const auto& info = row.Info();
    if (!IsDomain(info)) {
      return std::nullopt;
    }
    return FindSystemNamespace(info.builtin->nsp)->name;
  }),
  Col<"domain_name">([](const auto& row) -> std::optional<std::string_view> {
    const auto& info = row.Info();
    if (!IsDomain(info)) {
      return std::nullopt;
    }
    return info.builtin->name;
  }),
  Col<"character_maximum_length">(kBuiltinMeasure<&CharMaxLength>),
  Col<"character_octet_length">(kBuiltinMeasure<&CharOctetLength>),
  Col<"numeric_precision">(kBuiltinMeasure<&NumericPrecision>),
  Col<"numeric_precision_radix">(kBuiltinMeasure<&NumericPrecisionRadix>),
  Col<"numeric_scale">(kBuiltinMeasure<&NumericScale>),
  Col<"datetime_precision">(kBuiltinMeasure<&DatetimePrecision>),
  Col<"interval_type">(kBuiltinMeasure<&IntervalType>),
  Col<"collation_catalog">(
    [](const auto& row) -> std::optional<std::string_view> {
      if (!row.Info().collation) {
        return std::nullopt;
      }
      return row.database;
    }),
  Col<"collation_schema">(
    [](const auto& row) -> std::optional<std::string_view> {
      if (!row.Info().collation) {
        return std::nullopt;
      }
      return irs::StaticStrings::kPgCatalogSchema;
    }),
  Col<"collation_name">([](const auto& row) -> std::optional<std::string_view> {
    const auto* collation = row.Info().collation;
    if (!collation) {
      return std::nullopt;
    }
    return collation->name;
  })};

class InfoColumns final : public SystemTableScan<kInfoColumnsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSource{
    std::span{kSchemaObjectTypes}.first<2>(), SystemSchemas::Visit, kIndexes}};

  static constexpr auto kTableColumn = Shape<kSql, const InfoColumn>(
    kRelationColumns, kTypeColumns, Col<"is_nullable">([](const auto& row) {
      return std::string_view{row.not_null ? "NO" : "YES"};
    }),
    Col<"is_updatable">([](const auto& row) {
      return std::string_view{row.internal ? "NO" : "YES"};
    }),
    Col<"is_generated">([](const auto& row) {
      return std::string_view{row.column->Generated() ? "ALWAYS" : "NEVER"};
    }),
    Col<"generation_expression">(
      [](const auto& row) -> std::optional<std::string> {
        if (!row.column->Generated()) {
          return std::nullopt;
        }
        return row.column->GeneratedExpression().ToString();
      }),
    Col<"column_default">([](const auto& row) -> std::optional<std::string> {
      if (row.column->Generated() || !row.column->HasDefaultValue()) {
        return std::nullopt;
      }
      return row.column->DefaultValue().ToString();
    }));

  static constexpr auto kViewColumn = Shape<kSql, const InfoColumn>(
    kRelationColumns, kTypeColumns,
    Col<"is_nullable">([](const auto&) { return std::string_view{"YES"}; }),
    Col<"is_updatable">([](const auto&) { return std::string_view{"NO"}; }));

  void Row(const duckdb::TableCatalogEntry& table) {
    const bool visible = Visible(table.permissions);
    const auto schema = table.ParentSchemaName();
    const auto not_null = NotNullColumns(table);
    for (const auto& column :
         table.GetColumns().Logical() |
           std::views::filter([&](const duckdb::ColumnDefinition& column) {
             return visible ||
                    (_closure->HeldModes(column.Acl()) & kAnyColumnPrivilege) !=
                      duckdb::AclMode::NoRights;
           })) {
      Emit<kTableColumn>({.scan = *this,
                          .database = _database,
                          .session = _session,
                          .arrays = _arrays,
                          .schema = schema.GetIdentifierName(),
                          .relation = table.name.GetIdentifierName(),
                          .name = column.Name().GetIdentifierName(),
                          .position = static_cast<size_t>(Attnum(column)),
                          .type = column.Type(),
                          .column = &column,
                          .not_null = not_null[column.Logical().index],
                          .internal = table.internal,
                          .info = {},
                          .udt_schema = {},
                          .udt_name = {}});
    }
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    if (!Visible(view.permissions)) {
      return;
    }
    const auto columns = ViewColumns(Context(), view);
    if (!columns) {
      return;
    }
    const auto schema = view.ParentSchemaName();
    for (size_t i = 0; i < columns->types.size(); ++i) {
      Emit<kViewColumn>(
        {.scan = *this,
         .database = _database,
         .session = _session,
         .arrays = _arrays,
         .schema = schema.GetIdentifierName(),
         .relation = view.name.GetIdentifierName(),
         .name = view.ColumnName(*columns, i).GetIdentifierName(),
         .position = i + 1,
         .type = columns->types[i],
         .column = nullptr,
         .not_null = false,
         .internal = false,
         .info = {},
         .udt_schema = {},
         .udt_name = {}});
    }
  }

 private:
  bool Visible(const duckdb::Permissions& permissions) const {
    return !_closure || _closure->CanAny(duckdb::CatalogType::TABLE_ENTRY,
                                         permissions, kAnyColumnPrivilege);
  }

  std::string_view _database = Database().GetName().GetIdentifierName();
  Session _session = MakeSession(&Context());
  ArrayTypeNames _arrays{*this};
  std::shared_ptr<const auth::RoleClosure> _closure = SessionClosure();
};

}  // namespace

SystemTable gInfoColumns = SystemTableOf<InfoColumns>();

}  // namespace sdb::pg
