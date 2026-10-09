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

#include <array>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/list.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <vector>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kPgConstraintSql["oid"], SystemLookup::Object},
  {kPgConstraintSql["conrelid"], SystemLookup::Object},
};

struct ConstraintRow {
  const duckdb::TableCatalogEntry& table;
  const duckdb::Constraint& constraint;

  template<typename T>
  const T& As() const {
    return constraint.Cast<T>();
  }
};

struct ForeignKeyRow : ConstraintRow {
  mutable std::optional<duckdb::optional_ptr<const duckdb::TableCatalogEntry>>
    target = std::nullopt;
  mutable std::optional<std::vector<int16_t>> referenced = std::nullopt;

  duckdb::optional_ptr<const duckdb::TableCatalogEntry> Target(
    const SystemScan& scan) const {
    if (!target) {
      target = ReferencedTable(scan, table, As<duckdb::ForeignKeyConstraint>());
    }
    return *target;
  }

  std::span<const int16_t> Referenced(const SystemScan& scan) const {
    if (!referenced) {
      referenced = Attnums(Target(scan)->GetColumns(),
                           As<duckdb::ForeignKeyConstraint>().info.pk_keys);
    }
    return *referenced;
  }
};

constexpr std::tuple kConstraint{
  Col<"oid">([](const auto& row) { return row.constraint.oid; }),
  Col<"conrelid">([](const auto& row) { return row.table.oid; }),
  Col<"connamespace">(
    [](const auto& row) { return row.table.ParentSchemaOid(); }),
  Col<"conname">(
    [](const auto& row) { return ConstraintName(row.table, row.constraint); })};

class PgConstraint final : public SystemTableScan<kPgConstraintSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTableTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kOther = Shape<kSql, const ConstraintRow>(kConstraint);

  static constexpr auto kCheck = Shape<kSql, const ConstraintRow>(
    kConstraint, Col<"contype">([](const auto&) { return 'c'; }),
    Col<"conkey">(
      [](const ConstraintRow& row) -> std::optional<std::vector<int16_t>> {
        auto attnums = ExpressionAttnums(
          row.table, *row.As<duckdb::CheckConstraint>().expression);
        if (attnums.empty()) {
          return std::nullopt;
        }
        return attnums;
      }),
    Col<"conbin">([](const ConstraintRow& row) {
      return row.As<duckdb::CheckConstraint>().expression->ToString();
    }));

  static constexpr auto kNotNull = Shape<kSql, const ConstraintRow>(
    kConstraint, Col<"contype">([](const auto&) { return 'n'; }),
    Col<"conkey">([](const ConstraintRow& row) {
      return std::array{
        Attnum(row.table.GetColumn(row.As<duckdb::NotNullConstraint>().index))};
    }));

  static constexpr auto kUnique = Shape<kSql, const ConstraintRow>(
    kConstraint, Col<"contype">([](const ConstraintRow& row) {
      return row.As<duckdb::UniqueConstraint>().IsPrimaryKey() ? 'p' : 'u';
    }),
    Col<"condeferrable">([](const ConstraintRow& row) {
      return row.As<duckdb::UniqueConstraint>().IsDeferred();
    }),
    Col<"condeferred">([](const ConstraintRow& row) {
      return row.As<duckdb::UniqueConstraint>().IsDeferred();
    }),
    Col<"conindid">([](const ConstraintRow& row) {
      return row.As<duckdb::UniqueConstraint>().index_oid;
    }),
    Col<"connoinherit">([](const auto&) { return true; }),
    Col<"conkey">([](const ConstraintRow& row) {
      return Attnums(row.table.GetColumns(),
                     row.As<duckdb::UniqueConstraint>().GetLogicalIndexes(
                       row.table.GetColumns()));
    }));

  static constexpr auto kForeignKey = Shape<kSql, const ForeignKeyRow>(
    kConstraint, Col<"contype">([](const auto&) { return 'f'; }),
    Col<"confupdtype">([](const auto&) { return 'a'; }),
    Col<"confdeltype">([](const auto&) { return 'a'; }),
    Col<"confmatchtype">([](const auto&) { return 's'; }),
    Col<"connoinherit">([](const auto&) { return true; }),
    Col<"confrelid">([](const ForeignKeyRow& row, SystemScan& scan) {
      const auto target = row.Target(scan);
      return target ? target->oid : duckdb::idx_t{0};
    }),
    Col<"confkey">([](const ForeignKeyRow& row, SystemScan& scan)
                     -> std::optional<std::span<const int16_t>> {
      if (!row.Target(scan)) {
        return std::nullopt;
      }
      return row.Referenced(scan);
    }),
    Col<"conindid">([](const ForeignKeyRow& row, SystemScan& scan) {
      const auto target = row.Target(scan);
      const auto* key =
        target ? ReferencedKey(*target, row.Referenced(scan)) : nullptr;
      return key ? key->index_oid : duckdb::idx_t{0};
    }),
    Col<"conkey">([](const ForeignKeyRow& row) {
      return Attnums(row.table.GetColumns(),
                     row.As<duckdb::ForeignKeyConstraint>().info.fk_keys);
    }));

  void Row(const duckdb::TableCatalogEntry& table) {
    using enum duckdb::ConstraintType;
    if (!Allows<"conrelid">(table.oid)) {
      return;
    }
    for (const auto& constraint :
         table.GetConstraints() | std::views::filter(IsPgConstraint)) {
      switch (constraint->type) {
        case CHECK:
          Emit<kCheck>({table, *constraint});
          break;
        case NOT_NULL:
          Emit<kNotNull>({table, *constraint});
          break;
        case UNIQUE:
          Emit<kUnique>({table, *constraint});
          break;
        case FOREIGN_KEY:
          Emit<kForeignKey>({{table, *constraint}});
          break;
        case INVALID:
          Emit<kOther>({table, *constraint});
          break;
      }
    }
  }
};

}  // namespace

SystemTable gPgConstraint = SystemTableOf<PgConstraint>();

}  // namespace sdb::pg
