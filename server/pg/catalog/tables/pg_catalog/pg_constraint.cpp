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
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/parser/constraints/list.hpp>
#include <optional>
#include <ranges>

#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"

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

struct DomainConstraintRow {
  const duckdb::TypeCatalogEntry& type;
  const duckdb::DomainConstraint& constraint;
};

struct ForeignKeyRow : ConstraintRow {
  mutable duckdb::optional_ptr<const duckdb::TableCatalogEntry> target =
    nullptr;

  const duckdb::TableCatalogEntry& Target(const SystemScan& scan) const {
    if (!target) {
      target = &ReferencedTable(scan.Transaction(), table,
                                As<duckdb::ForeignKeyConstraint>());
    }
    return *target;
  }

  auto Referenced(const SystemScan& scan) const {
    return Attnums(Target(scan).GetColumns(),
                   As<duckdb::ForeignKeyConstraint>().info.pk_keys);
  }
};

constexpr std::tuple kConstraint{
  Col<"oid">([](const auto& row) { return row.constraint.oid; }),
  Col<"conrelid">([](const auto& row) { return row.table.oid; }),
  Col<"connamespace">(
    [](const auto& row) { return row.table.ParentSchemaOid(); }),
  Col<"conname">(
    [](const auto& row) { return ConstraintName(row.table, row.constraint); }),
  Col<"conenforced">([](const auto&) { return true; }),
  Col<"convalidated">([](const auto&) { return true; }),
  Col<"conislocal">([](const auto&) { return true; })};

constexpr std::tuple kDomainConstraint{
  Col<"oid">([](const auto& row) { return row.constraint.oid; }),
  Col<"conrelid">([](const auto&) { return duckdb::idx_t{0}; }),
  Col<"contypid">([](const auto& row) { return row.type.oid; }),
  Col<"connamespace">(
    [](const auto& row) { return row.type.ParentSchemaOid(); }),
  Col<"conname">([](const auto& row) { return row.constraint.name; }),
  Col<"conenforced">([](const auto&) { return true; }),
  Col<"convalidated">(
    [](const auto& row) { return row.constraint.validated; }),
  Col<"conislocal">([](const auto&) { return true; })};

constexpr std::tuple kNoForeignKey{
  Col<"confupdtype">([](const auto&) { return ' '; }),
  Col<"confdeltype">([](const auto&) { return ' '; }),
  Col<"confmatchtype">([](const auto&) { return ' '; })};

class PgConstraint final : public SystemTableScan<kPgConstraintSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = 10000;

  static constexpr std::tuple kSources{
    CatalogSource{kTableTypes, SystemSchemas::Skip, kIndexes},
    CatalogSource{kTypeTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kCheck = Shape<kSql, const ConstraintRow>(
    kConstraint, kNoForeignKey, Col<"contype">([](const auto&) { return 'c'; }),
    Col<"conkey">([](const ConstraintRow& row) {
      return NonEmpty(ExpressionAttnums(
        row.table, *row.As<duckdb::CheckConstraint>().expression));
    }),
    Col<"conbin">([](const ConstraintRow& row) {
      return ExpressionText(*row.As<duckdb::CheckConstraint>().expression);
    }));

  static constexpr auto kNotNull = Shape<kSql, const ConstraintRow>(
    kConstraint, kNoForeignKey, Col<"contype">([](const auto&) { return 'n'; }),
    Col<"conkey">([](const ConstraintRow& row) {
      return std::array{
        Attnum(row.table.GetColumn(row.As<duckdb::NotNullConstraint>().index))};
    }));

  static constexpr auto kDomainCheck = Shape<kSql, const DomainConstraintRow>(
    kDomainConstraint, kNoForeignKey,
    Col<"contype">([](const auto&) { return 'c'; }),
    Col<"conbin">([](const DomainConstraintRow& row) {
      return ExpressionText(*row.constraint.expression);
    }));

  static constexpr auto kDomainNotNull =
    Shape<kSql, const DomainConstraintRow>(
      kDomainConstraint, kNoForeignKey,
      Col<"contype">([](const auto&) { return 'n'; }));

  static constexpr auto kUnique = Shape<kSql, const ConstraintRow>(
    kConstraint, kNoForeignKey, Col<"contype">([](const ConstraintRow& row) {
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
      return KeyAttnums(row.table.GetColumns(),
                        row.As<duckdb::UniqueConstraint>());
    }));

  static constexpr auto kForeignKey = Shape<kSql, const ForeignKeyRow>(
    kConstraint, Col<"contype">([](const auto&) { return 'f'; }),
    Col<"confupdtype">([](const auto&) { return 'a'; }),
    Col<"confdeltype">([](const auto&) { return 'a'; }),
    Col<"confmatchtype">([](const auto&) { return 's'; }),
    Col<"connoinherit">([](const auto&) { return true; }),
    Col<"confrelid">([](const ForeignKeyRow& row, SystemScan& scan) {
      return row.Target(scan).oid;
    }),
    Col<"confkey">([](const ForeignKeyRow& row, SystemScan& scan) {
      return row.Referenced(scan);
    }),
    Col<"conindid">([](const ForeignKeyRow& row, SystemScan& scan) {
      const auto* key = ReferencedKey(row.Target(scan), row.Referenced(scan));
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
    for (const auto& constraint : table.GetConstraints()) {
      if (!IsPgConstraint(constraint)) {
        continue;
      }
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
          break;
      }
    }
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    using enum duckdb::ConstraintType;
    if (!type.domain_info || !Allows<"contypid">(type.oid)) {
      return;
    }

    for (const auto& constraint : type.domain_info->constraints) {
      if (!constraint.oid) {
        continue;
      }

      switch (constraint.type) {
        case CHECK:
          Emit<kDomainCheck>({type, constraint});
          break;
        case NOT_NULL:
          Emit<kDomainNotNull>({type, constraint});
          break;
        default:
          break;
      }
    }
  }
};

}  // namespace

SystemTable gPgConstraint = SystemTableOf<PgConstraint>();

}  // namespace sdb::pg
