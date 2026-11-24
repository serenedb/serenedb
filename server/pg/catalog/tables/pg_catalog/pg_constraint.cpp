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
  {kPgConstraintSql["contypid"], SystemLookup::Object},
};

constexpr duckdb::CatalogType kOwnerTypes[] = {duckdb::CatalogType::TABLE_ENTRY,
                                               duckdb::CatalogType::TYPE_ENTRY};

struct ConstraintRow {
  const duckdb::StandardEntry& owner;
  const duckdb::Constraint& constraint;

  template<typename T>
  const T& As() const {
    return constraint.Cast<T>();
  }

  duckdb::optional_ptr<const duckdb::TableCatalogEntry> Table() const {
    if (owner.type != duckdb::CatalogType::TABLE_ENTRY) {
      return nullptr;
    }
    return &owner.Cast<duckdb::TableCatalogEntry>();
  }
};

struct ForeignKeyRow : ConstraintRow {
  mutable duckdb::optional_ptr<const duckdb::TableCatalogEntry> target =
    nullptr;

  const duckdb::TableCatalogEntry& Target(const SystemScan& scan) const {
    if (!target) {
      target = &ReferencedTable(scan.Transaction(), *Table(),
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
  Col<"conrelid">([](const auto& row) {
    return row.Table() ? row.owner.oid : duckdb::idx_t{0};
  }),
  Col<"contypid">([](const auto& row) {
    return row.Table() ? duckdb::idx_t{0} : row.owner.oid;
  }),
  Col<"connamespace">(
    [](const auto& row) { return row.owner.ParentSchemaOid(); }),
  Col<"conname">([](const auto& row) {
    const auto table = row.Table();
    return table ? ConstraintName(*table, row.constraint)
                 : SystemConcatText{{row.constraint.constraint_name}};
  }),
  Col<"conenforced">([](const auto&) { return true; }),
  Col<"convalidated">([](const auto& row) { return row.constraint.validated; }),
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
    CatalogSource{kOwnerTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kCheck = Shape<kSql, const ConstraintRow>(
    kConstraint, kNoForeignKey, Col<"contype">([](const auto&) { return 'c'; }),
    Col<"conkey">([](const ConstraintRow& row) {
      const auto table = row.Table();
      return table ? NonEmpty(ExpressionAttnums(
                       *table, *row.As<duckdb::CheckConstraint>().expression))
                   : std::nullopt;
    }),
    Col<"conbin">([](const ConstraintRow& row) {
      const auto& expression = *row.As<duckdb::CheckConstraint>().expression;
      return row.Table() ? ExpressionText(expression)
                         : DomainCheckText(expression);
    }));

  static constexpr auto kNotNull = Shape<kSql, const ConstraintRow>(
    kConstraint, kNoForeignKey, Col<"contype">([](const auto&) { return 'n'; }),
    Col<"conkey">([](const ConstraintRow& row) {
      const auto table = row.Table();
      return table ? std::optional{std::array{Attnum(table->GetColumn(
                       row.As<duckdb::NotNullConstraint>().index))}}
                   : std::nullopt;
    }));

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
      return KeyAttnums(row.Table()->GetColumns(),
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
      return Attnums(row.Table()->GetColumns(),
                     row.As<duckdb::ForeignKeyConstraint>().info.fk_keys);
    }));

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"conrelid">(table.oid)) {
      return;
    }
    Constraints(table, table.GetConstraints());
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    if (!type.domain_info || !Allows<"contypid">(type.oid)) {
      return;
    }
    Constraints(type, type.domain_info->constraints);
  }

 private:
  void Constraints(
    const duckdb::StandardEntry& owner,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>>& constraints) {
    using enum duckdb::ConstraintType;
    for (const auto& constraint : constraints) {
      if (!IsPgConstraint(constraint)) {
        continue;
      }
      switch (constraint->type) {
        case CHECK:
          Emit<kCheck>({owner, *constraint});
          break;
        case NOT_NULL:
          Emit<kNotNull>({owner, *constraint});
          break;
        case UNIQUE:
          Emit<kUnique>({owner, *constraint});
          break;
        case FOREIGN_KEY:
          Emit<kForeignKey>({{owner, *constraint}});
          break;
        case INVALID:
          break;
      }
    }
  }
};

}  // namespace

SystemTable gPgConstraint = SystemTableOf<PgConstraint>();

}  // namespace sdb::pg
