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

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/parser/constraints/list.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <ranges>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

bool NamesSequence(const duckdb::ParsedExpression& expression,
                   const duckdb::Identifier& sequence) {
  if (const auto argument = NextvalArgument(expression)) {
    return duckdb::QualifiedName::Parse(*argument).Name() == sequence;
  }
  bool named = false;
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expression, [&](const duckdb::ParsedExpression& child) {
      named = named || NamesSequence(child, sequence);
    });
  return named;
}

struct RuleReferences {
  std::vector<const duckdb::BaseTableRef*> tables;
  std::vector<const duckdb::ColumnRefExpression*> columns;
  std::vector<const duckdb::StarExpression*> stars;
};

void CollectRule(duckdb::QueryNode& node, RuleReferences& references);

void CollectExpression(duckdb::ParsedExpression& expression,
                       RuleReferences& references) {
  const auto expression_class = expression.GetExpressionClass();
  if (expression_class == duckdb::ExpressionClass::COLUMN_REF) {
    references.columns.emplace_back(
      &expression.Cast<duckdb::ColumnRefExpression>());
  } else if (expression_class == duckdb::ExpressionClass::STAR) {
    references.stars.emplace_back(&expression.Cast<duckdb::StarExpression>());
  } else if (expression_class == duckdb::ExpressionClass::SUBQUERY) {
    CollectRule(
      *expression.Cast<duckdb::SubqueryExpression>().SubqueryMutable()->node,
      references);
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expression, [&](duckdb::ParsedExpression& child) {
      CollectExpression(child, references);
    });
}

void CollectRule(duckdb::QueryNode& node, RuleReferences& references) {
  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
    node,
    [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
      CollectExpression(*child, references);
    },
    [&](duckdb::TableRef& ref) {
      if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
        references.tables.emplace_back(&ref.Cast<duckdb::BaseTableRef>());
      }
    });
}

constexpr SystemIndex kIndexes[] = {
  {kPgDependSql["objid"], SystemLookup::Dependents},
  {kPgDependSql["refobjid"], SystemLookup::Dependents},
};

struct Dependency {
  duckdb::idx_t classid;
  duckdb::idx_t objid;
  int32_t objsubid;
  duckdb::idx_t refclassid;
  duckdb::idx_t refobjid;
  int32_t refobjsubid;
  char deptype;
};

class PgDepend final : public SystemTableScan<kPgDependSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kSchemaObjectTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kDependency = Shape<kSql, const Dependency>(
    Col<"classid">(&Dependency::classid), Col<"objid">(&Dependency::objid),
    Col<"objsubid">(&Dependency::objsubid),
    Col<"refclassid">(&Dependency::refclassid),
    Col<"refobjid">(&Dependency::refobjid),
    Col<"refobjsubid">(&Dependency::refobjsubid),
    Col<"deptype">(&Dependency::deptype));

  void Row(duckdb::TableCatalogEntry& table) {
    Namespace(table);
    RowType(table.oid);
    for (const auto& constraint :
         table.GetConstraints() | std::views::filter(IsPgConstraint)) {
      Constraint(table, *constraint);
    }
    for (const auto& column :
         table.GetColumns().Logical() | std::views::filter(HasAttrdef)) {
      Attrdef(table, column);
    }
    if (!Allows<"deptype">('n') && !Allows<"deptype">('a')) {
      return;
    }
    Dependencies().ScanEdges(
      Transaction(), table, true,
      [&](duckdb::CatalogEntry& object,
          const duckdb::DependencyDependentFlags& flags) {
        if (object.type == duckdb::CatalogType::TYPE_ENTRY) {
          ColumnTypes(table, object);
        } else if (object.type == duckdb::CatalogType::SEQUENCE_ENTRY &&
                   !table.NumbersRowsWith(object)) {
          Sequence(table, object, flags.IsOwnedBy());
        }
      });
    table.ScanTriggers(Transaction(), [&](duckdb::CatalogEntry& trigger) {
      Trigger(table, trigger.Cast<duckdb::TriggerCatalogEntry>());
    });
  }

  void Row(const duckdb::TriggerCatalogEntry& trigger) {
    if (!Allows<"deptype">('a') && !Allows<"deptype">('n')) {
      return;
    }
    if (auto table = TriggerTable(*this, trigger)) {
      Trigger(*table, trigger);
    }
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    Namespace(view);
    Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, view.oid, 0, 'i');
    RowType(view.oid);
    if (!Allows<"deptype">('n')) {
      return;
    }
    RuleReferences references;
    CollectRule(*view.query->node, references);
    Dependencies().ScanEdges(
      Transaction(), view, true,
      [&](duckdb::CatalogEntry& object,
          const duckdb::DependencyDependentFlags&) {
        if (object.type == duckdb::CatalogType::TABLE_ENTRY ||
            object.type == duckdb::CatalogType::VIEW_ENTRY) {
          RuleRelation(view, object, references);
        } else {
          Reference(kPgRewriteTable, view.oid, object, 'n');
        }
      });
  }

  void Row(duckdb::MacroCatalogEntry& macro) {
    Namespace(macro);
    if (Allows<"deptype">('n')) {
      References(kPgProcTable, macro);
    }
  }

  void Row(duckdb::IndexCatalogEntry& index) {
    if (!Allows<"deptype">('n') && !Allows<"deptype">('a')) {
      return;
    }
    Dependencies().ScanEdges(
      Transaction(), index, true,
      [&](duckdb::CatalogEntry& object,
          const duckdb::DependencyDependentFlags&) {
        if (object.oid != index.table_oid) {
          Reference(kPgClassTable, index.oid, object, 'n');
          return;
        }
        const auto* table = object.type == duckdb::CatalogType::TABLE_ENTRY
                              ? &object.Cast<duckdb::TableCatalogEntry>()
                              : nullptr;
        for (const auto column : index.column_ids) {
          Depend(kPgClassTable, index.oid, 0, kPgClassTable, object.oid,
                 table ? Attnum(table->GetColumns().GetColumn(
                           duckdb::PhysicalIndex{column}))
                       : static_cast<int32_t>(column + 1),
                 'a');
        }
        if (absl::c_none_of(index.parsed_expressions,
                            [](const auto& expression) {
                              return expression->GetExpressionClass() ==
                                     duckdb::ExpressionClass::COLUMN_REF;
                            })) {
          Depend(kPgClassTable, index.oid, 0, kPgClassTable, object.oid, 0,
                 'a');
        }
      });
  }

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!NumbersRows(sequence)) {
      Namespace(sequence);
    }
  }

  void Row(const duckdb::TypeCatalogEntry& type) {
    Namespace(type);
    Depend(kPgTypeTable, TypeArrayOid(type.oid), 0, kPgTypeTable, type.oid, 0,
           'i');
    if (duckdb::StructType::IsStruct(type.user_type)) {
      Depend(kPgClassTable, type.oid, 0, kPgTypeTable, type.oid, 0, 'i');
    }
  }

 private:
  void Depend(duckdb::idx_t classid, duckdb::idx_t objid, int32_t objsubid,
              duckdb::idx_t refclassid, duckdb::idx_t refobjid,
              int32_t refobjsubid, char deptype) {
    Emit<kDependency>(
      {classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype});
  }

  void Namespace(const duckdb::CatalogEntry& entry) {
    Depend(CatalogClassOid(entry.type), entry.oid, 0, kPgNamespaceTable,
           entry.ParentSchemaOid(), 0, 'n');
  }

  void RowType(duckdb::idx_t relation) {
    const auto row = RowTypeOid(relation);
    Depend(kPgTypeTable, row, 0, kPgClassTable, relation, 0, 'i');
    Depend(kPgTypeTable, TypeArrayOid(row), 0, kPgTypeTable, row, 0, 'i');
  }

  void Reference(duckdb::idx_t classid, duckdb::idx_t objid,
                 const duckdb::CatalogEntry& object, char deptype) {
    using enum duckdb::CatalogType;
    if (object.type == SCHEMA_ENTRY || object.type == ROLE_ENTRY ||
        object.type == DATABASE_ENTRY) {
      return;
    }
    Depend(classid, objid, 0, CatalogClassOid(object.type), object.oid, 0,
           deptype);
  }

  void References(duckdb::idx_t classid, duckdb::CatalogEntry& dependent) {
    Dependencies().ScanEdges(Transaction(), dependent, true,
                             [&](duckdb::CatalogEntry& object,
                                 const duckdb::DependencyDependentFlags&) {
                               Reference(classid, dependent.oid, object, 'n');
                             });
  }

  void Trigger(const duckdb::TableCatalogEntry& table,
               const duckdb::TriggerCatalogEntry& trigger) {
    Depend(kPgTriggerTable, trigger.oid, 0, kPgClassTable, table.oid, 0, 'a');
    for (const auto& column : trigger.columns) {
      if (table.GetColumns().ColumnExists(column)) {
        Depend(kPgTriggerTable, trigger.oid, 0, kPgClassTable, table.oid,
               Attnum(table.GetColumns().GetColumn(column)), 'n');
      }
    }
  }

  void RuleRelation(const duckdb::ViewCatalogEntry& view,
                    duckdb::CatalogEntry& relation,
                    const RuleReferences& references) {
    std::vector<duckdb::Identifier> qualifiers;
    for (const auto* table : references.tables) {
      const auto& name = table->GetQualifiedName();
      if (name.Name() == relation.name &&
          (name.Schema().empty() ||
           name.Schema() == relation.ParentSchemaName())) {
        qualifiers.emplace_back(table->alias.empty() ? name.Name()
                                                     : table->alias);
      }
    }
    if (qualifiers.empty()) {
      return;
    }
    std::vector<duckdb::Identifier> names;
    if (relation.type == duckdb::CatalogType::TABLE_ENTRY) {
      for (const auto& column :
           relation.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
        names.emplace_back(column.Name());
      }
    } else if (const auto columns = ViewColumns(
                 Context(), relation.Cast<duckdb::ViewCatalogEntry>())) {
      for (size_t i = 0; i < columns->types.size(); ++i) {
        names.emplace_back(
          relation.Cast<duckdb::ViewCatalogEntry>().ColumnName(*columns, i));
      }
    }
    std::vector<int32_t> attnums;
    const auto reference = [&](const duckdb::Identifier& column) {
      const auto it = absl::c_find(names, column);
      const auto attnum = static_cast<int32_t>(it - names.begin() + 1);
      if (it != names.end() && !absl::c_linear_search(attnums, attnum)) {
        attnums.emplace_back(attnum);
      }
    };
    for (const auto* column : references.columns) {
      const auto& parts = column->ColumnNames();
      if (parts.size() < 2 ||
          absl::c_linear_search(qualifiers, parts[parts.size() - 2])) {
        reference(parts.back());
      }
    }
    for (const auto* star : references.stars) {
      if (star->RelationName().empty() ||
          absl::c_linear_search(qualifiers, star->RelationName())) {
        for (const auto& name : names) {
          reference(name);
        }
      }
    }
    absl::c_sort(attnums);
    if (attnums.empty()) {
      Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, relation.oid, 0, 'n');
    }
    for (const auto attnum : attnums) {
      Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, relation.oid, attnum,
             'n');
    }
  }

  void Columns(duckdb::idx_t constraint, const duckdb::TableCatalogEntry& table,
               std::span<const int16_t> attnums, char deptype) {
    for (const auto attnum : attnums) {
      Depend(kPgConstraintTable, constraint, 0, kPgClassTable, table.oid,
             attnum, deptype);
    }
  }

  void Constraint(const duckdb::TableCatalogEntry& table,
                  const duckdb::Constraint& constraint) {
    using enum duckdb::ConstraintType;
    switch (constraint.type) {
      case FOREIGN_KEY:
        ForeignKey(table, constraint.Cast<duckdb::ForeignKeyConstraint>());
        return;
      case UNIQUE: {
        const auto& unique = constraint.Cast<duckdb::UniqueConstraint>();
        if (Allows<"deptype">('a')) {
          Columns(constraint.oid, table,
                  Attnums(table.GetColumns(),
                          unique.GetLogicalIndexes(table.GetColumns())),
                  'a');
        }
        Depend(kPgClassTable, unique.index_oid, 0, kPgConstraintTable,
               constraint.oid, 0, 'i');
        return;
      }
      case CHECK: {
        if (!Allows<"deptype">('a') && !Allows<"deptype">('n')) {
          return;
        }
        const auto attnums = ExpressionAttnums(
          table, *constraint.Cast<duckdb::CheckConstraint>().expression);
        if (attnums.empty()) {
          Depend(kPgConstraintTable, constraint.oid, 0, kPgClassTable,
                 table.oid, 0, 'a');
        }
        Columns(constraint.oid, table, attnums, 'a');
        Columns(constraint.oid, table, attnums, 'n');
        return;
      }
      case NOT_NULL:
        Depend(kPgConstraintTable, constraint.oid, 0, kPgClassTable, table.oid,
               Attnum(table.GetColumn(
                 constraint.Cast<duckdb::NotNullConstraint>().index)),
               'a');
        return;
      case INVALID:
        return;
    }
  }

  void ForeignKey(const duckdb::TableCatalogEntry& table,
                  const duckdb::ForeignKeyConstraint& fk) {
    if (Allows<"deptype">('a')) {
      Columns(fk.oid, table, Attnums(table.GetColumns(), fk.info.fk_keys), 'a');
    }
    if (!Allows<"deptype">('n')) {
      return;
    }
    const auto target = ReferencedTable(*this, table, fk);
    if (!target) {
      return;
    }
    const auto referenced = Attnums(target->GetColumns(), fk.info.pk_keys);
    if (const auto* key = ReferencedKey(*target, referenced)) {
      Depend(kPgConstraintTable, fk.oid, 0, kPgClassTable, key->index_oid, 0,
             'n');
    }
    Columns(fk.oid, *target, referenced, 'n');
  }

  void Attrdef(const duckdb::TableCatalogEntry& table,
               const duckdb::ColumnDefinition& column) {
    if (!column.Generated()) {
      Depend(kPgAttrdefTable, column.CatalogOid(), 0, kPgClassTable, table.oid,
             Attnum(column), 'a');
      return;
    }
    Depend(kPgAttrdefTable, column.CatalogOid(), 0, kPgClassTable, table.oid,
           Attnum(column), 'i');
    if (!Allows<"deptype">('n')) {
      return;
    }
    for (const auto attnum :
         ExpressionAttnums(table, column.GeneratedExpression())) {
      Depend(kPgAttrdefTable, column.CatalogOid(), 0, kPgClassTable, table.oid,
             attnum, 'n');
    }
  }

  void ColumnTypes(const duckdb::TableCatalogEntry& table,
                   const duckdb::CatalogEntry& type) {
    for (const auto& column : table.GetColumns().Logical()) {
      const auto oid = Type2Oid(column.Type());
      if (oid == type.oid || oid == TypeArrayOid(type.oid)) {
        Depend(kPgClassTable, table.oid, Attnum(column), kPgTypeTable, oid, 0,
               'n');
      }
    }
  }

  void Sequence(const duckdb::TableCatalogEntry& table,
                const duckdb::CatalogEntry& sequence, bool owned_by) {
    int32_t owner_column = 0;
    for (const auto& column :
         table.GetColumns().Logical() |
           std::views::filter([&](const duckdb::ColumnDefinition& column) {
             return HasAttrdef(column) &&
                    NamesSequence(DefaultExpression(column), sequence.name);
           })) {
      Depend(kPgAttrdefTable, column.CatalogOid(), 0, kPgClassTable,
             sequence.oid, 0, 'n');
      owner_column = Attnum(column);
    }
    if (owned_by) {
      Depend(kPgClassTable, sequence.oid, 0, kPgClassTable, table.oid,
             owner_column, 'a');
    }
  }
};

}  // namespace

SystemTable gPgDepend = SystemTableOf<PgDepend>();

}  // namespace sdb::pg
