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
#include <duckdb/parser/tableref/joinref.hpp>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/types/mapping.h"

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

struct RuleScope {
  size_t parent;
  std::vector<const duckdb::BaseTableRef*> tables;
  std::vector<const duckdb::ColumnRefExpression*> columns;
  std::vector<const duckdb::StarExpression*> stars;
  std::vector<const duckdb::Identifier*> joined;
};

constexpr size_t kNoScope = std::numeric_limits<size_t>::max();

void CollectRule(duckdb::QueryNode& node, std::vector<RuleScope>& scopes,
                 size_t scope);

void CollectExpression(duckdb::ParsedExpression& expression,
                       std::vector<RuleScope>& scopes, size_t scope) {
  const auto expression_class = expression.GetExpressionClass();
  if (expression_class == duckdb::ExpressionClass::COLUMN_REF) {
    scopes[scope].columns.emplace_back(
      &expression.Cast<duckdb::ColumnRefExpression>());
  } else if (expression_class == duckdb::ExpressionClass::STAR) {
    scopes[scope].stars.emplace_back(
      &expression.Cast<duckdb::StarExpression>());
  } else if (expression_class == duckdb::ExpressionClass::SUBQUERY) {
    scopes.push_back({.parent = scope});
    CollectRule(
      *expression.Cast<duckdb::SubqueryExpression>().SubqueryMutable()->node,
      scopes, scopes.size() - 1);
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expression, [&](duckdb::ParsedExpression& child) {
      CollectExpression(child, scopes, scope);
    });
}

void CollectRule(duckdb::QueryNode& node, std::vector<RuleScope>& scopes,
                 size_t scope) {
  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
    node,
    [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
      CollectExpression(*child, scopes, scope);
    },
    [&](duckdb::TableRef& ref) {
      if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
        scopes[scope].tables.emplace_back(&ref.Cast<duckdb::BaseTableRef>());
      } else if (ref.type == duckdb::TableReferenceType::JOIN) {
        for (const auto& column : ref.Cast<duckdb::JoinRef>().using_columns) {
          scopes[scope].joined.emplace_back(&column);
        }
      }
    });
}

const duckdb::Identifier& Qualifier(const duckdb::BaseTableRef& table) {
  return table.alias.empty() ? table.GetQualifiedName().Name() : table.alias;
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

  static constexpr duckdb::idx_t kRows = 100000;

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
    for (const auto& constraint : table.GetConstraints()) {
      if (IsPgConstraint(constraint)) {
        Constraint(table, *constraint);
      }
    }
    for (const auto& column : table.GetColumns().Logical()) {
      if (HasAttrdef(column)) {
        Attrdef(table, column);
      }
    }
    if (!Allows<"deptype">('n') && !Allows<"deptype">('a')) {
      return;
    }
    ScanSubjects(table, [&](duckdb::CatalogEntry& object,
                            const duckdb::DependencyDependentFlags& flags) {
      if (object.type == duckdb::CatalogType::TYPE_ENTRY) {
        ColumnTypes(table, object);
      } else if (object.type == duckdb::CatalogType::SEQUENCE_ENTRY) {
        Sequence(table, object, flags.IsOwnedBy());
      }
    });
    if (Triggered(table)) {
      table.ScanTriggers(Transaction(), [&](duckdb::CatalogEntry& trigger) {
        Trigger(table, trigger.Cast<duckdb::TriggerCatalogEntry>());
      });
    }
  }

  void Row(const duckdb::TriggerCatalogEntry& trigger) {
    if (!Allows<"deptype">('a') && !Allows<"deptype">('n')) {
      return;
    }
    Trigger(SiblingTable(Transaction(), trigger, trigger.base_table->Table()),
            trigger);
  }

  void Row(duckdb::ViewCatalogEntry& view) {
    Namespace(view);
    Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, view.oid, 0, 'i');
    RowType(view.oid);
    if (!Allows<"deptype">('n')) {
      return;
    }
    std::vector<RuleScope> scopes{{.parent = kNoScope}};
    CollectRule(*view.query->node, scopes, 0);
    std::vector<duckdb::CatalogEntry*> relations;
    ScanSubjects(view, [&](duckdb::CatalogEntry& object,
                           const duckdb::DependencyDependentFlags&) {
      if (object.type == duckdb::CatalogType::TABLE_ENTRY ||
          object.type == duckdb::CatalogType::VIEW_ENTRY) {
        relations.push_back(&object);
      } else {
        Reference(kPgRewriteTable, view.oid, object, 'n');
      }
    });
    for (auto* relation : relations) {
      RuleRelation(view, *relation, scopes, relations);
    }
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
    ScanSubjects(index, [&](duckdb::CatalogEntry& object,
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
      if (absl::c_none_of(index.parsed_expressions, [](const auto& expression) {
            return expression->GetExpressionClass() ==
                   duckdb::ExpressionClass::COLUMN_REF;
          })) {
        Depend(kPgClassTable, index.oid, 0, kPgClassTable, object.oid, 0, 'a');
      }
    });
  }

  void Row(duckdb::SequenceCatalogEntry& sequence) { Namespace(sequence); }

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
    ScanSubjects(dependent, [&](duckdb::CatalogEntry& object,
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

  bool HasColumn(duckdb::CatalogEntry& relation,
                 const duckdb::Identifier& column) {
    if (relation.type == duckdb::CatalogType::TABLE_ENTRY) {
      return relation.Cast<duckdb::TableCatalogEntry>().ColumnExists(column);
    }
    const auto columns =
      ViewColumns(Context(), relation.Cast<duckdb::ViewCatalogEntry>());
    return columns && absl::c_linear_search(columns->names, column);
  }

  void RuleRelation(const duckdb::ViewCatalogEntry& view,
                    duckdb::CatalogEntry& relation,
                    std::span<const RuleScope> scopes,
                    std::span<duckdb::CatalogEntry* const> relations) {
    const auto names = [&](const duckdb::CatalogEntry& entry,
                           const duckdb::BaseTableRef* table) {
      const auto& name = table->GetQualifiedName();
      return name.Name() == entry.name &&
             (name.Schema().empty() ||
              name.Schema() == entry.ParentSchema(Transaction()).name);
    };
    const auto names_relation = [&](const duckdb::BaseTableRef* table) {
      return names(relation, table);
    };
    const auto named = [&](const RuleScope& scope) {
      return absl::c_any_of(scope.tables, names_relation);
    };
    if (absl::c_none_of(scopes, named)) {
      return;
    }
    const auto has = [&](const duckdb::BaseTableRef* table,
                         const duckdb::Identifier& column) {
      return absl::c_any_of(relations, [&](duckdb::CatalogEntry* entry) {
        return names(*entry, table) && HasColumn(*entry, column);
      });
    };
    const auto qualified = [&](size_t scope,
                               const duckdb::Identifier& qualifier) {
      for (; scope != kNoScope; scope = scopes[scope].parent) {
        const auto& tables = scopes[scope].tables;
        const auto it =
          absl::c_find_if(tables, [&](const duckdb::BaseTableRef* table) {
            return Qualifier(*table) == qualifier;
          });
        if (it != tables.end()) {
          return names_relation(*it);
        }
      }
      return false;
    };
    const auto unqualified = [&](size_t scope,
                                 const duckdb::Identifier& column) {
      for (; scope != kNoScope; scope = scopes[scope].parent) {
        const auto& tables = scopes[scope].tables;
        const auto binds = [&](const duckdb::BaseTableRef* table) {
          return has(table, column);
        };
        if (absl::c_any_of(tables, binds)) {
          return absl::c_any_of(tables, [&](const duckdb::BaseTableRef* table) {
            return names_relation(table) && binds(table);
          });
        }
      }
      return false;
    };
    bool star = false;
    for (size_t scope = 0; scope < scopes.size() && !star; ++scope) {
      star = absl::c_any_of(
        scopes[scope].stars, [&](const duckdb::StarExpression* reference) {
          return reference->RelationName().empty()
                   ? named(scopes[scope])
                   : qualified(scope, reference->RelationName());
        });
    }
    const auto referenced = [&](const duckdb::Identifier& column) {
      if (star) {
        return true;
      }
      for (size_t scope = 0; scope < scopes.size(); ++scope) {
        if (absl::c_any_of(
              scopes[scope].joined, [&](const duckdb::Identifier* name) {
                return *name == column && unqualified(scope, column);
              })) {
          return true;
        }
        for (const auto* reference : scopes[scope].columns) {
          const auto& parts = reference->ColumnNames();
          if (parts.back() == column &&
              (parts.size() < 2 ? unqualified(scope, column)
                                : qualified(scope, parts[parts.size() - 2]))) {
            return true;
          }
        }
      }
      return false;
    };
    bool any = false;
    const auto depend = [&](int32_t attnum) {
      any = true;
      Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, relation.oid, attnum,
             'n');
    };
    if (relation.type == duckdb::CatalogType::TABLE_ENTRY) {
      for (const auto& column :
           relation.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
        if (referenced(column.Name())) {
          depend(Attnum(column));
        }
      }
    } else if (auto& target = relation.Cast<duckdb::ViewCatalogEntry>();
               const auto columns = ViewColumns(Context(), target)) {
      for (size_t i = 0; i < columns->types.size(); ++i) {
        if (referenced(columns->names[i])) {
          depend(static_cast<int32_t>(i + 1));
        }
      }
    }
    if (!any) {
      Depend(kPgRewriteTable, view.oid, 0, kPgClassTable, relation.oid, 0, 'n');
    }
  }

  template<typename R>
  void Columns(duckdb::idx_t constraint, const duckdb::TableCatalogEntry& table,
               const R& attnums, char deptype) {
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
          Columns(constraint.oid, table, KeyAttnums(table.GetColumns(), unique),
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
    const auto& target = ReferencedTable(Transaction(), table, fk);
    const auto referenced = Attnums(target.GetColumns(), fk.info.pk_keys);
    if (const auto* key = ReferencedKey(target, referenced)) {
      Depend(kPgConstraintTable, fk.oid, 0, kPgClassTable, key->index_oid, 0,
             'n');
    }
    Columns(fk.oid, target, referenced, 'n');
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
      const auto oid = CatalogTypeOid(column.Type());
      if (oid == type.oid || oid == TypeArrayOid(type.oid)) {
        Depend(kPgClassTable, table.oid, Attnum(column), kPgTypeTable, oid, 0,
               'n');
      }
    }
  }

  void Sequence(const duckdb::TableCatalogEntry& table,
                const duckdb::CatalogEntry& sequence, bool owned_by) {
    const auto& owner =
      sequence.Cast<duckdb::SequenceCatalogEntry>().owner_column;
    int32_t owner_column = 0;
    for (const auto& column : table.GetColumns().Logical()) {
      const bool names =
        (HasDefault(column) || column.Generated()) &&
        NamesSequence(DefaultExpression(column), sequence.name);
      if (names && HasAttrdef(column)) {
        Depend(kPgAttrdefTable, column.CatalogOid(), 0, kPgClassTable,
               sequence.oid, 0, 'n');
      }
      if (owner_column == 0 &&
          (owner.IsValid() ? owner.GetIndex() == column.CatalogOid() : names)) {
        owner_column = Attnum(column);
      }
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
