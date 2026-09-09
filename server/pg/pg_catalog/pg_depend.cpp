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

#include "pg/pg_catalog/pg_depend.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "pg/pg_catalog/pg_attrdef.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_constraint.h"
#include "pg/pg_catalog/pg_database.h"
#include "pg/pg_catalog/pg_foreign_server.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_rewrite.h"
#include "pg/pg_catalog/pg_ts_dict.h"
#include "pg/pg_catalog/pg_type.h"
#include "pg/pg_types.h"
#include "pg/sql_utils.h"

namespace sdb::pg {

Oid CatalogClassOid(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      return Oid{PgProc::kId};
    case duckdb::CatalogType::TYPE_ENTRY:
      return Oid{PgType::kId};
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return Oid{PgNamespace::kId};
    case duckdb::CatalogType::DATABASE_ENTRY:
      return Oid{PgDatabase::kId};
    case duckdb::CatalogType::ROLE_ENTRY:
      return Oid{PgAuthid::kId};
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
      return Oid{PgForeignServer::kId};
    case duckdb::CatalogType::TOKENIZER_ENTRY:
      return Oid{PgTsDict::kId};
    default:
      return Oid{PgClass::kId};
  }
}

namespace {

struct Edge {
  duckdb::CatalogEntry* object;
  duckdb::CatalogEntry* dependent;
  bool owned_by;
};

bool NamesSequence(const duckdb::ParsedExpression& expression,
                   const duckdb::Identifier& sequence) {
  bool named = false;
  if (expression.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION &&
      expression.Cast<duckdb::FunctionExpression>().FunctionName() ==
        duckdb::Identifier{"nextval"}) {
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expression, [&](const duckdb::ParsedExpression& argument) {
        if (argument.GetExpressionClass() ==
            duckdb::ExpressionClass::CONSTANT) {
          const auto name = duckdb::QualifiedName::Parse(
            argument.Cast<duckdb::ConstantExpression>().GetValue().ToString());
          named = named || name.Name() == sequence;
        }
      });
    return named;
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expression, [&](const duckdb::ParsedExpression& child) {
      named = named || NamesSequence(child, sequence);
    });
  return named;
}

std::vector<PgDepend> CollectEdges(duckdb::ClientContext& context,
                                   duckdb::Catalog& database) {
  std::vector<PgDepend> rows;
  const auto emit = [&](Oid classid, duckdb::idx_t objid, int32_t objsubid,
                        Oid refclassid, duckdb::idx_t refobjid,
                        int32_t refobjsubid, PgDepend::Deptype deptype) {
    rows.push_back({classid, Oid{objid}, objsubid, refclassid, Oid{refobjid},
                    refobjsubid, deptype});
  };
  const auto in_schema = [&](const duckdb::StandardEntry& entry) {
    emit(CatalogClassOid(entry.type), entry.oid, 0, Oid{PgNamespace::kId},
         entry.ParentSchema().oid, 0, PgDepend::Deptype::Normal);
  };

  std::vector<const duckdb::TableCatalogEntry*> tables;
  containers::FlatHashMap<std::string, const duckdb::TableCatalogEntry*>
    tables_by_name;
  VisitEntries<duckdb::TableCatalogEntry>(
    context, database, [&](const duckdb::TableCatalogEntry& table) {
      in_schema(table);
      tables.push_back(&table);
      tables_by_name.emplace(
        absl::StrCat(table.ParentSchema().name.GetIdentifierName(), ".",
                     table.name.GetIdentifierName()),
        &table);
    });
  VisitEntries<duckdb::ViewCatalogEntry>(
    context, database, [&](const duckdb::ViewCatalogEntry& view) {
      in_schema(view);
      emit(Oid{PgRewrite::kId}, view.oid, 0, Oid{PgClass::kId}, view.oid, 0,
           PgDepend::Deptype::Internal);
    });
  VisitEntries<duckdb::SequenceCatalogEntry>(context, database, in_schema);
  VisitEntries<duckdb::TypeCatalogEntry>(context, database, in_schema);
  VisitEntries<duckdb::ScalarMacroCatalogEntry>(context, database, in_schema);
  VisitEntries<duckdb::TableMacroCatalogEntry>(context, database, in_schema);

  for (const auto* table : tables) {
    const auto& constraints = table->GetConstraints();
    for (size_t position = 0; position != constraints.size(); ++position) {
      if (constraints[position]->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& fk =
        constraints[position]->Cast<duckdb::ForeignKeyConstraint>();
      if (fk.info.type == duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE) {
        continue;
      }
      const auto referenced = tables_by_name.find(
        absl::StrCat(fk.info.schema.GetIdentifierName(), ".",
                     fk.info.table.GetIdentifierName()));
      const auto& target =
        referenced == tables_by_name.end() ? *table : *referenced->second;
      for (const auto key : fk.info.pk_keys) {
        emit(Oid{PgConstraint::kId}, ConstraintOid(table->oid, position), 0,
             Oid{PgClass::kId}, target.oid, static_cast<int32_t>(key.index + 1),
             PgDepend::Deptype::Normal);
      }
    }
  }

  std::vector<Edge> edges;
  if (auto manager = database.GetDependencyManager()) {
    manager->Scan(context, [&](duckdb::CatalogEntry& object,
                               duckdb::CatalogEntry& dependent,
                               const duckdb::DependencyDependentFlags& flags) {
      edges.push_back({&object, &dependent, flags.IsOwnedBy()});
    });
  }
  for (const auto& [object, dependent, owned_by] : edges) {
    using enum duckdb::CatalogType;
    switch (dependent->type) {
      case VIEW_ENTRY:
        emit(Oid{PgRewrite::kId}, dependent->oid, 0,
             CatalogClassOid(object->type), object->oid, 0,
             PgDepend::Deptype::Normal);
        break;
      case MACRO_ENTRY:
      case TABLE_MACRO_ENTRY:
        emit(Oid{PgProc::kId}, dependent->oid, 0, CatalogClassOid(object->type),
             object->oid, 0, PgDepend::Deptype::Normal);
        break;
      case INDEX_ENTRY: {
        const auto& index = dependent->Cast<duckdb::IndexCatalogEntry>();
        bool emitted = false;
        if (object->type == TABLE_ENTRY) {
          const auto& table = object->Cast<duckdb::TableCatalogEntry>();
          for (const auto column_id : index.column_ids) {
            if (const auto attnum = TableEntryAttnum(table, column_id)) {
              emit(Oid{PgClass::kId}, index.oid, 0, Oid{PgClass::kId},
                   table.oid, attnum, PgDepend::Deptype::Auto);
              emitted = true;
            }
          }
        }
        if (!emitted) {
          emit(Oid{PgClass::kId}, index.oid, 0, CatalogClassOid(object->type),
               object->oid, 0, PgDepend::Deptype::Auto);
        }
        break;
      }
      case SEQUENCE_ENTRY:
        if (owned_by) {
          emit(Oid{PgClass::kId}, dependent->oid, 0,
               CatalogClassOid(object->type), object->oid, 0,
               PgDepend::Deptype::Auto);
        }
        break;
      case TABLE_ENTRY: {
        const auto& table = dependent->Cast<duckdb::TableCatalogEntry>();
        if (object->type == TYPE_ENTRY) {
          for (const auto& column : table.GetColumns().Logical()) {
            if (column.Type().HasAlias() &&
                duckdb::Identifier{column.Type().GetAlias()} == object->name) {
              emit(Oid{PgClass::kId}, table.oid,
                   static_cast<int32_t>(column.Logical().index + 1),
                   Oid{PgType::kId}, object->oid, 0, PgDepend::Deptype::Normal);
            }
          }
        } else if (object->type == SEQUENCE_ENTRY) {
          for (const auto& column : table.GetColumns().Logical()) {
            if (column.HasDefaultValue() &&
                NamesSequence(column.DefaultValue(), object->name)) {
              emit(Oid{PgAttrdef::kId}, column.Oid(), 0, Oid{PgClass::kId},
                   object->oid, 0, PgDepend::Deptype::Normal);
            }
          }
        }
        break;
      }
      default:
        break;
    }
  }
  return rows;
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgDepend>::GetTableData() {
  auto values = CollectEdges(_context, GetDatabase());

  auto result = CreateColumns<PgDepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
