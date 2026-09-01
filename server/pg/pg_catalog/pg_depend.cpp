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

#include <duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "catalog/table.h"
#include "pg/pg_catalog/pg_attrdef.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_constraint.h"
#include "pg/pg_catalog/pg_database.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_rewrite.h"
#include "pg/pg_catalog/pg_type.h"

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
    default:
      return Oid{PgClass::kId};
  }
}

namespace {

using catalog::Permissions;
using duckdb::CreateTableInfo;

// A relation, a function or a type as pg_depend names the referenced side of
// an edge pointing at it.
struct Referenced {
  ObjectId id;
  ObjectId schema_id;
  ObjectId owner_table_id;
  Oid classid;
};

std::vector<PgDepend> CollectEdges(duckdb::ClientContext* context,
                                   ObjectId db_id) {
  std::vector<PgDepend> edges;
  containers::FlatHashMap<ObjectId, const catalog::SereneDBTableEntry*> tables;
  catalog::Visit<catalog::SereneDBTableEntry>(
    context, db_id, [&](const catalog::SereneDBTableEntry& table) {
      tables.emplace(catalog::IdOf(table), &table);
    });
  std::vector<const duckdb::ViewCatalogEntry*> views;
  catalog::Visit<catalog::SereneDBViewEntry>(
    context, db_id,
    [&](const duckdb::ViewCatalogEntry& view) { views.push_back(&view); });
  std::vector<Referenced> functions;
  catalog::VisitFunctions(
    context, db_id, [&](const duckdb::MacroCatalogEntry& function) {
      functions.push_back({ObjectId{function.oid},
                           ObjectId{function.ParentSchema().oid}, ObjectId{},
                           Oid{PgProc::kId}});
    });
  // A sequence and a user type are referenced sides of their own: a DEFAULT
  // names a sequence and a column's declared type names a type, and neither row
  // is reachable from the dependent's side of the graph.
  std::vector<Referenced> sequences;
  catalog::Visit<catalog::SereneDBSequenceEntry>(
    context, db_id, [&](const catalog::SereneDBSequenceEntry& sequence) {
      sequences.push_back({ObjectId{sequence.oid},
                           ObjectId{sequence.ParentSchema().oid},
                           sequence.GetOwnerTableId(), Oid{PgClass::kId}});
    });
  std::vector<Referenced> types;
  catalog::Visit<catalog::SereneDBTypeEntry>(
    context, db_id, [&](const duckdb::TypeCatalogEntry& type) {
      types.push_back({ObjectId{type.oid}, ObjectId{type.ParentSchema().oid},
                       ObjectId{}, Oid{PgType::kId}});
    });
  auto dependents = catalog::EdgeAttachments(*context);
  const auto attnum = [](const catalog::SereneDBTableEntry& table,
                         ObjectId col) -> int32_t {
    const auto* column = catalog::ColumnById(table.GetColumns(), col);
    return column ? static_cast<int32_t>(column->Logical().index) + 1 : 0;
  };
  const auto emit = [&](ObjectId dependent, int32_t dependent_sub,
                        Oid dependent_class, ObjectId referenced,
                        int32_t referenced_sub, Oid referenced_class,
                        PgDepend::Deptype deptype) {
    edges.push_back({dependent_class, Oid{dependent.id()}, dependent_sub,
                     referenced_class, Oid{referenced.id()}, referenced_sub,
                     deptype});
  };
  // The graph half: one row per recorded edge, with the class of the dependent
  // read off its own entry. Constraint and index dependents and the foreign-key
  // back-edge are deliberately not projected -- pg_depend has no row for them
  // today; the FK rows below are synthesized from the referencing table's own
  // list instead, and role references belong to pg_shdepend.
  const auto emit_graph = [&](ObjectId ref, Oid referenced_class) {
    dependents.ScanDependents(
      catalog::DependencyInfo(ref),
      [&](duckdb::optional_ptr<duckdb::CatalogEntry> entry,
          duckdb::DependencyEntry& edge) {
        const auto id = catalog::DependencyInfoId(edge.EntryInfo());
        if (!entry || !id.isSet()) {
          return;
        }
        switch (entry->type) {
          using enum duckdb::CatalogType;
          case VIEW_ENTRY:
            // A view depends through its rewrite rule, which is the class
            // postgres names on the dependent side.
            emit(id, 0, Oid{PgRewrite::kId}, ref, 0, referenced_class,
                 PgDepend::Deptype::Normal);
            break;
          case MACRO_ENTRY:
          case TABLE_MACRO_ENTRY:
            emit(id, 0, Oid{PgProc::kId}, ref, 0, referenced_class,
                 PgDepend::Deptype::Normal);
            break;
          case INDEX_ENTRY: {
            // An expression index naming a function. An inverted index also
            // names its dictionaries, which postgres has no class for -- but a
            // dictionary is never a referenced side here. The relation the
            // index is on is one of its edges too; postgres addresses that one
            // per covered column, which emit_indexes below does.
            const auto* index =
              catalog::EntryOf<catalog::SereneDBIndexEntry>(entry);
            if (index != nullptr && index->GetRelationId() == ref) {
              break;
            }
            emit(id, 0, Oid{PgClass::kId}, ref, 0, referenced_class,
                 PgDepend::Deptype::Auto);
            break;
          }
          case TABLE_ENTRY: {
            const auto table = tables.find(id);
            if (table == tables.end()) {
              break;
            }
            for (const auto& piece : edge.Dependent().pieces) {
              const ObjectId sub{piece.sub_object};
              switch (piece.kind) {
                case duckdb::DependencyPieceKind::COLUMN_DEFAULT:
                  emit(sub, 0, Oid{PgAttrdef::kId}, ref, 0, referenced_class,
                       PgDepend::Deptype::Normal);
                  break;
                case duckdb::DependencyPieceKind::CHECK:
                  emit(sub, 0, Oid{PgConstraint::kId}, ref, 0, referenced_class,
                       PgDepend::Deptype::Normal);
                  break;
                case duckdb::DependencyPieceKind::COLUMN_TYPE:
                  // A column's declared type: pg_depend addresses it as the
                  // table plus an attnum, not as the column object.
                  emit(id, attnum(*table->second, sub), Oid{PgClass::kId}, ref,
                       0, referenced_class, PgDepend::Deptype::Normal);
                  break;
                case duckdb::DependencyPieceKind::FOREIGN_KEY:
                case duckdb::DependencyPieceKind::NONE:
                  break;
              }
            }
            break;
          }
          default:
            break;
        }
      });
  };
  // The indexes over one relation, as pg_depend addresses them. Read off the
  // relation's own schema rather than out of a walk: the callers below iterate
  // what they already collected, so no set is open here.
  const auto emit_indexes = [&](ObjectId schema_id, ObjectId ref,
                                const catalog::SereneDBTableEntry* table,
                                ObjectId pk_index) {
    for (const auto& index :
         catalog::RelationIndexRecords(context, schema_id, ref)) {
      const auto idx = index->GetId();
      if (idx == pk_index || index->ReferencesColumn(catalog::kGeneratedPKId)) {
        continue;
      }
      bool emitted = false;
      if (table != nullptr) {
        for (auto col : index->GetColumns()) {
          if (auto sub = attnum(*table, col)) {
            emit(idx, 0, Oid{PgClass::kId}, ref, sub, Oid{PgClass::kId},
                 PgDepend::Deptype::Auto);
            emitted = true;
          }
        }
      }
      if (!emitted) {
        emit(idx, 0, Oid{PgClass::kId}, ref, 0, Oid{PgClass::kId},
             PgDepend::Deptype::Auto);
      }
    }
  };
  for (const auto& [table_id, table] : tables) {
    emit(table_id, 0, Oid{PgClass::kId}, catalog::ParentIdOf(*table), 0,
         Oid{PgNamespace::kId}, PgDepend::Deptype::Normal);
    emit_graph(table_id, Oid{PgClass::kId});
    const auto* pk = catalog::TablePrimaryKey(table->GetConstraints());
    emit_indexes(catalog::ParentIdOf(*table), table_id, table,
                 pk == nullptr ? ObjectId{} : ObjectId{pk->host_index_id});
    for (const auto& constraint : table->GetConstraints()) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
      const ObjectId referenced{fk.host_referenced_id};
      if (!catalog::StatesForeignKey(fk) || !referenced.isSet() ||
          referenced == table_id) {
        continue;
      }
      const auto ref_held = tables.find(referenced);
      for (const auto column_id : fk.host_pk_column_ids) {
        emit(ObjectId{fk.oid}, 0, Oid{PgConstraint::kId}, referenced,
             ref_held == tables.end()
               ? 0
               : attnum(*ref_held->second, ObjectId{column_id}),
             Oid{PgClass::kId}, PgDepend::Deptype::Normal);
      }
    }
    for (const auto& column : table->GetColumns().Logical()) {
      if (!column.HasDefaultValue() && !column.Generated()) {
        continue;
      }
      const ObjectId column_id{column.CatalogOid()};
      if (auto sub = attnum(*table, column_id)) {
        emit(column_id, 0, Oid{PgAttrdef::kId}, table_id, sub,
             Oid{PgClass::kId}, PgDepend::Deptype::Auto);
      }
    }
  }
  for (const auto& view : views) {
    const auto ref = catalog::IdOf(*view);
    emit(ref, 0, Oid{PgClass::kId}, catalog::ParentIdOf(*view), 0,
         Oid{PgNamespace::kId}, PgDepend::Deptype::Normal);
    emit_graph(ref, Oid{PgClass::kId});
    emit_indexes(catalog::ParentIdOf(*view), ref, nullptr, ObjectId{});
    emit(ref, 0, Oid{PgRewrite::kId}, ref, 0, Oid{PgClass::kId},
         PgDepend::Deptype::Internal);
  }
  const auto emit_referenced = [&](const std::vector<Referenced>& list) {
    for (const auto& object : list) {
      emit(object.id, 0, object.classid, object.schema_id, 0,
           Oid{PgNamespace::kId}, PgDepend::Deptype::Normal);
      emit_graph(object.id, object.classid);
      // A sequence a relation owns goes with it, which postgres records as an
      // AUTO dependency on the owning table rather than on its schema.
      if (object.owner_table_id.isSet()) {
        emit(object.id, 0, object.classid, object.owner_table_id, 0,
             Oid{PgClass::kId}, PgDepend::Deptype::Auto);
      }
    }
  };
  emit_referenced(functions);
  emit_referenced(sequences);
  emit_referenced(types);
  return edges;
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgDepend>::GetTableData() {
  auto values = CollectEdges(&_config.GetClientContext(), GetDatabaseId());

  auto result = CreateColumns<PgDepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
