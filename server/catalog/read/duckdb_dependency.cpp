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

#include "catalog/read/duckdb_dependency.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/down_cast.h"
#include "catalog/index.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

// The transaction one catalog's edges are written through. A null context is
// boot, WAL replay or a background drop: no transaction of its own, and
// everything it may see is committed.
duckdb::CatalogTransaction EdgeTransaction(duckdb::ClientContext* context,
                                           duckdb::Catalog& owner) {
  if (context != nullptr && context->transaction.HasActiveTransaction()) {
    return owner.GetCatalogTransaction(*context);
  }
  return duckdb::CatalogTransaction::GetCommittedTransaction(
    owner.GetAttached().GetDatabase());
}

// The subjects a table's own definition names by id: the user types its
// columns are declared as, and the table each foreign key points at. Filed
// under the piece that names them, so a cascade can trim the column or the
// key instead of taking the table whole.
void AddDefinitionEdges(
  const duckdb::CreateTableInfo& table,
  absl::FunctionRef<void(const duckdb::LogicalDependency&)> add) {
  const auto edge = [&](ObjectId subject, duckdb::DependencyPiece piece) {
    if (!subject.isSet()) {
      return;
    }
    duckdb::LogicalDependency dep{nullptr, DependencyInfo(subject),
                                  duckdb::Identifier{}};
    dep.pieces.push_back(piece);
    add(dep);
  };
  for (idx_t i = 0; i < table.columns.LogicalColumnCount(); ++i) {
    const auto& column = table.columns.GetColumn(duckdb::LogicalIndex{i});
    std::vector<ObjectId> type_ids;
    CollectTypeIds(column.Type(), type_ids);
    for (const auto type_id : type_ids) {
      edge(type_id,
           duckdb::DependencyPiece{duckdb::DependencyPieceKind::COLUMN_TYPE,
                                   column.CatalogOid()});
    }
  }
  for (const auto& constraint : table.constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    // Only the key this table states: the reciprocal entry the referenced
    // table holds names the referencing one, and an edge there would refuse
    // the drop of a table nothing actually depends on.
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
      continue;
    }
    edge(ObjectId{fk.host_referenced_id},
         duckdb::DependencyPiece{duckdb::DependencyPieceKind::FOREIGN_KEY,
                                 constraint->oid});
  }
}

}  // namespace

duckdb::CatalogEntryInfo DependencyInfo(ObjectId id) {
  return duckdb::CatalogEntryInfo{duckdb::CatalogType::INVALID,
                                  duckdb::Identifier{}, duckdb::Identifier{},
                                  duckdb::Identifier{}, id.id()};
}

ObjectId DependencyInfoId(const duckdb::CatalogEntryInfo& info) noexcept {
  return ObjectId{info.oid};
}

duckdb::LogicalDependencyList EntryDependencies(
  const duckdb::CreateInfo& info) {
  // What the body resolved to when this version was written, which duckdb
  // carries on the info itself -- per overload for a function, whose set is
  // the union over the survivors -- and every record therefore round-trips.
  // What the definition states outright -- a column's type, a foreign key's
  // referenced table, the relation an index is on -- is derived here instead,
  // so it is never written down: a version placed by a statement and one
  // replayed from a record derive the same edges, and a piece a record still
  // carries is ignored.
  duckdb::LogicalDependencyList out;
  const auto self = catalog::IdOf(info);
  const auto add = [&](const duckdb::LogicalDependency& dep) {
    // A recursive body binds its own previous version; an object is not its
    // own dependent.
    if (!self.isSet() || ObjectId{dep.entry.oid} != self) {
      out.AddDependency(dep);
    }
  };
  for (const auto& dep : info.dependencies.Set()) {
    duckdb::vector<duckdb::DependencyPiece> stated;
    for (const auto& piece : dep.pieces) {
      switch (piece.kind) {
        using enum duckdb::DependencyPieceKind;
        case COLUMN_TYPE:
        case FOREIGN_KEY:
          break;
        case NONE:
        case COLUMN_DEFAULT:
        case CHECK:
          stated.push_back(piece);
          break;
      }
    }
    if (dep.pieces.empty() || !stated.empty()) {
      auto kept = dep;
      kept.pieces = std::move(stated);
      add(kept);
    }
  }
  if (info.type == duckdb::CatalogType::TABLE_ENTRY) {
    AddDefinitionEdges(basics::downCast<const duckdb::CreateTableInfo>(info),
                       add);
  }
  if (info.type == duckdb::CatalogType::INDEX_ENTRY) {
    // Postgres' AUTO dependency: duckdb's cascade takes the index when the
    // relation goes, without CASCADE being asked for. Stated per edge, so what
    // an expression names -- a function, a dictionary -- still blocks.
    const auto relation =
      basics::downCast<const CreateIndexInfo>(info).GetRelationId();
    if (relation.isSet()) {
      duckdb::LogicalDependency dep{nullptr, DependencyInfo(relation),
                                    duckdb::Identifier{}};
      dep.automatic = true;
      add(dep);
    }
  }
  if (info.type == duckdb::CatalogType::MACRO_ENTRY ||
      info.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    const auto& macros =
      basics::downCast<const duckdb::CreateMacroInfo>(info).macros;
    for (const auto& dep :
         duckdb::MacroFunction::UnionDependencies(macros).Set()) {
      add(dep);
    }
    // The typed signatures resolve before any collecting bind runs, so their
    // ids are read off the types themselves.
    std::vector<ObjectId> type_ids;
    for (const auto& macro : macros) {
      if (!macro) {
        continue;
      }
      for (const auto& t : macro->types) {
        CollectTypeIds(t, type_ids);
      }
      for (const auto& t : macro->return_types) {
        CollectTypeIds(t, type_ids);
      }
    }
    for (const auto type_id : type_ids) {
      if (type_id.isSet()) {
        out.AddDependency(duckdb::LogicalDependency{
          nullptr, DependencyInfo(type_id), duckdb::Identifier{}});
      }
    }
  }
  return out;
}

void SetEntryDependencies(duckdb::ClientContext* context,
                          duckdb::CatalogEntry& entry,
                          const duckdb::LogicalDependencyList& deps) try {
  auto& owner = entry.ParentCatalog();
  if (auto manager = owner.GetDependencyManager()) {
    manager->AddObject(EdgeTransaction(context, owner), entry, deps);
  }
} catch (const duckdb::TransactionException&) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent DDL on a dependency "
            "of the same object"));
}

duckdb::DependencyManager::Attachments EdgeAttachments(
  duckdb::ClientContext& context) {
  return duckdb::DependencyManager::Attachments{context, /*skip=*/nullptr};
}

void VisitAllEdges(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(ObjectId referenced, ObjectId dependent)> visitor) {
  EdgeAttachments(context).ScanAllEdges([&](duckdb::DependencyEntry& dep) {
    const auto referenced = DependencyInfoId(dep.SourceInfo());
    const auto dependent = DependencyInfoId(dep.EntryInfo());
    if (referenced.isSet() && dependent.isSet()) {
      visitor(referenced, dependent);
    }
  });
}

}  // namespace sdb::catalog
