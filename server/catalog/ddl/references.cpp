////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "catalog/ddl/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/read/duckdb_catalog_sets.h"

namespace sdb::catalog {
namespace {

// Every catalog entry a bind of `expr` looks up in the table's own catalog,
// filed on the table with the piece that states it. The bind is duckdb's --
// column refs resolve against the table's columns the way a CHECK binds -- so
// what a nextval string, a function call or a CAST names is recorded by the
// binder's own lookup, once, here.
void CollectExpressionDeps(duckdb::ClientContext& context,
                           duckdb::CreateTableInfo& table,
                           const duckdb::ParsedExpression& expr,
                           duckdb::DependencyPiece piece) {
  auto binder = duckdb::Binder::CreateBinder(context);
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, table.GetQualifiedName().Catalog());
  binder->EntryRetriever().SetCallback([&](duckdb::CatalogEntry& entry) {
    if (&entry.ParentCatalog() != &catalog) {
      return;
    }
    switch (entry.type) {
      using enum duckdb::CatalogType;
      case SEQUENCE_ENTRY:
      case MACRO_ENTRY:
      case TABLE_MACRO_ENTRY:
      case TYPE_ENTRY: {
        duckdb::LogicalDependency dep{entry};
        dep.pieces.push_back(piece);
        table.dependencies.AddDependency(dep);
        return;
      }
      default:
        return;
    }
  });
  auto copy = expr.Copy();
  duckdb::physical_index_set_t bound_columns;
  duckdb::CheckBinder expr_binder(*binder, context, table.GetTableName(),
                                  table.columns, bound_columns);
  expr_binder.Bind(copy);
}

// The DEFAULT or the generated-column body, read-only; null when the column
// has neither.
const duckdb::ParsedExpression* ColumnExpression(
  const duckdb::ColumnDefinition& column) {
  if (column.Generated()) {
    return &column.GeneratedExpression();
  }
  return column.HasDefaultValue() ? &column.DefaultValue() : nullptr;
}

}  // namespace

void RefreshExpressionReferences(duckdb::ClientContext* context,
                                 duckdb::CreateTableInfo& table) {
  if (context == nullptr) {
    return;
  }
  duckdb::LogicalDependencyList kept;
  for (const auto& dep : table.dependencies.Set()) {
    if (dep.pieces.empty()) {
      kept.AddDependency(dep);
    }
  }
  table.dependencies = std::move(kept);

  for (idx_t i = 0; i < table.columns.LogicalColumnCount(); ++i) {
    const auto& column = table.columns.GetColumn(duckdb::LogicalIndex{i});
    if (const auto* expression = ColumnExpression(column)) {
      CollectExpressionDeps(
        *context, table, *expression,
        duckdb::DependencyPiece{duckdb::DependencyPieceKind::COLUMN_DEFAULT,
                                column.CatalogOid()});
    }
  }
  for (const auto& constraint : table.constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      CollectExpressionDeps(
        *context, table,
        *constraint->Cast<duckdb::CheckConstraint>().expression,
        duckdb::DependencyPiece{duckdb::DependencyPieceKind::CHECK,
                                constraint->oid});
    }
  }
}

std::optional<ObjectId> TryFindSchemaId(duckdb::ClientContext* context,
                                        ObjectId database_id,
                                        std::string_view name) {
  const auto id = catalog::FindSchemaId(context, database_id, name);
  return id.isSet() ? std::optional{id} : std::nullopt;
}

}  // namespace sdb::catalog
