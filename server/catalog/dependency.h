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

#pragma once

#include <absl/functional/function_ref.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry_map.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <span>
#include <vector>

namespace sdb::catalog {

using EdgeVisitor = absl::FunctionRef<void(
  duckdb::CatalogEntry&, const duckdb::DependencyDependentFlags&)>;

void ScanSubjects(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& dependent, EdgeVisitor visit);

void ScanDependents(duckdb::CatalogTransaction transaction,
                    duckdb::CatalogEntry& subject, EdgeVisitor visit);

void DropDependents(duckdb::CatalogTransaction transaction,
                    duckdb::CatalogEntry& entry, bool cascade);

duckdb::catalog_entry_vector_t OwnedEntries(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owner);

duckdb::optional_ptr<duckdb::CatalogEntry> OwnerOf(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owned);

void RefuseDependents(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& entry,
  absl::FunctionRef<bool(const duckdb::CatalogEntry&)> refuses);

struct PendingDefault {
  duckdb::idx_t table = 0;
  duckdb::Identifier column;
  duckdb::unique_ptr<duckdb::ParsedExpression> value;
};

void FollowRename(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& entry,
                  const duckdb::Identifier& new_name,
                  std::vector<PendingDefault>& defaults);

void FollowSchemaMove(duckdb::CatalogTransaction transaction,
                      duckdb::CatalogEntry& entry,
                      const duckdb::Identifier& new_schema,
                      std::span<const duckdb::idx_t> moving,
                      std::vector<PendingDefault>& defaults);

void SetDefaults(duckdb::CatalogTransaction transaction,
                 duckdb::Catalog& catalog,
                 std::vector<PendingDefault>& defaults);

void FollowReferences(duckdb::CatalogTransaction transaction,
                      duckdb::CatalogEntry& table,
                      const duckdb::Identifier& old_schema,
                      const duckdb::Identifier& old_name);

duckdb::idx_t ReferencedTableOid(const duckdb::TableCatalogEntry& table,
                                 const duckdb::ForeignKeyConstraint& fk);

void FollowColumnRename(duckdb::CatalogTransaction transaction,
                        duckdb::CatalogEntry& table,
                        const duckdb::Identifier& old_column,
                        const duckdb::Identifier& new_column);

void FollowSchemaRename(duckdb::CatalogTransaction transaction,
                        duckdb::CatalogEntry& schema,
                        const duckdb::Identifier& new_name,
                        std::vector<PendingDefault>& defaults);

// Re-links the sequences `previous` owned through a column that `current`
// keeps under a new oid.
void FollowColumnOids(duckdb::CatalogTransaction transaction,
                      const duckdb::TableCatalogEntry& previous,
                      const duckdb::TableCatalogEntry& current);

}  // namespace sdb::catalog
