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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>

#include "catalog/identifiers/object_id.h"

namespace sdb::connector {

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info);

class SereneDBCatalog final : public duckdb::Catalog {
 public:
  SereneDBCatalog(duckdb::AttachedDatabase& db, ObjectId database_id);

  ObjectId GetDatabaseId() const { return _database_id; }

  std::string GetCatalogType() final { return "serenedb"; }
  std::string GetDefaultSchema() const final { return "public"; }
  void Initialize(bool load_builtin) final;

  duckdb::optional_idx GetCatalogVersion(duckdb::ClientContext& context) final;

  duckdb::ErrorData SupportsCreateTable(
    duckdb::BoundCreateTableInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) final;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& schema_lookup,
    duckdb::OnEntryNotFound if_not_found) final;

  void ScanSchemas(
    duckdb::ClientContext& context,
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) final;

  void DropSchema(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  void OnDetach(duckdb::ClientContext& context) final;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanInsert(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalInsert& op,
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) final;

  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanUpdate(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalUpdate& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanMergeInto(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndex(
    duckdb::Binder& binder, duckdb::CreateStatement& stmt,
    duckdb::CatalogEntry& table,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindAlterAddIndex(
    duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
    duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) final;

  duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext& context) final;

  bool InMemory() final { return false; }

  std::string GetDBPath() final { return ""; }

 private:
  ObjectId _database_id;
};

}  // namespace sdb::connector
