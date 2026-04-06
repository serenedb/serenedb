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

#include "catalog/catalog.h"
#include "connector/duckdb_schema_entry.h"

namespace sdb::connector {

class SereneDBCatalog final : public duckdb::Catalog {
 public:
  explicit SereneDBCatalog(duckdb::AttachedDatabase& db);

  std::string GetCatalogType() override { return "serenedb"; }
  void Initialize(bool load_builtin) override;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) override;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& schema_lookup,
    duckdb::OnEntryNotFound if_not_found) override;

  void ScanSchemas(
    duckdb::ClientContext& context,
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) override;

  void DropSchema(duckdb::ClientContext& context,
                  duckdb::DropInfo& info) override;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) override;

  duckdb::PhysicalOperator& PlanInsert(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalInsert& op,
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;

  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) override;

  duckdb::PhysicalOperator& PlanUpdate(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalUpdate& op,
                                       duckdb::PhysicalOperator& plan) override;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndex(
    duckdb::Binder& binder, duckdb::CreateStatement& stmt,
    duckdb::TableCatalogEntry& table,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) override;

  duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext& context) override;
  bool InMemory() override { return false; }
  std::string GetDBPath() override { return ""; }

 private:
  duckdb::unique_ptr<SereneDBSchemaEntry> _default_schema;
};

}  // namespace sdb::connector
