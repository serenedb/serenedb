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

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <functional>
#include <string>

#include "catalog1/entry/foreign_server.h"
#include "catalog1/entry/tokenizer.h"

namespace duckdb {

class PhysicalPlanGenerator;
class PhysicalOperator;
class LogicalInsert;
struct DropInfo;

}  // namespace duckdb
namespace sdb::catalog {

class SereneDBCatalog : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "serenedb";

  explicit SereneDBCatalog(duckdb::AttachedDatabase& db);

  std::string GetCatalogType() override { return kStorageType; }

  void Initialize(bool load_builtin) override;

  std::string GetDefaultSchema() const override;

  duckdb::unique_ptr<duckdb::IndexCatalogEntry> MakeIndexEntry(
    duckdb::DuckSchemaEntry& schema, duckdb::CreateIndexInfo& info,
    duckdb::TableCatalogEntry& table) override;

  duckdb::unique_ptr<duckdb::TableCatalogEntry> MakeTableEntry(
    duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
    duckdb::BoundCreateTableInfo& info) override;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchemaById(
    duckdb::optional_ptr<duckdb::ClientContext> context, duckdb::idx_t id);

  duckdb::optional_ptr<duckdb::CatalogEntry> FindEntryById(
    duckdb::optional_ptr<duckdb::ClientContext> context,
    duckdb::CatalogType type, duckdb::idx_t id);

  template<typename T>
  duckdb::optional_ptr<T> FindIn(
    duckdb::optional_ptr<duckdb::ClientContext> context, duckdb::idx_t id) {
    auto entry = FindEntryById(context, T::Type, id);
    return entry ? &entry->template Cast<T>() : nullptr;
  }

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
    duckdb::CatalogEntry& table,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) override;

  duckdb::ErrorData SupportsCreateTable(
    duckdb::BoundCreateTableInfo& info) override;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTokenizer(
    duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
    CreateTokenizerInfo& info);

  void DropTokenizer(duckdb::ClientContext& context, duckdb::DropInfo& info);

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateForeignServer(
    duckdb::CatalogTransaction transaction, CreateForeignServerInfo& info);

  bool DropForeignServer(duckdb::CatalogTransaction transaction,
                         const duckdb::Identifier& name, bool cascade);

  void ScanForeignServers(
    duckdb::CatalogTransaction transaction,
    const std::function<void(duckdb::CatalogEntry&)>& callback);

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupForeignServer(
    duckdb::CatalogTransaction transaction, const duckdb::Identifier& name);

 private:
  duckdb::CatalogSet _foreign_servers;
};

}  // namespace sdb::catalog
