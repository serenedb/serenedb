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

#include <atomic>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/database_directory.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry/subscription.h"
#include "catalog/snapshot.h"

namespace duckdb {

class PhysicalPlanGenerator;
class PhysicalOperator;
class LogicalInsert;
class LogicalCreateTable;
class LogicalMergeInto;

}  // namespace duckdb
namespace sdb::catalog {

class SereneDBCatalog final : public BaseCatalog {
 public:
  static constexpr const char* kStorageType = "serenedb";

  SereneDBCatalog(duckdb::AttachedDatabase& db,
                  std::shared_ptr<DatabaseDirectory> directory, bool durable)
    : BaseCatalog{db, true},
      _directory{std::move(directory)},
      _durable{durable} {}

  const std::shared_ptr<DatabaseDirectory>& Directory() const noexcept {
    return _directory;
  }
  CatalogSnapshots& Snapshots() noexcept { return _snapshots; }
  EntrySet& Servers() noexcept { return _servers; }
  EntrySet& Subscriptions() noexcept { return _subscriptions; }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSubscription(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSubscriptionInfo& info);
  void DropSubscription(duckdb::CatalogTransaction transaction,
                        duckdb::DropInfo& info);

  EntrySet& Set(duckdb::CatalogType type) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateForeignServer(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateForeignServerInfo& info) final;
  void DropForeignServer(duckdb::CatalogTransaction transaction,
                         duckdb::DropInfo& info) final;

  void ReplayCreate(duckdb::ClientContext& context,
                    duckdb::unique_ptr<duckdb::CreateInfo> info) final;
  void ReplayDrop(duckdb::ClientContext& context,
                  duckdb::CatalogEntry& entry) final;

  std::string GetCatalogType() final { return kStorageType; }

  duckdb::SqlCompatibility Compatibility() const final {
    return duckdb::SqlCompatibility::POSTGRES;
  }

  bool SupportsMultipleDMLCTEs() const final { return true; }
  bool LogsSequences() const final { return _durable; }
  void LogSequenceValue(const duckdb::SequenceValue& value) final;
  bool IsDropped() const { return _detached.load(std::memory_order_acquire); }
  bool Durable() const final { return _durable; }
  bool InMemory() final { return !_durable; }
  std::string GetDBPath() final;

  void Initialize(bool load_builtin) final;

  void OnDetach(duckdb::ClientContext& context) final;

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  duckdb::optional<duckdb::Identifier> GetDefaultSchema() const final {
    return duckdb::Identifier{GetAttached().IsTemporary()
                                ? irs::StaticStrings::kPgTempSchema
                                : irs::StaticStrings::kPublic};
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) final;

  duckdb::unique_ptr<duckdb::CatalogEntry> MakeTable(
    duckdb::CatalogTransaction transaction, Schema& schema,
    duckdb::BoundCreateTableInfo& info) final;

  duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndex(
    duckdb::CatalogTransaction transaction, Schema& schema,
    duckdb::CreateIndexInfo& info, duckdb::CatalogEntry& relation) final;

  duckdb::PhysicalOperator& PlanInsert(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalInsert& op,
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) final;

  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanUpdate(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalUpdate& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndex(
    duckdb::Binder& binder, duckdb::CreateStatement& stmt,
    duckdb::TableCatalogEntry& table,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateViewIndex(
    duckdb::Binder& binder, duckdb::CreateStatement& stmt,
    duckdb::ViewCatalogEntry& view,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) final;

  void BindIndexDefinition(duckdb::Binder& binder,
                           duckdb::CreateStatement& stmt,
                           duckdb::CatalogEntry& target);

  void RefuseUnsupportedAlter(duckdb::ClientContext& context,
                              duckdb::AlterInfo& info);

  duckdb::unique_ptr<duckdb::LogicalOperator> BindAlterAddIndex(
    duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
    duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) final;

  duckdb::ErrorData SupportsCreateTable(
    duckdb::BoundCreateTableInfo& info) final;

 private:
  std::shared_ptr<DatabaseDirectory> _directory;
  CatalogSnapshots _snapshots{*this};
  EntrySet _servers{true, _oids};
  EntrySet _subscriptions{true, _oids};
  bool _durable;
  std::atomic_bool _detached{false};
};

duckdb::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> SereneDBDatabases(
  duckdb::DatabaseManager& manager);

void ForEachSearchStorage(
  SereneDBCatalog& catalog, absl::FunctionRef<void(SearchTableEntry&)> table,
  std::optional<absl::FunctionRef<void(InvertedIndexEntry&)>> index);

std::vector<std::shared_ptr<search::Store>> SearchTableStorages(
  SereneDBCatalog& catalog);

duckdb::optional_ptr<SereneDBCatalog> DefaultSereneDBCatalog(
  duckdb::ClientContext& context);

bool IsSereneDB(duckdb::Catalog& catalog);

void ThrowIfReadOnly(const duckdb::Catalog& catalog, const auto&... command) {
  if (catalog.GetAttached().IsReadOnly()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                    ERR_MSG("cannot run ", command..., " on database \"",
                            catalog.GetName().GetIdentifierName(),
                            "\": it is attached read-only"));
  }
}

}  // namespace sdb::catalog
