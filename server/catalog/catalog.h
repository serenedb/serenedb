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

#include <atomic>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string>
#include <utility>

#include "catalog/database_directory.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/tokenizer.h"

namespace duckdb {

class PhysicalPlanGenerator;
class PhysicalOperator;
class LogicalInsert;
class LogicalCreateTable;
class LogicalMergeInto;
struct DropInfo;

}  // namespace duckdb
namespace sdb::catalog {

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type =
                       duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);

class SereneDBCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "serenedb";

  SereneDBCatalog(duckdb::AttachedDatabase& db,
                  std::shared_ptr<DatabaseDirectory> directory)
    : duckdb::DuckCatalog{db, true}, _directory{std::move(directory)} {}

  const std::shared_ptr<DatabaseDirectory>& Directory() const noexcept {
    return _directory;
  }

  std::string GetCatalogType() final { return kStorageType; }

  duckdb::SqlCompatibility Compatibility() const final {
    return duckdb::SqlCompatibility::POSTGRES;
  }

  bool UsesCatalogLog() const final { return true; }
  bool IsDropped() const final {
    return _detached.load(std::memory_order_acquire);
  }
  duckdb::shared_ptr<duckdb::WriteAheadLog> CatalogLog() final;
  void RequestCatalogLogSync(duckdb::shared_ptr<duckdb::WriteAheadLog> log,
                             duckdb::idx_t offset) final;
  bool AppendLocalIndexes(
    duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
    duckdb::RowGroupCollection& source,
    const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
    duckdb::row_t row_start, duckdb::ErrorData& error) final;

  void Initialize(bool load_builtin) final;
  duckdb::idx_t DefaultSchemaOid() const final;

  void OnDetach(duckdb::ClientContext& context) final;

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  duckdb::optional<duckdb::Identifier> GetDefaultSchema() const final {
    return duckdb::Identifier{irs::StaticStrings::kPublic};
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) final;

  duckdb::unique_ptr<duckdb::IndexCatalogEntry> MakeIndexEntry(
    duckdb::DuckSchemaEntry& schema, duckdb::CreateIndexInfo& info,
    duckdb::CatalogEntry& relation) final;

  duckdb::unique_ptr<duckdb::TableCatalogEntry> MakeTableEntry(
    duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
    duckdb::BoundCreateTableInfo& info) final;

  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeForeignServerEntry(
    duckdb::CreateForeignServerInfo& info) final {
    return duckdb::make_uniq<ForeignServerCatalogEntry>(*this, info);
  }

  duckdb::unique_ptr<duckdb::StandardEntry> MakeTokenizerEntry(
    duckdb::DuckSchemaEntry& schema, duckdb::CreateTokenizerInfo& info) final {
    return duckdb::make_uniq<TokenizerCatalogEntry>(*this, schema, info);
  }

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchemaById(
    duckdb::ClientContext& context, duckdb::idx_t id);

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
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) final;

  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanMergeInto(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) final;

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

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTokenizer(
    duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
    duckdb::CreateTokenizerInfo& info);

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateForeignServer(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateForeignServerInfo& info);

  void DropForeignServer(duckdb::CatalogTransaction transaction,
                         duckdb::DropInfo& info);

 private:
  std::shared_ptr<DatabaseDirectory> _directory;
  std::atomic_bool _detached{false};
};

}  // namespace sdb::catalog
