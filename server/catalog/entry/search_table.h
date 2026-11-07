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

#include <array>
#include <cstdint>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/table_column.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/comment_on_column_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry_set.h"
#include "catalog/persistence/search_table.h"
#include "connector/search_table_backfill.h"
#include "query/config_variable_names.h"
#include "search/key_spec.h"

namespace duckdb {

class ClientContext;
struct CreateInfo;
struct CreateTableInfo;
struct BoundCreateTableInfo;

}  // namespace duckdb
namespace irs {

class DirectoryReader;

}  // namespace irs
namespace sdb::search {

class Store;

}  // namespace sdb::search
namespace sdb::catalog {

inline constexpr auto kSearchTableMaintenanceSettings = std::to_array({
  kCompactionIntervalSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
});

inline constexpr auto kSearchTableSettings = std::to_array({
  kCompactionIntervalSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
  kRowGroupSizeSetting,
  kSegmentMemoryMaxSetting,
});

inline constexpr auto kSearchTableOptions = std::to_array({
  kCompactionIntervalSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
  kRowGroupSizeSetting,
  kSegmentMemoryMaxSetting,
  kOptimizeTopKSetting,
});

void RequireSearchTableIndexOption(std::string_view name);

struct TableOption {
  std::string_view name;
  duckdb::Value value;
};

using persistence::SearchTableOptions;

class SearchTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SearchTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::BoundCreateTableInfo& info,
                   duckdb::CatalogTransaction transaction,
                   std::shared_ptr<search::Store> inherited_storage = {},
                   std::shared_ptr<EntrySet> triggers = {});

  const duckdb::ColumnList& GetColumns() const final { return _columns; }

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext&, duckdb::column_t) final {
    return nullptr;
  }

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext&) final;

  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) final;

  static duckdb::vector<duckdb::ColumnSegmentInfo> ColumnSegmentRows(
    const irs::DirectoryReader& reader, const duckdb::TableCatalogEntry& table,
    duckdb::column_t key_column);

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;

  duckdb::Value Option(std::string_view name) const;
  std::vector<TableOption> ExplicitOptions() const;

  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;

  void InternalAppend(duckdb::ClientContext& context,
                      duckdb::ColumnDataCollection& collection) final;

  void BeforeDrop(duckdb::CatalogTransaction transaction) final;
  void OnDrop() final;

  void Rollback(duckdb::CatalogEntry& prev_entry) final;

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTrigger(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTriggerInfo& info) final;
  void ScanTriggers(
    duckdb::CatalogTransaction transaction,
    const std::function<void(duckdb::CatalogEntry&)>& callback) const final;
  duckdb::optional_ptr<duckdb::CatalogEntry> GetTrigger(
    duckdb::CatalogTransaction transaction,
    const duckdb::Identifier& name) const final;
  bool DropTrigger(duckdb::CatalogTransaction transaction,
                   const duckdb::Identifier& name, bool cascade) final;
  duckdb::vector<std::reference_wrapper<duckdb::TableCatalogEntry>>
  ReferencingTables(duckdb::ClientContext& context) final;

  const auto& Storage() const noexcept { return _storage; }
  void RequireCurrentSchema() const;
  const std::vector<search::KeySpec>& KeySpecs() const noexcept {
    return _keys;
  }
  irs::DirectoryReader RowsWithOwn(duckdb::ClientContext& context) const;
  void AdoptAsWritten(duckdb::ClientContext& context) const;

 private:
  duckdb::unique_ptr<duckdb::CreateTableInfo> Definition() const;
  duckdb::unique_ptr<SearchTableEntry> Successor(
    duckdb::ClientContext& context, duckdb::BoundCreateTableInfo& bound) const;
  duckdb::unique_ptr<SearchTableEntry> Altered(
    duckdb::ClientContext& context, const duckdb::AlterInfo& info,
    duckdb::unique_ptr<duckdb::CreateTableInfo> definition,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>* bound_defaults =
      nullptr) const;
  duckdb::unique_ptr<duckdb::CatalogEntry> ChangeColumns(
    duckdb::ClientContext& context, const duckdb::AlterInfo& info,
    duckdb::unique_ptr<SearchTableEntry> altered,
    absl::FunctionRef<void(SearchTableEntry&)> change_rows);
  duckdb::unique_ptr<duckdb::CatalogEntry> AddVerifiedConstraint(
    duckdb::ClientContext& context, duckdb::AlterInfo& info,
    const duckdb::Constraint& constraint);

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterColumnComment(
    duckdb::ClientContext& context, duckdb::SetColumnCommentInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterRenameColumn(
    duckdb::ClientContext& context, duckdb::RenameColumnInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterColumnDefault(
    duckdb::ClientContext& context, duckdb::SetDefaultInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterSetNotNull(
    duckdb::ClientContext& context, duckdb::SetNotNullInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterDropNotNull(
    duckdb::ClientContext& context, duckdb::DropNotNullInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterAddConstraint(
    duckdb::ClientContext& context, duckdb::AddConstraintInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterDropConstraint(
    duckdb::ClientContext& context, duckdb::DropConstraintInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterRenameConstraint(
    duckdb::ClientContext& context, duckdb::RenameConstraintInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterAddColumn(
    duckdb::ClientContext& context, duckdb::AddColumnInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterDropColumn(
    duckdb::ClientContext& context, duckdb::RemoveColumnInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterColumnType(
    duckdb::ClientContext& context, duckdb::ChangeColumnTypeInfo& info);
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterOptions(
    duckdb::ClientContext& context, duckdb::AlterTableInfo& info);

  duckdb::unique_ptr<duckdb::Expression> BindConversion(
    duckdb::Binder& binder, duckdb::ClientContext& context,
    duckdb::ChangeColumnTypeInfo& info);
  duckdb::idx_t UnusedColumnOid() const;

  void BeginSchemaChange(duckdb::ClientContext& context);
  void ApplyKeys() const;
  duckdb::unique_ptr<duckdb::CatalogEntry> AddKeyConstraint(
    duckdb::ClientContext& context, duckdb::AddConstraintInfo& info);
  void VerifyKey(duckdb::ClientContext& context,
                 const search::KeySpec& spec) const;
  connector::SearchBackfillTarget BackfillTarget(
    duckdb::ClientContext& context, irs::field_id field,
    duckdb::unique_ptr<duckdb::Expression> value) const;
  void Backfill(duckdb::ClientContext& context, irs::field_id field,
                duckdb::unique_ptr<duckdb::Expression> value) const;
  void ReplaceStoreColumnsAtCommit(duckdb::ClientContext& context) const;
  void VerifyExistingRows(duckdb::ClientContext& context,
                          const duckdb::BoundConstraint& constraint) const;
  void AlterTriggers(duckdb::ClientContext& context,
                     duckdb::AlterTableInfo& info,
                     const duckdb::Identifier& name);

  duckdb::ColumnList _columns;
  std::shared_ptr<search::Store> _storage;
  std::shared_ptr<EntrySet> _triggers;
  SearchTableOptions _options;
  uint64_t _schema_epoch = 0;
  std::vector<search::KeySpec> _keys;
  std::vector<std::shared_ptr<const search::Referencing>> _referencing;
};

inline bool IsSearchTable(const duckdb::CatalogEntry& entry) {
  return dynamic_cast<const SearchTableEntry*>(&entry) != nullptr;
}

}  // namespace sdb::catalog
