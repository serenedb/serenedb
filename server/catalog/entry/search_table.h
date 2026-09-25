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

#include <array>
#include <cstdint>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/table_column.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/persistence/search_table.h"
#include "query/config_variable_names.h"

namespace duckdb {

class ClientContext;
class SequenceCatalogEntry;
struct CreateInfo;
struct CreateTableInfo;
struct BoundCreateTableInfo;

}  // namespace duckdb
namespace irs {

class DirectoryReader;

}  // namespace irs
namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace sdb::catalog {

enum class TableEngine : uint8_t {
  Transactional = 0,
  Search = 1,
};

inline constexpr std::string_view kStorageOption = "storage";

inline constexpr auto kSearchTableMaintenanceSettings = std::to_array({
  kRefreshIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
});

inline constexpr auto kSearchTableSettings = std::to_array({
  kRefreshIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
  kRowGroupSizeSetting,
  kSegmentMemoryMaxSetting,
});

inline constexpr auto kSearchTableOptions = std::to_array({
  kRefreshIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
  kRowGroupSizeSetting,
  kSegmentMemoryMaxSetting,
  kOptimizeTopKSetting,
});

TableEngine ReadStorageEngine(
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& options);

inline constexpr std::string_view kGeneratedPkSequenceTag =
  "sdb_generated_pk_seq";

using persistence::SearchTableOptions;

class SearchTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SearchTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::BoundCreateTableInfo& info,
                   duckdb::CatalogTransaction transaction,
                   std::shared_ptr<search::SearchTable> inherited_storage = {});

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext&, duckdb::column_t) final {
    return nullptr;
  }

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext&) final {
    return {};
  }

  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) final;

  static duckdb::vector<duckdb::ColumnSegmentInfo> ColumnSegmentRows(
    const irs::DirectoryReader& reader, const duckdb::TableCatalogEntry& table,
    duckdb::column_t generated_pk);

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::ClientContext& context, duckdb::AlterInfo& info) final;

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;

  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const final;

  void OnDrop() final;

  void Rollback(duckdb::CatalogEntry& prev_entry) final;

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) final;

  duckdb::optional_ptr<duckdb::SequenceCatalogEntry> GeneratedPkSequence(
    duckdb::ClientContext& context) const;

  const duckdb::Identifier& PkSequenceName() const noexcept {
    return _pk_sequence;
  }

  const auto& Storage() const noexcept { return _storage; }

 private:
  std::shared_ptr<search::SearchTable> _storage;
  SearchTableOptions _options;
  duckdb::Identifier _pk_sequence;
};

}  // namespace sdb::catalog
