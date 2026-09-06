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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/table_column.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog1/persistence/search_table.h"

namespace duckdb {

struct CreateInfo;
struct CreateTableInfo;
struct BoundCreateTableInfo;

}  // namespace duckdb

namespace sdb::search {

class SearchTable;

}  // namespace sdb::search

namespace sdb::catalog {

enum class TableEngine : uint8_t {
  Transactional = 0,
  Search = 1,
};

inline constexpr std::string_view kStorageOption = "storage";

using persistence::SearchTableOptions;

class SearchTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SearchTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::BoundCreateTableInfo& info,
                   duckdb::CatalogTransaction transaction,
                   std::shared_ptr<search::SearchTable> inherited_storage = {});

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) override;

  void AdoptStorage(std::shared_ptr<search::SearchTable> storage) {
    _storage = std::move(storage);
  }

  const auto& Storage() const noexcept { return _storage; }
  const std::shared_ptr<search::SearchTable>& EnsureStorage() const;
  const auto& Options() const noexcept { return _options; }

 private:
  mutable std::shared_ptr<search::SearchTable> _storage;
  SearchTableOptions _options;
};

}  // namespace sdb::catalog
