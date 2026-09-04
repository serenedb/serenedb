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
#include <duckdb/common/constants.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/common/table_column.hpp>
#include <memory>
#include <string>
#include <string_view>

namespace duckdb {

struct CreateInfo;
struct BoundCreateTableInfo;

}  // namespace duckdb

namespace sdb::search {

class SearchTable;

}  // namespace sdb::search

namespace sdb::catalog {

// Which storage a table's rows live in. CREATE TABLE ... WITH (storage='...')
// selects it; the connector parses the WITH clause and the value reaches the
// entry through the tags below.
enum class TableEngine : uint8_t {
  Transactional = 0,
  Search = 1,
};

inline constexpr std::string_view kStorageOption = "storage";

// A search table's maintenance intervals. Far smaller than an inverted index's
// option surface, and pg_class.reloptions does not echo them.
struct SearchTableOptions {
  uint32_t refresh_interval_ms{1000};
  uint32_t compaction_interval_ms{1000};
  uint32_t cleanup_interval_step{1};

  bool operator==(const SearchTableOptions& rhs) const = default;
};

using TableOptions =
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::ParsedExpression>>;

SearchTableOptions ReadSearchTableOptions(const TableOptions& options);

void WriteSearchTableOptions(const SearchTableOptions& search_options,
                             TableOptions& options);

class SearchTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SearchTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::BoundCreateTableInfo& info,
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

  // TableCatalogEntry drops CreateTableInfo::options, so the entry carries
  // them itself and puts them back -- what IndexCatalogEntry does for its own.
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;

  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) override;

  const std::shared_ptr<search::SearchTable>& Storage() const noexcept {
    return _storage;
  }

  void AdoptStorage(std::shared_ptr<search::SearchTable> storage) {
    _storage = std::move(storage);
  }

  const std::shared_ptr<search::SearchTable>& EnsureStorage() const;

  const SearchTableOptions& Options() const noexcept { return _options; }

 private:
  mutable std::shared_ptr<search::SearchTable> _storage;
  SearchTableOptions _options;
};

}  // namespace sdb::catalog
