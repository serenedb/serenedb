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
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <optional>
#include <tuple>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "pg/catalog/engine/system_table.h"

namespace sdb::pg {

inline constexpr duckdb::CatalogType kStoreTypes[] = {
  duckdb::CatalogType::INDEX_ENTRY, duckdb::CatalogType::TABLE_ENTRY};

inline bool OwnsStore(const duckdb::IndexCatalogEntry& index) {
  return index.index_type == catalog::kInvertedIndexTypeName &&
         index.Cast<catalog::InvertedIndexEntry>().Storage();
}

inline bool OwnsStore(const duckdb::TableCatalogEntry& table) {
  return dynamic_cast<const catalog::SearchTableEntry*>(&table);
}

inline std::optional<duckdb::timestamp_tz_t> At(int64_t micros) {
  if (micros == 0) {
    return std::nullopt;
  }
  return duckdb::timestamp_tz_t{micros};
}

inline constexpr std::tuple kMergeInputColumns{
  Col<"inputs">([](const auto& r) -> const auto& { return r.stats.inputs; }),
  Col<"input_docs">([](const auto& r) { return r.stats.input_docs; }),
  Col<"input_live_docs">([](const auto& r) { return r.stats.input_live_docs; }),
  Col<"input_bytes">([](const auto& r) { return r.stats.input_bytes; })};

}  // namespace sdb::pg
