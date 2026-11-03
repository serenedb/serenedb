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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <span>
#include <string>
#include <vector>

#include "server/utils/primary_key.h"

namespace duckdb {

class RowIdDeduplicator;

}  // namespace duckdb
namespace sdb::connector::primary_key {

std::vector<duckdb::LogicalIndex> KeyColumns(
  const duckdb::TableCatalogEntry& entry);

// One component of a row key: the chunk slot the value arrives in and the type
// it is encoded from.
struct PKColumn {
  duckdb::idx_t input_col_idx;
  duckdb::LogicalType type;
};

void PreparePKFormats(duckdb::DataChunk& chunk,
                      std::span<const PKColumn> columns,
                      std::vector<duckdb::UnifiedVectorFormat>& formats);

duckdb::idx_t KeepFirstRowIds(duckdb::RowIdDeduplicator& seen,
                              duckdb::DataChunk& chunk, const PKColumn& rowid);

void Create(std::span<const duckdb::UnifiedVectorFormat> formats,
            std::span<const PKColumn> columns, duckdb::idx_t row,
            std::string& key);

}  // namespace sdb::connector::primary_key
