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

#include <absl/base/internal/endian.h>

#include <cmath>
#include <concepts>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "basics/assert.h"
#include "basics/primary_key.hpp"
#include "basics/string_utils.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/key_encoding.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog::duckdb_primary_key {

struct PKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
  size_t struct_child = duckdb::DConstants::INVALID_INDEX;
};

// The glob view-index pk term is the raw unsigned file half plus the
// sortable signed row half; the file half alone is the whole-file prefix.
inline std::string PkFilePrefix(uint64_t file_id) {
  std::string key;
  connector::primary_key::AppendUnsigned(key, file_id);
  return key;
}

// The declared primary key of `info`, in key order, as positions in its own
// column list. Empty when it declares none -- there the row identity is the
// generated PK instead.
inline std::vector<PKColumn> BuildPKColumns(
  const duckdb::CreateTableInfo& info) {
  const auto* key = catalog::TablePrimaryKey(info.constraints);
  if (key == nullptr) {
    return {};
  }
  std::vector<PKColumn> result;
  result.reserve(key->GetColumnNames().size());
  for (const auto& name : key->GetColumnNames()) {
    const auto* column = catalog::ColumnByName(info, name.GetIdentifierName());
    if (column == nullptr) {
      continue;
    }
    result.push_back(PKColumn{.input_col_idx = column->Logical().index,
                              .type = column->Type()});
  }
  return result;
}

inline void PreparePKFormats(
  duckdb::DataChunk& chunk, std::span<const PKColumn> pk_columns,
  std::vector<duckdb::UnifiedVectorFormat>& pk_formats) {
  const auto num_rows = chunk.size();
  pk_formats.resize(pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    auto& vec = chunk.data[pk_columns[c].input_col_idx];
    const auto child = pk_columns[c].struct_child;
    if (child != duckdb::DConstants::INVALID_INDEX) {
      duckdb::StructVector::GetEntries(vec)[child].ToUnifiedFormat(
        num_rows, pk_formats[c]);
    } else {
      vec.ToUnifiedFormat(num_rows, pk_formats[c]);
    }
  }
}

inline void Create(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string& key) {
  SDB_ASSERT(pk_formats.size() == pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    connector::key_encoding::AppendScalarValue(key, pk_formats[c], row_idx,
                                               pk_columns[c].type);
  }
}

// Sortable signed encoding -- caller must have reserved the id.
inline void AppendGenerated(std::string& key, uint64_t generated_id) {
  connector::primary_key::AppendSigned(key,
                                       std::bit_cast<int64_t>(generated_id));
}

}  // namespace sdb::catalog::duckdb_primary_key
