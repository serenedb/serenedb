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

#include "connector/primary_key.h"

#include <duckdb/parser/constraints/unique_constraint.hpp>

#include "connector/key_encoding.h"

namespace sdb::connector::primary_key {

std::vector<duckdb::LogicalIndex> KeyColumns(
  const duckdb::TableCatalogEntry& entry) {
  const auto key = entry.GetPrimaryKey();
  if (!key) {
    return {};
  }
  return key->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(
    entry.GetColumns());
}

std::vector<PKColumn> PKColumns(const duckdb::TableCatalogEntry& entry) {
  const auto& columns = entry.GetColumns();
  const auto keys = KeyColumns(entry);
  std::vector<PKColumn> out;
  out.reserve(keys.size());
  for (const auto key : keys) {
    out.push_back(
      {.input_col_idx = key.index, .type = columns.GetColumn(key).Type()});
  }
  return out;
}

void PreparePKFormats(duckdb::DataChunk& chunk,
                      std::span<const PKColumn> columns,
                      std::vector<duckdb::UnifiedVectorFormat>& formats) {
  formats.resize(columns.size());
  for (size_t i = 0; i != columns.size(); ++i) {
    chunk.data[columns[i].input_col_idx].ToUnifiedFormat(chunk.size(),
                                                         formats[i]);
  }
}

void Create(std::span<const duckdb::UnifiedVectorFormat> formats,
            std::span<const PKColumn> columns, duckdb::idx_t row,
            std::string& key) {
  for (size_t i = 0; i != columns.size(); ++i) {
    key_encoding::AppendScalarValue(key, formats[i], row, columns[i].type);
  }
}

void AppendGenerated(std::string& key, uint64_t value) {
  AppendSigned(key, static_cast<int64_t>(value));
}

std::string PkFilePrefix(uint64_t file_id) {
  std::string prefix;
  AppendUnsigned(prefix, file_id);
  return prefix;
}

}  // namespace sdb::connector::primary_key
