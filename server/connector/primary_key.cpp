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
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

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

std::vector<KeySlot> KeySlots(const duckdb::TableCatalogEntry& entry) {
  const auto& columns = entry.GetColumns();
  std::vector<KeySlot> slots;
  for (const auto index : KeyColumns(entry)) {
    const auto& column = columns.GetColumn(index);
    slots.emplace_back(static_cast<duckdb::idx_t>(index.index),
                       column.Name().GetIdentifierName());
  }
  return slots;
}

void VerifyNotNull(duckdb::DataChunk& chunk, std::span<const KeySlot> slots,
                   duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  duckdb::UnifiedVectorFormat format;
  for (const auto& slot : slots) {
    chunk.data[slot.input_col_idx].ToUnifiedFormat(count, format);
    if (format.validity.CheckAllValid(count)) {
      continue;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NOT_NULL_VIOLATION),
                    ERR_MSG("null value in column \"", slot.name,
                            "\" violates not-null constraint"));
  }
}

std::string PkFilePrefix(uint64_t file_id) {
  std::string prefix;
  AppendUnsigned(prefix, file_id);
  return prefix;
}

}  // namespace sdb::connector::primary_key
