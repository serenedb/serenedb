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

#include "duckdb_vector_builder.hpp"

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/map_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>

#include "basics/assert.h"

namespace sdb::connector::test {

////////////////////////////////////////////////////////////////////////////////
/// VARCHAR flat vectors
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeFlatVarchar(std::span<const std::string_view> values) {
  duckdb::Vector result(duckdb::LogicalType::VARCHAR, values.size());
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  for (duckdb::idx_t i = 0; i < values.size(); ++i) {
    data[i] = duckdb::StringVector::AddString(result, values[i].data(),
                                              values[i].size());
  }
  return result;
}

duckdb::Vector MakeNullableVarchar(
  std::span<const std::optional<std::string_view>> values) {
  duckdb::Vector result(duckdb::LogicalType::VARCHAR, values.size());
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& validity = duckdb::FlatVector::Validity(result);
  for (duckdb::idx_t i = 0; i < values.size(); ++i) {
    if (values[i].has_value()) {
      data[i] = duckdb::StringVector::AddString(result, values[i]->data(),
                                                values[i]->size());
    } else {
      validity.SetInvalid(i);
    }
  }
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// LIST<VARCHAR> vectors
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeVarcharList(
  std::span<const std::vector<std::string_view>> rows) {
  const duckdb::idx_t row_count = rows.size();
  duckdb::idx_t total_elements = 0;
  for (const auto& row : rows) {
    total_elements += row.size();
  }

  duckdb::Vector result(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
                        row_count);
  duckdb::ListVector::Reserve(result, total_elements);

  auto& child = duckdb::ListVector::GetEntry(result);
  auto* child_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
  auto* entries = duckdb::ListVector::GetData(result);

  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < row_count; ++i) {
    entries[i] = {offset, rows[i].size()};
    for (const auto& str : rows[i]) {
      child_data[offset++] =
        duckdb::StringVector::AddString(child, str.data(), str.size());
    }
  }
  duckdb::ListVector::SetListSize(result, total_elements);
  return result;
}

duckdb::Vector MakeNullableVarcharList(
  std::span<const std::optional<std::vector<std::string_view>>> rows) {
  const duckdb::idx_t row_count = rows.size();
  duckdb::idx_t total_elements = 0;
  for (const auto& row : rows) {
    if (row.has_value()) {
      total_elements += row->size();
    }
  }

  duckdb::Vector result(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
                        row_count);
  duckdb::ListVector::Reserve(result, total_elements);

  auto& child = duckdb::ListVector::GetEntry(result);
  auto* child_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
  auto* entries = duckdb::ListVector::GetData(result);
  auto& validity = duckdb::FlatVector::Validity(result);

  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < row_count; ++i) {
    if (rows[i].has_value()) {
      entries[i] = {offset, rows[i]->size()};
      for (const auto& str : *rows[i]) {
        child_data[offset++] =
          duckdb::StringVector::AddString(child, str.data(), str.size());
      }
    } else {
      entries[i] = {0, 0};
      validity.SetInvalid(i);
    }
  }
  duckdb::ListVector::SetListSize(result, total_elements);
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// STRUCT vectors
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeStruct(duckdb::LogicalType struct_type,
                          std::vector<duckdb::Vector> children,
                          duckdb::idx_t count, std::span<const bool> nulls) {
  duckdb::Vector result(struct_type, count);
  auto& entries = duckdb::StructVector::GetEntries(result);
  SDB_ASSERT(entries.size() == children.size());
  for (duckdb::idx_t i = 0; i < children.size(); ++i) {
    // Reference keeps the child buffer alive via buffer_ptr ref-counting even
    // after children is destroyed at the end of this function.
    entries[i].Reference(children[i]);
  }
  if (!nulls.empty()) {
    SDB_ASSERT(nulls.size() == count);
    auto& validity = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (!nulls[i]) {
        validity.SetInvalid(i);
      }
    }
  }
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// MAP vectors
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeMap(duckdb::LogicalType map_type, duckdb::Vector keys_flat,
                       duckdb::Vector values_flat,
                       std::span<const duckdb::list_entry_t> entries,
                       std::span<const bool> nulls) {
  const duckdb::idx_t row_count = entries.size();

  duckdb::idx_t total_elements = 0;
  for (const auto& e : entries) {
    total_elements += e.length;
  }

  duckdb::Vector result(map_type, row_count);

  // Copy per-row list entries (offset + length).
  auto* list_entries = duckdb::ListVector::GetData(result);
  std::copy(entries.begin(), entries.end(), list_entries);
  duckdb::ListVector::SetListSize(result, total_elements);

  // A MAP's list child is a STRUCT{key, value}. Reference the provided flat
  // key and value vectors into that struct's field slots.
  auto& child_struct = duckdb::ListVector::GetEntry(result);
  auto& struct_entries = duckdb::StructVector::GetEntries(child_struct);
  SDB_ASSERT(struct_entries.size() == 2);
  // Reference is safe: buffer_ptr is ref-counted so the underlying buffer
  // outlives keys_flat / values_flat going out of scope.
  struct_entries[0].Reference(keys_flat);
  struct_entries[1].Reference(values_flat);

  if (!nulls.empty()) {
    SDB_ASSERT(nulls.size() == row_count);
    auto& validity = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t i = 0; i < row_count; ++i) {
      if (!nulls[i]) {
        validity.SetInvalid(i);
      }
    }
  }
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// DICTIONARY vectors
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeDict(duckdb::Vector child,
                        std::span<const duckdb::sel_t> indices) {
  const duckdb::idx_t count = indices.size();
  duckdb::SelectionVector sel(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    sel.set_index(i, indices[i]);
  }
  // Slice mutates child in-place: it becomes a DICTIONARY_VECTOR whose
  // internal buffer holds a DictionaryBuffer(sel_copy, child_data_ref).
  // The selection vector is copied into the buffer so sel can be stack-local.
  child.Slice(sel, count);
  return child;
}

////////////////////////////////////////////////////////////////////////////////
/// DataChunk assembly
////////////////////////////////////////////////////////////////////////////////

void MakeChunk(duckdb::DataChunk& out, std::vector<duckdb::LogicalType> types,
               std::vector<duckdb::Vector> columns, duckdb::idx_t count) {
  SDB_ASSERT(types.size() == columns.size());
  duckdb::vector<duckdb::LogicalType> duck_types(types.begin(), types.end());
  out.InitializeEmpty(duck_types);
  for (duckdb::idx_t i = 0; i < columns.size(); ++i) {
    // Reference: buffer_ptr ref-counting keeps column data alive after
    // columns[] is destroyed when this function returns.
    out.data[i].Reference(columns[i]);
  }
  out.SetCardinality(count);
}

}  // namespace sdb::connector::test
