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

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>

#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace sdb::connector::test {

////////////////////////////////////////////////////////////////////////////////
/// Type trait: C++ type -> DuckDB LogicalType
///
/// Specialised for every scalar type that maps cleanly to a DuckDB column type.
/// Not defined for string_t — use MakeFlatVarchar / MakeNullableVarchar instead.
////////////////////////////////////////////////////////////////////////////////

template<class T>
duckdb::LogicalType DuckDBLogicalType() = delete;

template<>
inline duckdb::LogicalType DuckDBLogicalType<bool>() {
  return duckdb::LogicalType::BOOLEAN;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<int8_t>() {
  return duckdb::LogicalType::TINYINT;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<int16_t>() {
  return duckdb::LogicalType::SMALLINT;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<int32_t>() {
  return duckdb::LogicalType::INTEGER;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<int64_t>() {
  return duckdb::LogicalType::BIGINT;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<float>() {
  return duckdb::LogicalType::FLOAT;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<double>() {
  return duckdb::LogicalType::DOUBLE;
}
template<>
inline duckdb::LogicalType DuckDBLogicalType<duckdb::timestamp_t>() {
  return duckdb::LogicalType::TIMESTAMP;
}

////////////////////////////////////////////////////////////////////////////////
/// Flat scalar vectors
////////////////////////////////////////////////////////////////////////////////

// Creates a non-nullable flat vector from values.
// T must have a DuckDBLogicalType<T> specialisation.
template<class T>
duckdb::Vector MakeFlat(std::span<const T> values) {
  duckdb::Vector result(DuckDBLogicalType<T>(), values.size());
  auto *data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (duckdb::idx_t i = 0; i < values.size(); ++i) {
    data[i] = values[i];
  }
  return result;
}

template<class T>
duckdb::Vector MakeFlat(std::initializer_list<T> values) {
  return MakeFlat<T>(std::span<const T>(values.begin(), values.size()));
}

// Creates a nullable flat vector. nullopt entries set the validity bit to 0.
template<class T>
duckdb::Vector MakeNullableFlat(std::span<const std::optional<T>> values) {
  duckdb::Vector result(DuckDBLogicalType<T>(), values.size());
  auto *data = duckdb::FlatVector::GetDataMutable<T>(result);
  auto &validity = duckdb::FlatVector::Validity(result);
  for (duckdb::idx_t i = 0; i < values.size(); ++i) {
    if (values[i].has_value()) {
      data[i] = *values[i];
    } else {
      validity.SetInvalid(i);
    }
  }
  return result;
}

template<class T>
duckdb::Vector MakeNullableFlat(std::initializer_list<std::optional<T>> values) {
  return MakeNullableFlat<T>(
    std::span<const std::optional<T>>(values.begin(), values.size()));
}

////////////////////////////////////////////////////////////////////////////////
/// VARCHAR flat vectors (declared here, implemented in .cpp — need StringVector)
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeFlatVarchar(std::span<const std::string_view> values);

inline duckdb::Vector MakeFlatVarchar(
  std::initializer_list<std::string_view> values) {
  return MakeFlatVarchar(
    std::span<const std::string_view>(values.begin(), values.size()));
}

duckdb::Vector MakeNullableVarchar(
  std::span<const std::optional<std::string_view>> values);

inline duckdb::Vector MakeNullableVarchar(
  std::initializer_list<std::optional<std::string_view>> values) {
  return MakeNullableVarchar(std::span<const std::optional<std::string_view>>(
    values.begin(), values.size()));
}

////////////////////////////////////////////////////////////////////////////////
/// LIST vectors
///
/// For non-VARCHAR element types. rows[i] holds the elements for row i.
/// Nulls inside a list go through MakeNullableFlat on the child before passing
/// to the low-level MakeListFromChild helper; for whole-row nulls use
/// MakeNullableList.
////////////////////////////////////////////////////////////////////////////////

template<class T>
duckdb::Vector MakeList(std::span<const std::vector<T>> rows) {
  const duckdb::idx_t row_count = rows.size();
  duckdb::idx_t total_elements = 0;
  for (const auto &row : rows) {
    total_elements += row.size();
  }

  duckdb::Vector result(duckdb::LogicalType::LIST(DuckDBLogicalType<T>()),
                        row_count);
  duckdb::ListVector::Reserve(result, total_elements);

  auto &child = duckdb::ListVector::GetEntry(result);
  auto *child_data = duckdb::FlatVector::GetDataMutable<T>(child);
  auto *entries = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);

  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < row_count; ++i) {
    entries[i] = {offset, rows[i].size()};
    for (const auto &val : rows[i]) {
      child_data[offset++] = val;
    }
  }
  duckdb::ListVector::SetListSize(result, total_elements);
  return result;
}

template<class T>
duckdb::Vector MakeList(std::initializer_list<std::vector<T>> rows) {
  std::vector<std::vector<T>> r(rows);
  return MakeList<T>(std::span<const std::vector<T>>(r));
}

// Creates a nullable LIST vector. nullopt rows produce a null entry (zero-length
// list with the validity bit cleared).
template<class T>
duckdb::Vector MakeNullableList(
  std::span<const std::optional<std::vector<T>>> rows) {
  const duckdb::idx_t row_count = rows.size();
  duckdb::idx_t total_elements = 0;
  for (const auto &row : rows) {
    if (row.has_value()) {
      total_elements += row->size();
    }
  }

  duckdb::Vector result(duckdb::LogicalType::LIST(DuckDBLogicalType<T>()),
                        row_count);
  duckdb::ListVector::Reserve(result, total_elements);

  auto &child = duckdb::ListVector::GetEntry(result);
  auto *child_data = duckdb::FlatVector::GetDataMutable<T>(child);
  auto *entries = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto &validity = duckdb::FlatVector::Validity(result);

  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < row_count; ++i) {
    if (rows[i].has_value()) {
      entries[i] = {offset, rows[i]->size()};
      for (const auto &val : *rows[i]) {
        child_data[offset++] = val;
      }
    } else {
      entries[i] = {0, 0};
      validity.SetInvalid(i);
    }
  }
  duckdb::ListVector::SetListSize(result, total_elements);
  return result;
}

template<class T>
duckdb::Vector MakeNullableList(
  std::initializer_list<std::optional<std::vector<T>>> rows) {
  std::vector<std::optional<std::vector<T>>> r(rows);
  return MakeNullableList<T>(
    std::span<const std::optional<std::vector<T>>>(r));
}

////////////////////////////////////////////////////////////////////////////////
/// LIST<VARCHAR> vectors (declared here, implemented in .cpp)
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeVarcharList(
  std::span<const std::vector<std::string_view>> rows);

inline duckdb::Vector MakeVarcharList(
  std::initializer_list<std::vector<std::string_view>> rows) {
  std::vector<std::vector<std::string_view>> r(rows);
  return MakeVarcharList(std::span<const std::vector<std::string_view>>(r));
}

duckdb::Vector MakeNullableVarcharList(
  std::span<const std::optional<std::vector<std::string_view>>> rows);

////////////////////////////////////////////////////////////////////////////////
/// STRUCT vectors
///
/// children must be pre-built and all carry exactly `count` logical rows.
/// struct_type must be LogicalType::STRUCT({...}) whose field order matches
/// the order of children.
///
/// nulls: if non-empty, nulls[i]==false marks row i as a null struct.
/// Null child fields within a non-null struct are carried in the children
/// themselves via their own validity masks.
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeStruct(duckdb::LogicalType struct_type,
                           std::vector<duckdb::Vector> children,
                           duckdb::idx_t count,
                           std::span<const bool> nulls = {});

////////////////////////////////////////////////////////////////////////////////
/// MAP vectors
///
/// A DuckDB MAP is internally a LIST of STRUCT{key, value}. Callers supply
/// pre-built flat key and value vectors (all rows concatenated) together with
/// per-row list_entry_t offsets/lengths.
///
/// To produce null map elements (null key or value within a row's list), set
/// the validity on keys_flat / values_flat via MakeNullableFlat before passing
/// them in.  To produce a null map row, set nulls[i]=false.
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeMap(duckdb::LogicalType map_type,
                        duckdb::Vector keys_flat,
                        duckdb::Vector values_flat,
                        std::span<const duckdb::list_entry_t> entries,
                        std::span<const bool> nulls = {});

////////////////////////////////////////////////////////////////////////////////
/// DICTIONARY vectors
///
/// child is a flat vector acting as the dictionary value pool.
/// indices[i] selects which child element output row i maps to.
///
/// To produce null output rows, include null entries in child at specific
/// positions (via FlatVector::SetNull) and point those output rows to them.
/// This keeps nulls inside the dictionary rather than in the outer validity,
/// which is consistent with DuckDB's typical encoding for dictionary-with-nulls.
////////////////////////////////////////////////////////////////////////////////

duckdb::Vector MakeDict(duckdb::Vector child,
                         std::span<const duckdb::sel_t> indices);

////////////////////////////////////////////////////////////////////////////////
/// DataChunk assembly
///
/// Fills `out` with the given LogicalTypes and pre-built Vectors.
/// All vectors must carry exactly `count` logical rows.
/// DataChunk has a deleted copy constructor and no move constructor, so it
/// cannot be returned by value — callers must provide an existing chunk.
////////////////////////////////////////////////////////////////////////////////

void MakeChunk(duckdb::DataChunk& out, std::vector<duckdb::LogicalType> types,
               std::vector<duckdb::Vector> columns, duckdb::idx_t count);

////////////////////////////////////////////////////////////////////////////////
/// Argument-list helper
///
/// duckdb::Vector is move-only (copy constructor deleted), so brace-init of
/// std::vector<duckdb::Vector> via std::initializer_list doesn't compile —
/// initializer_list elements are const and must be copied.
/// Vecs(...) accepts any number of duckdb::Vector rvalues and moves them
/// into a std::vector, letting callers write natural call syntax:
///
///   MakeChunk(out, types, Vecs(MakeFlat<int32_t>(...), MakeFlatVarchar(...)), n);
///   MakeStruct(type, Vecs(fieldA, fieldB, fieldC), count);
////////////////////////////////////////////////////////////////////////////////

template<class... Vs>
std::vector<duckdb::Vector> Vecs(Vs&&... vs) {
  std::vector<duckdb::Vector> r;
  r.reserve(sizeof...(vs));
  (r.push_back(std::forward<Vs>(vs)), ...);
  return r;
}

}  // namespace sdb::connector::test
