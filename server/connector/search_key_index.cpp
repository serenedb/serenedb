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

#include "connector/search_key_index.h"

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/storage/index.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::connector {
namespace {

constexpr duckdb::idx_t kDummyTableIndex = 0;
constexpr std::string_view kIndexName = "sdb_search_pk";

}  // namespace

SearchKeyIndex::SearchKeyIndex(duckdb::AttachedDatabase& db,
                               std::span<const primary_key::KeySlot> slots)
  : _db{db},
    _io_manager{db.GetStorageManager().GetTableIOManager(nullptr)} {
  SDB_ASSERT(!slots.empty());
  _column_ids.reserve(slots.size());
  _unbound_expressions.reserve(slots.size());
  for (duckdb::idx_t i = 0; i < slots.size(); ++i) {
    _column_ids.emplace_back(slots[i].input_col_idx);
    _unbound_expressions.emplace_back(
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        slots[i].type, duckdb::ColumnBinding{duckdb::TableIndex{kDummyTableIndex},
                                             duckdb::ProjectionIndex{i}}));
  }
}

void SearchKeyIndex::Insert(duckdb::DataChunk& chunk, uint64_t pk_base) {
  const auto count = chunk.size();
  SDB_ASSERT(count != 0);
  if (!_art) {
    _art = duckdb::make_uniq<duckdb::ART>(
      duckdb::Identifier{std::string{kIndexName}},
      duckdb::IndexConstraintType::UNIQUE, _column_ids, *_io_manager,
      _unbound_expressions, _db);
  }

  duckdb::row_t row_data[STANDARD_VECTOR_SIZE];
  duckdb::Vector row_ids{duckdb::LogicalType::ROW_TYPE,
                         duckdb::data_ptr_cast(row_data), STANDARD_VECTOR_SIZE};
  duckdb::VectorOperations::GenerateSequence(
    row_ids, count, static_cast<int64_t>(pk_base), 1);

  auto& index = static_cast<duckdb::BoundIndex&>(*_art);
  const auto error = index.Append(chunk, row_ids);

  if (error.HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                    ERR_MSG(error.RawMessage()));
  }
}

void SearchKeyIndex::Clear() { _art.reset(); }

}  // namespace sdb::connector
