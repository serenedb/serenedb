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
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/planner/bound_constraint.hpp>

#include "basics/containers/flat_hash_set.h"
#include "catalog/table.h"
#include "connector/duckdb_primary_key.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

// Mirrors DataTable::VerifyAppendConstraints in DuckDB: for INSERT.
// `chunk` has all physical columns at their PhysicalIndex positions.
// Throws duckdb::Exception (ExceptionType::CONSTRAINT) on violation, with
// PG-compatible error message and DETAIL.
void VerifyAppendConstraints(
  duckdb::ClientContext& context, const catalog::Table& table,
  const duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints,
  duckdb::DataChunk& chunk);

// Mirrors DataTable::VerifyUpdateConstraints in DuckDB: for UPDATE.
// `chunk` slots 0..column_ids.size()-1 hold the updated column values in
// the order of `column_ids`. pk_chunk_positions gives, for each PK in
// table.PKColumns() order, the chunk slot carrying its value -- used only
// to enrich the PG "Failing row contains (...)" detail.
void VerifyUpdateConstraints(
  duckdb::ClientContext& context, const catalog::Table& table,
  const duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints,
  duckdb::DataChunk& chunk,
  const std::vector<duckdb::PhysicalIndex>& column_ids,
  const std::vector<duckdb::idx_t>& pk_chunk_positions);

// Build PG-compatible "Key (col1, col2)=(val1, val2) already exists." detail.
std::string BuildPKViolationDetail(
  const duckdb::DataChunk& chunk,
  std::span<const duckdb_primary_key::PKColumn> pk_columns,
  std::span<const std::string> pk_col_names, duckdb::idx_t row_idx);

// DuckDB-native version of WriteConflictResolver (data_sink.hpp).
// Checks row keys against existing DB data and handles conflicts per policy.
class DuckDBWriteConflictResolver {
 public:
  DuckDBWriteConflictResolver() = default;

  void Init(rocksdb::Transaction& txn, rocksdb::ColumnFamilyHandle& cf,
            duckdb::OnConflictAction on_conflict, std::string_view table_name);

  // Returns number of skipped rows.
  // CheckOldKeys=true (update_pk): skip rows where old_key == new_key.
  // CheckOldKeys=false (insert): old_keys ignored.
  template<bool CheckOldKeys>
  size_t HandleWriteConflicts(
    std::vector<std::string>& keys, const duckdb::DataChunk& chunk,
    std::span<const duckdb_primary_key::PKColumn> pk_columns,
    std::span<const std::string> pk_col_names,
    std::span<const std::string> old_keys = {});

 private:
  rocksdb::Transaction* _txn = nullptr;
  rocksdb::ColumnFamilyHandle* _cf = nullptr;
  containers::FlatHashSet<std::string> _batch_keys;
  duckdb::OnConflictAction _on_conflict = duckdb::OnConflictAction::THROW;
  std::string_view _table_name;
  rocksdb::ReadOptions _read_options;
  rocksdb::PinnableSlice _lookup_value;
};

}  // namespace sdb::connector
