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
#include <duckdb/planner/bound_constraint.hpp>

#include "catalog/table.h"

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

}  // namespace sdb::connector
