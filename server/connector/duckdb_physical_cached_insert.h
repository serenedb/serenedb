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
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/execution/operator/persistent/physical_insert.hpp>
#include <memory>

#include "catalog/sequence.h"

namespace sdb::connector {

// Physical operator for INSERT INTO a kSearch table. Routes writes through
// DuckDB's PhysicalInsert against the per-shard cache table in
// `sdb_cache$`. Inherits Insert's parallel sink machinery (parallel=true);
// the only custom logic is the per-chunk projection that prepends the
// cache's synthetic identity columns:
//
//   [sdb_op$ = 0][sdb_pk$ = reserved-from-sequence?][...user columns...]
//
// `sdb_pk$` is added only for generated-PK tables (when
// `gen_pk_sequence` is non-null). User columns are zero-copy aliased into
// the cache chunk -- no data is materialised by the projection itself.
//
// SELECT still reads from iresearch (unchanged for now); sync drains the
// cache into iresearch in a follow-up step. A BatchInsert specialisation
// (PreserveInsertionOrder + use_batch_index path) is deferred until
// serened sources expose batch indices.
class SereneDBCachedInsert final : public duckdb::PhysicalInsert {
 public:
  // `cache_table` is the cache table in `sdb_cache$`; its insert column
  // types are derived from the entry automatically by PhysicalInsert.
  // `user_col_count` is the column count of the incoming chunks
  // (= number of user columns); the cache chunk has
  // 1 + (gen_pk_sequence ? 1 : 0) + user_col_count columns.
  // `gen_pk_sequence` is the auto-PK sequence for generated-PK tables;
  // null for explicit-PK tables.
  SereneDBCachedInsert(
    duckdb::PhysicalPlan& plan,
    duckdb::DuckTableEntry& cache_table,
    duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
      bound_constraints,
    duckdb::idx_t user_col_count,
    std::shared_ptr<catalog::Sequence> gen_pk_sequence,
    duckdb::idx_t estimated_cardinality);

  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;

  // Mark `sdb_cache$` as modified on the user's MetaTransaction so the
  // cache-side write isn't rejected as a "read-only transaction made
  // changes". The LogicalInsert was bound against the user search table,
  // so the bind-time RegisterDBModify only marked the user DB; the cache
  // DB needs an explicit ModifyDatabase here.
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;

 private:
  duckdb::idx_t _user_col_count;
  // 1 for explicit-PK (just sdb_op$), 2 for generated-PK (sdb_op$ +
  // sdb_pk$). Cache chunk's user columns start at this offset.
  duckdb::idx_t _synthetic_col_count;
  std::shared_ptr<catalog::Sequence> _gen_pk_sequence;
};

}  // namespace sdb::connector
