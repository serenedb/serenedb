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

#include "connector/duckdb_physical_cached_insert.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <utility>

#include "basics/assert.h"
#include "query/duckdb_engine.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBCachedInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
  // Mark sdb_cache$'s underlying transaction as read-write so the per-DB
  // commit check accepts the writes. We can't use MetaTransaction's
  // ModifyDatabase here because the bind-time RegisterDBModify on the user
  // search table already claimed the txn's single-DB slot for the user DB
  // (meta_transaction.cpp:256-260). The single-DB rule is purely a
  // MetaTransaction-level invariant; per-AttachedDatabase commit only
  // checks that the DB's own transaction is read-write, so this bypass is
  // safe for the cache write (which is the only write that hits
  // sdb_cache$ in this statement).
  auto& cache_catalog = duckdb::Catalog::GetCatalog(
    context, std::string{query::kSearchCacheDatabase});
  duckdb::Transaction::Get(context, cache_catalog.GetAttached()).SetReadWrite();
  return duckdb::PhysicalInsert::GetGlobalSinkState(context);
}

SereneDBCachedInsert::SereneDBCachedInsert(
  duckdb::PhysicalPlan& plan,
  duckdb::DuckTableEntry& cache_table,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> bound_constraints,
  duckdb::idx_t user_col_count,
  std::shared_ptr<catalog::Sequence> gen_pk_sequence,
  duckdb::idx_t estimated_cardinality)
  // PhysicalInsert's `types` argument is the operator's *output* types
  // (the rowcount column emitted as a source), NOT the insert column
  // types. Insert column types are derived from `cache_table.GetTypes()`
  // inside the base ctor (insert_types(table.GetTypes())). Passing the
  // cache schema here caused the rowcount to be cast to TINYINT — fine
  // for ≤127 rows, overflow for larger inserts.
  : duckdb::PhysicalInsert(
      plan, duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::BIGINT},
      cache_table, std::move(bound_constraints),
      /*set_expressions=*/{}, /*set_columns=*/{}, /*set_types=*/{},
      estimated_cardinality, /*return_chunk=*/false, /*parallel=*/true,
      duckdb::OnConflictAction::THROW,
      /*on_conflict_condition=*/nullptr, /*do_update_condition=*/nullptr,
      /*on_conflict_filter=*/{}, /*columns_to_fetch=*/{},
      /*update_is_del_and_insert=*/false),
    _user_col_count(user_col_count),
    _synthetic_col_count(gen_pk_sequence ? 2 : 1),
    _gen_pk_sequence(std::move(gen_pk_sequence)) {}

duckdb::SinkResultType SereneDBCachedInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  SDB_ASSERT(chunk.ColumnCount() == _user_col_count);

  // Build a cache-schema chunk: [sdb_op$, [sdb_pk$,] user cols...]. We
  // need a buffer for `sdb_pk$` (flat vector we write into per row); the
  // other columns are subsequently overwritten with constant references
  // (sdb_op$) or zero-copy aliases (user cols).
  duckdb::DataChunk cache_chunk;
  cache_chunk.Initialize(context.client, insert_types);

  duckdb::idx_t out_col = 0;

  // sdb_op$ = 0: constant vector across the whole chunk.
  cache_chunk.data[out_col++].Reference(duckdb::Value::TINYINT(0),
                                        duckdb::count_t(chunk.size()));

  // sdb_pk$: bulk-reserve a contiguous range from the auto-PK sequence,
  // fill a flat int64 vector. One sequence call per chunk, not per row.
  if (_gen_pk_sequence) {
    const auto num_rows = chunk.size();
    const auto base = _gen_pk_sequence->ReserveWriteUnsafe(num_rows);
    int64_t* out =
      duckdb::FlatVector::GetDataMutable<int64_t>(cache_chunk.data[out_col]);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      out[i] = static_cast<int64_t>(base + i);
    }
    ++out_col;
  }

  // User columns: zero-copy alias into the input chunk.
  for (duckdb::idx_t i = 0; i < _user_col_count; ++i) {
    cache_chunk.data[out_col + i].Reference(chunk.data[i]);
  }
  cache_chunk.SetCardinality(chunk.size());

  return duckdb::PhysicalInsert::Sink(context, cache_chunk, input);
}

}  // namespace sdb::connector
