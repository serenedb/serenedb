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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <span>
#include <string>

#include "catalog/table_options.h"
#include "connector/index_source_view.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

// Point lookup for a view over an attached external-DB table (ClickHouse,
// postgres) keyed by the table's own primary-key value
// (PkSpec::ExternalDBKey). Matched rows are re-fetched with
//   SELECT <pk>, <projection> FROM <catalog>.<schema>.<table>
//   WHERE <pk> IN (<keys>)
// -- plain DuckDB SQL through the attached catalog, so the connector's own
// scan, projection and filter pushdown do the work.
//
// CREATE INDEX guarantees at most one indexed document per key value
// (on_conflict = 'throw' refused duplicates, 'nothing' collapsed them), so
// each key resolves to one row: the first source row returned for a key fills
// every output slot that requested it; further rows with the same key --
// duplicates written after a 'nothing' build, ClickHouse's sorting key is not
// unique -- are ignored; a key the source no longer has leaves its slots NULL.
class ExternalLookupIndexSource final : public ViewIndexSourceBase {
 public:
  ExternalLookupIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);

  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::I64;
  }

  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  // "SELECT <pk>, <cols> FROM <table> WHERE <pk> IN (" -- constant across
  // Materialize() calls; only the key list varies.
  std::string _sql_prefix;
  duckdb::idx_t _num_proj_cols = 0;
};

}  // namespace sdb::connector
