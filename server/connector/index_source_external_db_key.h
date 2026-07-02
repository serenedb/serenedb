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
#include <vector>

#include "catalog/table_options.h"
#include "connector/index_source.h"
#include "connector/index_source_view.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

// Materialiser for a view over an attached external-DB table (e.g. ClickHouse)
// whose PK comes from the engine's own metadata (PkSpec::ExternalDBKey). The
// index keys postings by the PK column's value; at search time we re-fetch the
// matched rows engine-agnostically by running
//   SELECT <projection>, <pk> FROM <catalog>.<schema>.<table> WHERE <pk> IN
//   (...)
// through the catalog -- which routes to the connector's own scan + filter
// pushdown -- and map each returned row back to its output slot by PK. No
// dependency on the connector extension itself: this is pure DuckDB SQL.
class ExternalDBKeyIndexSource final : public ViewIndexSourceBase {
 public:
  ExternalDBKeyIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);

  PrimaryKeyBatch CreatePkBatch() const final {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeyI64>};
  }

  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  // Quoted "catalog"."schema"."table" and the quoted PK column name.
  std::string _qualified_table;
  std::string _pk_quoted;
  // Cached "SELECT <pk>, <cols> FROM <table> WHERE <pk> IN (" -- everything but
  // the per-call IN list, which is all that varies between Materialize() calls.
  std::string _sql_prefix;
  // Number of projected real columns (scratch columns, in _tf_target order).
  duckdb::idx_t _num_proj_cols = 0;
};

}  // namespace sdb::connector
