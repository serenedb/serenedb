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

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/main/connection.hpp>
#include <memory>
#include <span>

#include "catalog/table_options.h"
#include "connector/index_source_view.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

class ExternalLookupIndexSource final : public ViewIndexSourceBase {
 public:
  ExternalLookupIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);
  ~ExternalLookupIndexSource() final = default;

  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::Struct;
  }

  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  // The batch ships as array literal(s) inside a raw-query passthrough
  // (postgres_query / clickhouse_query): duckdb plans a tiny constant
  // statement instead of rebinding a 2048-node IN per Execute. Postgres keys
  // go as one '{..}'::T[] per key column (composite: (k1,..) IN
  // (SELECT * FROM unnest(a1, ..))); ClickHouse as IN [..] (composite:
  // tuples).
  enum class LookupMode : uint8_t { PgArray, ChArray };

  duckdb::idx_t _num_proj_cols = 0;
  duckdb::idx_t _num_key_cols = 0;
  // ExternalPostgresCtid: stored split as STRUCT{page, tuple} for compression;
  // shipped to postgres as a tid[] array literal and probed back from
  // ctid::text.
  bool _postgres_ctid = false;
  LookupMode _mode = LookupMode::PgArray;

  std::unique_ptr<duckdb::Connection> _con;

  // The per-batch query is _sql_parts[0] + _key_texts[0] + _sql_parts[1] + ...
  // -- one key-text buffer per embedded array literal.
  std::vector<std::string> _sql_parts;
  std::vector<std::string> _key_texts;
  absl::flat_hash_map<duckdb::Value, duckdb::idx_t> _struct_slot;
};

}  // namespace sdb::connector
