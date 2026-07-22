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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/main/connection.hpp>
#include <memory>
#include <span>
#include <vector>

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
  // The batch ships as one array literal per key column inside a raw-query
  // passthrough (postgres_query / clickhouse_query): duckdb plans a tiny
  // constant statement instead of rebinding a 2048-node IN per Execute. The
  // remote engine echoes each row's 1-based position in the shipped arrays
  // as __sdb_ord (postgres: unnest WITH ORDINALITY join; ClickHouse:
  // transform() over the key array, or an ARRAY JOIN + arrayEnumerate join
  // for composite keys), so the probe places rows by ordinal -- no
  // client-side key routing at all.
  enum class Dialect : uint8_t { Postgres, ClickHouse };

  // The native remote type of each key column, fetched from the remote
  // catalog itself (postgres: format_type() over pg_attribute; ClickHouse:
  // system.columns.type) -- the templates cast the key arrays to exactly
  // these, so any type the connector can scan can be a lookup key with no
  // client-side type mapping. One metadata query per source.
  std::vector<std::string> ResolveKeyTypeNames(const CatalogTableRef& ref);
  void BuildQuery(const CatalogTableRef& ref,
                  const std::vector<std::string>& select_names);
  void BuildPostgresQuery(const CatalogTableRef& ref,
                          const std::vector<std::string>& select_names,
                          const std::vector<std::string>& key_type_names);
  void BuildClickHouseQuery(const CatalogTableRef& ref,
                            const std::vector<std::string>& select_names,
                            const std::vector<std::string>& key_type_names);

  duckdb::idx_t _num_proj_cols = 0;
  duckdb::idx_t _num_key_cols = 0;
  // ExternalPostgresCtid: stored split as STRUCT{page, tuple} for compression;
  // shipped to postgres as a tid[] array literal.
  bool _postgres_ctid = false;
  Dialect _dialect = Dialect::Postgres;

  std::unique_ptr<duckdb::Connection> _con;

  // The outer passthrough statement is PREPAREd once with the inner SQL as
  // $1 (`SELECT * FROM postgres_query('cat', $1, schema_query := '...')`);
  // per batch only the inner text is assembled and Execute'd -- duckdb never
  // re-parses the outer statement (it still re-binds per Execute, but the
  // describe cache keyed by the constant schema_query keeps that
  // round-trip-free). The inner query is _sql_parts[0] + _key_texts[0] +
  // _sql_parts[1] + ... -- one gap per key column, in key-column order.
  // _null_sql_parts is the ClickHouse single-key fallback for batches whose
  // keys contain NULL (transform() rejects NULL array elements; the join
  // form just never matches them), consuming the same key texts through the
  // same prepared statement.
  duckdb::unique_ptr<duckdb::PreparedStatement> _stmt;
  std::vector<std::string> _sql_parts;
  std::vector<std::string> _null_sql_parts;
  std::vector<std::string> _key_texts;

  // Slots already filled this batch: drops duplicate/garbage ordinals so a
  // misbehaving remote can never fill one output row twice.
  std::vector<uint8_t> _filled;
  // Accepted rows of the current result chunk: one gather Copy per column
  // appends them to _tf_target at the running row offset.
  duckdb::SelectionVector _take_sel;
};

}  // namespace sdb::connector
