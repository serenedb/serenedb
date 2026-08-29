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
#include <span>
#include <string>
#include <vector>

#include "basics/primary_key.hpp"
#include "connector/duckdb_sink_writer_base.h"
namespace sdb::connector {

// The chunk positions a CREATE INDEX backfill has to read: the indexed columns
// plus the key columns the row identity is built from, sorted and deduplicated.
// Both are already positions in the relation's column list, so nothing here
// needs the relation itself.
std::vector<size_t> BuildCreateIndexProjection(
  std::span<const duckdb::LogicalIndex> pk_column_positions,
  std::span<const duckdb::idx_t> index_column_positions);

// One indexed base column of a feed chunk: its position in the chunk and the
// (column id, type) the sink needs to tokenize it.
struct FeedColumn {
  duckdb::idx_t slot;
  ColumnDescriptor desc;
};

// One already-evaluated indexed expression: the field it feeds and its values
// for this batch. Callers evaluate with whatever executor they own -- duckdb's
// index expressions for DML/recovery, the pipeline for CREATE INDEX.
struct ExpressionValue {
  irs::field_id field_id;
  duckdb::Vector* values;
};

// The single feed path shared by every caller that tokenizes a chunk into an
// inverted index (CREATE INDEX backfill, live insert, recovery replay): open
// the batch, hand each indexed column and expression value to the sink, close.
// `writer` already holds the target iresearch transaction. `commit_on_flush`
// (nullable) commits a mid-batch flush and reports it back, which the online
// build's delete-log tracking follows.
void FeedChunk(DuckDBSinkIndexWriter& writer, duckdb::idx_t count,
               const PkChunk& pk, duckdb::DataChunk& chunk,
               std::span<const FeedColumn> columns,
               std::span<const ExpressionValue> expression_values = {},
               irs::CommitOnFlush* commit_on_flush = nullptr);

// The single removal path shared by every caller that deletes rows from an
// inverted index (live commit, recovery replay, the CREATE INDEX delete-log
// drain). Postings are keyed by AppendSigned(rowid), so a removal needs the row
// ids and nothing else. `row_at(i)` yields the i-th one, so a caller can pass a
// span, duckdb's own rowid vector or a sorted log without copying it first;
// `key` is the caller's scratch, reused across rows.
template<typename RowAt>
void FeedDeletes(DuckDBSinkIndexWriter& writer, std::string& key, size_t count,
                 RowAt&& row_at) {
  writer.Init(count, {});
  for (size_t i = 0; i < count; ++i) {
    key.clear();
    primary_key::AppendSigned(key, row_at(i));
    writer.DeleteRow(key);
  }
  writer.Finish();
}

}  // namespace sdb::connector
