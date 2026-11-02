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
#include <vector>

#include "connector/duckdb_sink_writer_base.h"
namespace sdb::connector {

// One indexed base column of a feed chunk: its position in the chunk and the
// (column id, type) the sink needs to tokenize it.
struct FeedColumn {
  duckdb::idx_t slot;
  ColumnDescriptor desc;
};

struct ExpressionSlot {
  irs::field_id field_id;
  duckdb::idx_t slot;
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
               std::span<const ExpressionSlot> expressions = {},
               irs::CommitOnFlush* commit_on_flush = nullptr);

}  // namespace sdb::connector
