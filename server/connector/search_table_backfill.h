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
#include <duckdb/common/vector.hpp>
#include <memory>
#include <vector>

#include "catalog/column_id.h"
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::pg {

struct ProgressMetrics;

}  // namespace sdb::pg
namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace sdb::connector {

// What a search-table CREATE INDEX needs in order to rewrite the rows that were
// published before the index's config was.
struct SearchBackfillTarget {
  std::shared_ptr<search::SearchTable> shard;
  ObjectId table_id;
  // The table's stored columns in entry order: catalog id and chunk type.
  std::vector<catalog::ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> column_types;
  // Bytes of existing segments rewritten per swap (§6.3 of the design doc).
  // 0 = no limit: every stale segment goes into one group and one swap.
  uint64_t group_bytes = 0;
};

// Rewrites every segment written before the shard's current index config so
// it carries the new term fields, swapping groups of them in through ordinary
// index-meta commits. Runs after the config is published (CreateIndexImpl) and
// inside the CREATE INDEX statement; nothing it does is transactional beyond
// that, and an abandoned build leaves only inert extra fields behind.
//
// Sequence and reasoning: search_table_backfill.md §3.2.
void RunSearchTableBackfill(duckdb::ClientContext& context,
                            const SearchBackfillTarget& target,
                            pg::ProgressMetrics* progress);

}  // namespace sdb::connector
