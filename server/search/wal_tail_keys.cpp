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

#include "search/wal_tail_keys.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/utils/assert.hpp>
#include <utility>

namespace sdb::search {

void WalTailKeyCollector::EndBatch(bool committed) {
  if (!committed) {
    _staged.Clear();
    return;
  }
  if (_staged.truncated) {
    // Everything the tail said before this batch is dead, and so is everything
    // the published reader holds: a tail entry always outranks the reader.
    _committed.Clear();
    _committed.truncated = true;
  }
  _committed.keys.insert(_committed.keys.end(),
                         std::make_move_iterator(_staged.keys.begin()),
                         std::make_move_iterator(_staged.keys.end()));
  _committed.segments.insert(_committed.segments.end(),
                             std::make_move_iterator(_staged.segments.begin()),
                             std::make_move_iterator(_staged.segments.end()));
  _committed.removed.insert(_committed.removed.end(), _staged.removed.begin(),
                            _staged.removed.end());
  _staged.Clear();
}

void WalTailKeyCollector::ReplayInsert(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk,
                                       duckdb::optional_idx) {
  if (_slots.empty()) {
    return;
  }
  connector::EncodeChunkKeys(chunk, _slots, _staged.keys);
}

void WalTailKeyCollector::ReplayDelete(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk) {
  SDB_ASSERT(chunk.ColumnCount() == 1);
  for (duckdb::idx_t row = 0; row != chunk.size(); ++row) {
    const auto rowid = chunk.GetValue(0, row).GetValue<int64_t>();
    AppendRowRanges(_staged.removed, std::span{&rowid, 1});
  }
}

void WalTailKeyCollector::ReplayDeleteRanges(
  duckdb::ClientContext&, const duckdb::vector<duckdb::row_t>& starts,
  const duckdb::vector<duckdb::idx_t>& counts) {
  SDB_ASSERT(starts.size() == counts.size());
  for (size_t i = 0; i != starts.size(); ++i) {
    _staged.removed.emplace_back(starts[i],
                                 starts[i] + static_cast<int64_t>(counts[i]));
  }
}

void WalTailKeyCollector::ReplayTruncate(duckdb::ClientContext&) {
  // Within the batch too: rows this transaction wrote before its TRUNCATE are
  // as dead as the ones before it.
  _staged.Clear();
  _staged.truncated = true;
}

void WalTailKeyCollector::ReplayAdoptSegments(
  duckdb::ClientContext&, duckdb::vector<std::string> segments) {
  _staged.segments.insert(_staged.segments.end(),
                          std::make_move_iterator(segments.begin()),
                          std::make_move_iterator(segments.end()));
}

}  // namespace sdb::search
