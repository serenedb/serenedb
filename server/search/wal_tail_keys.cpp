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

#include <utility>

namespace sdb::search {

void WalTailKeyCollector::EndBatch(bool committed) {
  if (!committed) {
    _staged.Clear();
    return;
  }
  if (_staged.truncated) {
    // Everything the tail said before this batch is dead, and so is everything
    // the published reader holds; the barrier travels with the result so a
    // probe can drop that tier instead of consulting it.
    _committed.Clear();
    _committed.truncated = true;
  }
  auto& keys = _committed.keys;
  keys.insert(keys.end(), std::make_move_iterator(_staged.keys.begin()),
              std::make_move_iterator(_staged.keys.end()));
  auto& segments = _committed.segments;
  segments.insert(segments.end(),
                  std::make_move_iterator(_staged.segments.begin()),
                  std::make_move_iterator(_staged.segments.end()));
  _committed.removals.Merge(std::move(_staged.removals));
  _staged.Clear();
}

void WalTailKeyCollector::ReplayInsert(duckdb::ClientContext&,
                                       duckdb::DataChunk& chunk) {
  if (_slots.empty()) {
    return;
  }
  connector::EncodeChunkKeys(chunk, _slots, _staged.keys);
}

void WalTailKeyCollector::ReplayTruncate() {
  // Within a batch a TRUNCATE precedes everything else the record holds, so
  // what it kills is what earlier batches staged, not this one's rows.
  _staged.keys.clear();
  _staged.segments.clear();
  _staged.removals.Clear();
  _staged.truncated = true;
}

void WalTailKeyCollector::ReplayAdoptSegments(
  duckdb::vector<std::string> segments) {
  _staged.segments.insert(_staged.segments.end(),
                          std::make_move_iterator(segments.begin()),
                          std::make_move_iterator(segments.end()));
}

void WalTailKeyCollector::ReplayDeleteRowPositions(
  std::shared_ptr<const duckdb::data_t> entry,
  const duckdb::vector<duckdb::WALFrozenRowPositionGroup>& groups) {
  _staged.removals.AddPositions(std::move(entry), groups);
}

void WalTailKeyCollector::ReplayRowPositionMaps(
  std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
  const duckdb::vector<duckdb::WALFrozenRowPositionMap>& maps) {
  _staged.removals.AddMaps(std::move(entry), source, maps);
}

}  // namespace sdb::search
