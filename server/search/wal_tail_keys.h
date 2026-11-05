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

#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_key_unique.h"
#include "search/row_position_wal.h"

namespace sdb::search {

// What one search table's committed-but-unpublished WAL tail says about its
// keys. A committed transaction left its rows in one of two forms: as bare
// rows where it did not flush, which is where `keys` comes from, and as whole
// segments where it did, which `segments` names for a probe to open. The
// barrier matters because everything a TRUNCATE precedes is dead, in the tail
// and in the published reader alike.
struct WalTailKeys {
  std::vector<connector::KeyTerms> keys;
  std::vector<std::string> segments;
  RemovalLog removals;
  bool truncated = false;

  void Clear() noexcept {
    keys.clear();
    segments.clear();
    removals.Clear();
    truncated = false;
  }

  bool Empty() const noexcept {
    return keys.empty() && segments.empty() && removals.Empty() && !truncated;
  }
};

// Collects `WalTailKeys` for one table out of a WAL tail read. A batch's
// entries arrive before its transaction's fate is known, so they are staged
// and only folded in once the reader confirms the batch committed.
class WalTailKeyCollector final : public duckdb::TableLogStorage,
                                  public duckdb::TableLogStorageResolver {
 public:
  WalTailKeyCollector(duckdb::idx_t table_oid,
                      std::span<const connector::primary_key::KeySlot> slots)
    : _table_oid{table_oid}, _slots{slots.begin(), slots.end()} {}

  const WalTailKeys& Keys() const noexcept { return _committed; }
  WalTailKeys TakeKeys() { return std::move(_committed); }

  duckdb::optional_ptr<duckdb::TableLogStorage> Resolve(
    duckdb::idx_t table_oid) final {
    return table_oid == _table_oid ? this : nullptr;
  }
  void EndBatch(bool committed) final;

  void ReplayInsert(duckdb::ClientContext& context,
                    duckdb::DataChunk& chunk) final;
  void ReplayTruncate() final;
  void ReplayAdoptSegments(duckdb::vector<std::string> segments) final;
  void ReplayDeleteRowPositions(
    std::shared_ptr<const duckdb::data_t> entry,
    const duckdb::vector<duckdb::WALFrozenRowPositionGroup>& groups) final;
  void ReplayRowPositionMaps(
    std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
    const duckdb::vector<duckdb::WALFrozenRowPositionMap>& maps) final;

 private:
  duckdb::idx_t _table_oid;
  std::vector<connector::primary_key::KeySlot> _slots;
  WalTailKeys _staged;
  WalTailKeys _committed;
};

}  // namespace sdb::search
