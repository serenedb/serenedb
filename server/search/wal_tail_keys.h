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
#include <duckdb/storage/table/table_log_storage.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_key_unique.h"
#include "search/search_table_changes.h"

namespace sdb::search {

// What one search table's committed-but-unpublished WAL tail says about its
// keys: the same three things a probe needs, plus the barrier a TRUNCATE
// leaves. `truncate_tick` is the offset of the last TRUNCATE in the tail;
// everything the tail said before it, and everything the published reader
// holds, is dead.
struct WalTailKeys {
  std::vector<connector::KeyTerms> keys;
  std::vector<std::string> segments;
  std::vector<RowRange> removed;
  bool truncated = false;

  void Clear() {
    keys.clear();
    segments.clear();
    removed.clear();
    truncated = false;
  }

  bool Empty() const noexcept {
    return keys.empty() && segments.empty() && removed.empty() && !truncated;
  }
};

// Collects `WalTailKeys` for one table out of a WAL tail read. A batch's
// entries arrive before its transaction's fate is known, so they are staged and
// only folded in when the reader confirms the batch committed.
class WalTailKeyCollector final : public duckdb::TableLogStorage,
                                  public duckdb::TableLogStorageResolver {
 public:
  WalTailKeyCollector(duckdb::idx_t table_oid,
                      std::span<const connector::primary_key::KeySlot> slots)
    : _table_oid{table_oid}, _slots{slots.begin(), slots.end()} {}

  const WalTailKeys& Keys() const noexcept { return _committed; }

  duckdb::optional_ptr<duckdb::TableLogStorage> Resolve(
    duckdb::idx_t table_oid) final {
    return table_oid == _table_oid ? this : nullptr;
  }

  void EndBatch(bool committed) final;

  void ReplayInsert(duckdb::ClientContext& context, duckdb::DataChunk& chunk,
                    duckdb::optional_idx row_start) final;
  void ReplayDelete(duckdb::ClientContext& context,
                    duckdb::DataChunk& chunk) final;
  void ReplayDeleteRanges(duckdb::ClientContext& context,
                          const duckdb::vector<duckdb::row_t>& starts,
                          const duckdb::vector<duckdb::idx_t>& counts) final;
  void ReplayTruncate(duckdb::ClientContext& context) final;
  void ReplayAdoptSegments(duckdb::ClientContext& context,
                           duckdb::vector<std::string> segments) final;

 private:
  duckdb::idx_t _table_oid;
  std::vector<connector::primary_key::KeySlot> _slots;
  WalTailKeys _staged;
  WalTailKeys _committed;
};

}  // namespace sdb::search
