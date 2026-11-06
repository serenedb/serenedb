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
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/table/table_log_storage.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_key_unique.h"
#include "search/row_position_wal.h"

namespace sdb::search {

// Where a tail read stopped, so the next one answers only for what was
// committed since. The generation names the WAL the offset belongs to: a
// checkpoint retires the live WAL under a generation of its own, so an offset
// alone stops meaning anything the moment one lands.
struct WalTailCursor {
  uint64_t generation = 0;
  uint64_t offset = 0;

  friend auto operator<=>(const WalTailCursor&, const WalTailCursor&) = default;
};

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

// One WAL file of a tail span, open and bounded.
struct WalTailFile {
  uint64_t generation = 0;
  duckdb::unique_ptr<duckdb::FileHandle> handle;
  uint64_t start = 0;
  uint64_t end = 0;
};

// The files holding everything committed between `from` and the end of `db`'s
// WAL. A checkpoint that lands between two reads retires the live WAL to a file
// of its own, so a span is one file per generation: the one `from` names, every
// generation after it, and the WAL in use now.
//
// Resolving and opening belong together and must happen under the commit lock
// or a shared checkpoint key, one of which the caller holds: a checkpoint is
// the only thing that moves these files, and it moves them under the commit
// lock, so either excludes it from moving one while the file is being found and
// opened. An open handle then keeps the file it was opened on through any later
// rename or unlink, which is what lets a pass that is not committing give its
// lock back before it reads.
std::vector<WalTailFile> OpenTailFiles(duckdb::AttachedDatabase& db,
                                       const WalTailCursor& from);

// Reads `files` into `collector` and answers where the next read resumes.
WalTailCursor ReadTailKeys(duckdb::AttachedDatabase& db,
                           duckdb::ClientContext& context,
                           const WalTailCursor& from,
                           std::span<WalTailFile> files,
                           WalTailKeyCollector& collector);

}  // namespace sdb::search
