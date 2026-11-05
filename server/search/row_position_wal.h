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
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <vector>

namespace duckdb {

class WALEntryBuffer;

}  // namespace duckdb
namespace sdb::search {

void WriteRemoval(duckdb::WALEntryBuffer& entries,
                  const irs::DocRemoval& removal);
void WriteDocMaps(duckdb::WALEntryBuffer& entries,
                  const std::vector<irs::DocMap>& maps);

std::shared_ptr<const irs::DocRemoval> ReplayRemoval(
  irs::DocRemoval removal, const irs::DirectoryReader& recovered,
  const irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>>&
    doc_maps);

// What one run of WAL entries removes: bare keys, doc positions, and the maps
// that carry a position onto the segment a consolidation has replaced it with.
// The entries are read in one pass and settled in another, because a position
// only means something against a reader, and which reader that is belongs to
// the consumer: recovery settles on the one it rebuilt, a certifier on the one
// it snapshotted.
class RemovalLog {
 public:
  void AddKey(int64_t key) { _keys.push_back(key); }
  void AddPositions(std::shared_ptr<const duckdb::data_t> entry,
                    std::span<const duckdb::WALFrozenRowPositionGroup> groups);
  void AddMaps(std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
               std::span<const duckdb::WALFrozenRowPositionMap> maps);
  void Clear() noexcept;

  bool HasKeys() const noexcept { return !_keys.empty(); }
  bool HasPositions() const noexcept { return !_positions.groups.empty(); }

  std::vector<int64_t> TakeKeys();
  // Null when nothing was recorded, so a caller can hand the result straight
  // to a remover without asking twice.
  std::shared_ptr<const irs::DocRemoval> Resolve(
    const irs::DirectoryReader& reader) &&;

 private:
  std::vector<int64_t> _keys;
  irs::DocRemoval _positions;
  irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>> _doc_maps;
};

}  // namespace sdb::search
