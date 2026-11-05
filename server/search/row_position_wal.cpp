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

#include "search/row_position_wal.h"

#include <duckdb/common/fault_points.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <utility>
#include <vector>

namespace sdb::search {

void WriteRemoval(duckdb::WALEntryBuffer& entries,
                  const irs::DocRemoval& removal) {
  SDB_ASSERT(!removal.groups.empty());
  auto groups = removal.groups.size();
  SDB_IF_FAILURE("search_delete_wal_no_segment") { groups = 0; }
  entries.WriteDeleteRowPositions(groups, [&](duckdb::idx_t i) {
    const auto& group = removal.groups[i];
    return duckdb::WALRowPositionGroup{.segment = group.segment,
                                       .docs = &group.docs.Bitmap()};
  });
}

void WriteDocMaps(duckdb::WALEntryBuffer& entries,
                  const std::vector<irs::DocMap>& maps) {
  SDB_ASSERT(!maps.empty());
  const auto source = maps.front().source;
  entries.WriteRowPositionMaps(source, maps.size(), [&](duckdb::idx_t i) {
    const auto& map = maps[i];
    SDB_ASSERT(map.source == source);
    return duckdb::WALRowPositionMap{
      .target = map.target,
      .base = map.base,
      .rank_begin = map.rank_begin,
      .rank_end = map.rank_end,
      .mask = map.mask ? &map.mask->Bitmap() : nullptr};
  });
}

std::shared_ptr<const irs::DocRemoval> ReplayRemoval(
  irs::DocRemoval removal, const irs::DirectoryReader& recovered,
  const irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>>&
    doc_maps) {
  irs::containers::FlatHashMap<uint64_t, const irs::SegmentReaderImpl*> readers;
  readers.reserve(recovered.size());
  for (const auto& sub : recovered) {
    const auto& reader =
      *irs::utils::downCast<irs::SegmentReader>(sub).GetImpl();
    readers.emplace(reader.Id(), &reader);
  }
  auto& groups = removal.groups;
  auto replayed = std::make_shared<irs::DocRemoval>();
  replayed->groups.reserve(groups.size());
  for (size_t i = 0; i != groups.size(); ++i) {
    const auto segment = groups[i].segment;
    auto docs = std::move(groups[i].docs);
    if (const auto it = readers.find(segment); it != readers.end()) {
      replayed->groups.push_back({.segment = segment,
                                  .docs = std::move(docs),
                                  .maps = it->second->DocMaps()});
      continue;
    }
    const auto it = doc_maps.find(segment);
    if (it == doc_maps.end()) {
      continue;
    }
    for (const auto& map : it->second) {
      if (irs::DocumentMaskBuilder moved; irs::MapDocs(map, docs, moved)) {
        groups.push_back(
          {.segment = map.target, .docs = std::move(moved).Take()});
      }
    }
  }
  return replayed;
}

void RemovalLog::AddPositions(
  std::shared_ptr<const duckdb::data_t> entry,
  std::span<const duckdb::WALFrozenRowPositionGroup> groups) {
  if (groups.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("Malformed doc removal: it names no segment"));
  }
  for (const auto& group : groups) {
    _positions.groups.push_back(
      {.segment = group.segment,
       .docs =
         irs::DocumentMask{{entry, group.docs.data()}, group.docs.size()}});
  }
}

void RemovalLog::AddMaps(
  std::shared_ptr<const duckdb::data_t> entry, uint64_t source,
  std::span<const duckdb::WALFrozenRowPositionMap> maps) {
  auto& doc_maps = _doc_maps[source];
  doc_maps.clear();
  doc_maps.reserve(maps.size());
  for (const auto& map : maps) {
    if (source >= map.target) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATA_CORRUPTED),
        ERR_MSG("Malformed doc map: it does not lead to a newer segment"));
    }
    doc_maps.push_back({.source = source,
                        .target = map.target,
                        .mask = map.mask.empty()
                                  ? nullptr
                                  : std::make_shared<const irs::DocumentMask>(
                                      std::shared_ptr<const irs::byte_type>{
                                        entry, map.mask.data()},
                                      map.mask.size()),
                        .rank_begin = map.rank_begin,
                        .rank_end = map.rank_end,
                        .base = map.base});
  }
}

void RemovalLog::Clear() noexcept {
  _keys.clear();
  _positions = {};
  _doc_maps.clear();
}

std::vector<int64_t> RemovalLog::TakeKeys() {
  auto keys = std::move(_keys);
  _keys.clear();
  return keys;
}

std::shared_ptr<const irs::DocRemoval> RemovalLog::Resolve(
  const irs::DirectoryReader& reader) && {
  if (_positions.groups.empty()) {
    return nullptr;
  }
  return ReplayRemoval(std::move(_positions), reader, _doc_maps);
}

}  // namespace sdb::search
