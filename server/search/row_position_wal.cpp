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

#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <string>
#include <string_view>

namespace sdb::search {
namespace {

[[noreturn]] void Malformed(std::string_view what, std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("Malformed ", what, ": ", why));
}

uint32_t CheckedSegment(uint32_t number, std::string_view what) {
  if (number == irs::DocRemoval::kNoSegment) {
    Malformed(what, "a segment number is out of range");
  }
  return number;
}

std::string Frozen(const irs::DocumentMask& mask) {
  std::string frozen;
  frozen.resize_and_overwrite(mask.FrozenSize(), [&](char* buf, size_t size) {
    mask.WriteFrozen(buf);
    return size;
  });
  return frozen;
}

std::shared_ptr<const irs::DocumentMask> Thaw(std::string_view frozen) {
  irs::BytesViewInput in{irs::ViewCast<irs::byte_type>(frozen)};
  return std::make_shared<const irs::DocumentMask>(
    irs::DocumentMask::ReadFrozen(in, frozen.size()));
}

}  // namespace

duckdb::WALDeleteRowPositions ToWal(const irs::DocRemoval& removal) {
  SDB_ASSERT(!removal.all);
  duckdb::WALDeleteRowPositions entry;
  entry.segments.assign(removal.segments.begin(), removal.segments.end());
  entry.docs.reserve(removal.docs.size());
  for (const auto& docs : removal.docs) {
    entry.docs.emplace_back(Frozen(*docs));
  }
  return entry;
}

duckdb::WALRowPositionMaps ToWal(const irs::DocMapList& maps) {
  duckdb::WALRowPositionMaps entry;
  entry.maps.reserve(maps.size());
  for (const auto& map : maps) {
    auto& wal = entry.maps.emplace_back();
    wal.source = map->source;
    wal.target = map->target;
    wal.rank_begin = map->rank_begin;
    wal.rank_end = map->rank_end;
    wal.base = map->base;
    if (map->mask != nullptr) {
      wal.mask = Frozen(*map->mask);
    }
  }
  return entry;
}

irs::DocRemoval FromWal(const duckdb::WALDeleteRowPositions& entry) {
  static constexpr std::string_view kWhat = "doc removal";
  if (entry.segments.size() != entry.docs.size()) {
    Malformed(kWhat, "its segments and doc sets differ in count");
  }
  irs::DocRemoval removal;
  removal.segments.reserve(entry.segments.size());
  removal.docs.reserve(entry.docs.size());
  for (size_t group = 0; group < entry.segments.size(); ++group) {
    const auto segment = CheckedSegment(entry.segments[group], kWhat);
    if (!removal.segments.empty() && removal.segments.back() >= segment) {
      Malformed(kWhat, "its segments are out of order");
    }
    removal.segments.emplace_back(segment);
    removal.docs.emplace_back(Thaw(entry.docs[group]));
  }
  return removal;
}

irs::DocMapList FromWal(const duckdb::WALRowPositionMaps& entry) {
  static constexpr std::string_view kWhat = "doc map";
  irs::DocMapList maps;
  maps.reserve(entry.maps.size());
  for (const auto& wal : entry.maps) {
    if (wal.rank_begin > wal.rank_end || !irs::doc_limits::valid(wal.base) ||
        wal.rank_end - wal.rank_begin > irs::doc_limits::eof() - wal.base) {
      Malformed(kWhat, "a window is out of range");
    }
    maps.emplace_back(std::make_shared<const irs::DocMap>(irs::DocMap{
      .source = CheckedSegment(wal.source, kWhat),
      .target = CheckedSegment(wal.target, kWhat),
      .mask = wal.mask.empty() ? nullptr : Thaw(wal.mask),
      .rank_begin = wal.rank_begin,
      .rank_end = wal.rank_end,
      .base = wal.base,
    }));
  }
  return maps;
}

}  // namespace sdb::search
