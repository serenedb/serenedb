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

#include "iresearch/index/doc_removal.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <optional>
#include <utility>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs {
namespace {

class Runs {
 public:
  explicit Runs(const DocumentMask* mask) {
    if (mask) {
      _cursor.emplace(*mask);
      Fill();
    }
  }

  bool Empty() const noexcept { return _at == _size; }
  const MaskRun& Front() const noexcept { return _buffer[_at]; }

  void Pop() {
    if (++_at == _size) {
      Fill();
    }
  }

  uint64_t SkipBefore(doc_id_t doc) {
    uint64_t skipped = 0;
    while (!Empty() && Front().max < doc) {
      skipped += uint64_t{Front().max} - Front().min + 1;
      Pop();
    }
    return skipped;
  }

 private:
  void Fill() {
    _size = _cursor->ReadRuns(_buffer);
    _at = 0;
  }

  std::optional<DocumentMask::Cursor> _cursor;
  std::array<MaskRun, 64> _buffer;
  size_t _at = 0;
  size_t _size = 0;
};

}  // namespace

DocumentMaskBuilder& DocRemovalBuilder::Group(uint32_t index) {
  if (_last < _groups.size() && _groups[_last].first == index) {
    return _groups[_last].second;
  }
  auto it = absl::c_lower_bound(
    _groups, index,
    [](const auto& group, uint32_t key) { return group.first < key; });
  if (it == _groups.end() || it->first != index) {
    it = _groups.emplace(it, index, DocumentMaskBuilder{});
  }
  _last = static_cast<size_t>(it - _groups.begin());
  return it->second;
}

void DocRemovalBuilder::Add(uint32_t index, doc_id_t doc) {
  Group(index).Add(doc);
}

void DocRemovalBuilder::AddRange(uint32_t index, doc_id_t first,
                                 doc_id_t last) {
  Group(index).AddRange(first, last);
}

std::shared_ptr<const DocRemoval> DocRemovalBuilder::Finish(
  const DirectoryReader& reader) && {
  auto removal = std::make_shared<DocRemoval>();
  removal->groups.reserve(_groups.size());
  for (auto& [index, docs] : _groups) {
    if (docs.Empty()) {
      continue;
    }
    SDB_ASSERT(index < reader.size());
    const auto& segment =
      *utils::downCast<SegmentReader>(reader[index]).GetImpl();
    removal->groups.push_back(
      {.segment = segment.Id(),
       .docs = std::move(docs).Finish(DocumentMaskBuilder::kCanonical)});
  }
  _groups = {};
  _last = 0;
  return removal;
}

bool MapDocs(const DocMap& map, const DocumentMask& docs,
             DocumentMaskBuilder& out) {
  const auto* mask = map.mask.get();
  std::optional<DocumentMask> kept;
  if (mask && docs.Intersects(*mask)) {
    DocumentMaskBuilder live{docs};
    live.Subtract(*mask);
    kept.emplace(std::move(live).Take());
  }
  Runs holes{mask};
  uint64_t masked = 0;
  bool mapped = false;
  DocumentMask::Cursor cursor{kept ? *kept : docs};
  std::array<MaskRun, 64> runs;
  while (const auto count = cursor.ReadRuns(runs)) {
    for (const auto& run : std::span{runs}.first(count)) {
      masked += holes.SkipBefore(run.min);
      const uint64_t rank = run.min - doc_limits::min() - masked;
      const uint64_t end = rank + (uint64_t{run.max} - run.min + 1);
      const auto from = std::max<uint64_t>(rank, map.rank_begin);
      const auto to = std::min<uint64_t>(end, map.rank_end);
      if (from < to) {
        out.AddRange(static_cast<doc_id_t>(map.base + (from - map.rank_begin)),
                     static_cast<doc_id_t>(map.base + (to - map.rank_begin)));
        mapped = true;
      }
      if (end >= map.rank_end) {
        return mapped;
      }
    }
  }
  return mapped;
}

void MapRows(const DocMap& map, std::span<const uint64_t> rows,
             std::vector<MappedRow>& out) {
  Runs holes{map.mask.get()};
  uint64_t masked = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    SDB_ASSERT(i == 0 || rows[i - 1] <= rows[i]);
    const auto doc = static_cast<doc_id_t>(rows[i] + doc_limits::min());
    masked += holes.SkipBefore(doc);
    if (!holes.Empty() && holes.Front().min <= doc) {
      continue;
    }
    const uint64_t rank = rows[i] - masked;
    if (rank < map.rank_begin) {
      continue;
    }
    if (rank >= map.rank_end) {
      return;
    }
    out.push_back(
      {.index = i,
       .row = map.base - doc_limits::min() + (rank - map.rank_begin)});
  }
}

}  // namespace irs
