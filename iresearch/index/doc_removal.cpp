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
#include <utility>

#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs {

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
       .docs = std::move(docs).Finish(DocumentMaskBuilder::kCanonical),
       .maps = segment.DocMaps()});
  }
  _groups = {};
  _last = 0;
  return removal;
}

void ResolveRemoval(const DocRemoval& removal, const RemovalTargets& targets) {
  const auto removed_of = [&](uint64_t id) -> DocumentMaskBuilder* {
    const auto it = targets.find(id);
    return it != targets.end() ? it->second.removed : nullptr;
  };
  const auto follow = [&](this const auto& self, const DocMapsSlot& slot,
                          const DocumentMask& docs) -> void {
    for (const auto& map : slot.maps) {
      if (auto* removed = removed_of(map.target)) {
        MapDocs(map, docs, *removed);
      } else if (DocumentMaskBuilder moved; MapDocs(map, docs, moved)) {
        SDB_ASSERT(map.next != nullptr);
        self(*map.next, std::move(moved).Take());
      }
    }
  };
  for (const auto& group : removal.groups) {
    if (auto* removed = removed_of(group.segment)) {
      removed->Merge(group.docs);
    } else if (group.maps != nullptr) {
      follow(*group.maps, group.docs);
    }
  }
}

bool MapDocs(const DocMap& map, const DocumentMask& docs,
             DocumentMaskBuilder& out) {
  const auto* mask = map.mask.get();
  const auto before = out.Count();
  doc_id_t counted = doc_limits::min();
  uint64_t masked = 0;
  docs.ForEachRun(mask, [&](doc_id_t first, doc_id_t last) {
    if (mask) {
      masked += mask->CountInRange(counted, first);
      counted = last;
    }
    const uint64_t rank = first - doc_limits::min() - masked;
    const auto from = std::max<uint64_t>(rank, map.rank_begin);
    const auto to = std::min<uint64_t>(rank + (last - first), map.rank_end);
    if (from < to) {
      out.AddRange(static_cast<doc_id_t>(map.base + (from - map.rank_begin)),
                   static_cast<doc_id_t>(map.base + (to - map.rank_begin)));
    }
    return to < map.rank_end;
  });
  return out.Count() != before;
}

void MapRows(const DocMap& map, std::span<const uint64_t> rows,
             std::vector<MappedRow>& out) {
  const auto* mask = map.mask.get();
  doc_id_t counted = doc_limits::min();
  uint64_t masked = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto doc = static_cast<doc_id_t>(rows[i] + doc_limits::min());
    if (mask) {
      if (mask->Contains(doc)) {
        continue;
      }
      masked += mask->CountInRange(counted, doc);
      counted = doc;
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
