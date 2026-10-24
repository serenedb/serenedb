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

#include <array>
#include <utility>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

DocumentMaskBuilder& DocRemovalBuilder::Group(uint32_t segment) {
  SDB_ASSERT(segment != DocRemoval::kNoSegment);
  if (_last < _groups.size() && _groups[_last].first == segment) {
    return _groups[_last].second;
  }
  auto it = absl::c_lower_bound(
    _groups, segment,
    [](const auto& group, uint32_t number) { return group.first < number; });
  if (it == _groups.end() || it->first != segment) {
    it = _groups.emplace(it, segment, DocumentMaskBuilder{});
  }
  _last = static_cast<size_t>(it - _groups.begin());
  return it->second;
}

void DocRemovalBuilder::Add(uint32_t segment, doc_id_t doc) {
  Group(segment).Add(doc);
}

void DocRemovalBuilder::AddRange(uint32_t segment, doc_id_t first,
                                 doc_id_t last) {
  Group(segment).AddRange(first, last);
}

void DocRemovalBuilder::Add(uint32_t segment, const DocumentMask& docs) {
  Group(segment).Merge(docs);
}

DocRemoval DocRemovalBuilder::Finish() && {
  DocRemoval removal;
  removal.segments.reserve(_groups.size());
  removal.docs.reserve(_groups.size());
  for (auto& [segment, docs] : _groups) {
    if (docs.Empty()) {
      continue;
    }
    removal.segments.emplace_back(segment);
    removal.docs.emplace_back(std::make_shared<const DocumentMask>(
      std::move(docs).Finish(DocumentMaskBuilder::kCanonical)));
  }
  _groups = {};
  _last = 0;
  return removal;
}

uint32_t SegmentNumber(std::string_view name) noexcept {
  uint64_t id = 0;
  if (!ParseSegmentId(name, id) || id >= DocRemoval::kNoSegment) {
    return DocRemoval::kNoSegment;
  }
  return static_cast<uint32_t>(id);
}

size_t DocRemoval::Find(uint32_t segment) const noexcept {
  const auto it = absl::c_lower_bound(segments, segment);
  if (it == segments.end() || *it != segment) {
    return segments.size();
  }
  return static_cast<size_t>(it - segments.begin());
}

bool MapDocs(const DocMap& map, const DocumentMask& docs,
             DocumentMaskBuilder& out) {
  static constexpr size_t kBatch = 1024;
  std::array<doc_id_t, kBatch> batch;
  std::array<uint64_t, kBatch> masked{};
  size_t size = 0;
  bool added = false;
  const auto flush = [&] {
    if (map.mask != nullptr) {
      map.mask->CountUpTo({batch.data(), size}, {masked.data(), size});
    }
    for (size_t i = 0; i < size; ++i) {
      if (map.mask != nullptr && map.mask->Contains(batch[i])) {
        continue;
      }
      const auto rank =
        static_cast<doc_id_t>(batch[i] - doc_limits::min() - masked[i]);
      if (map.rank_begin <= rank && rank < map.rank_end) {
        added |= out.Add(map.base + (rank - map.rank_begin));
      }
    }
    size = 0;
  };
  docs.ForEach([&](doc_id_t doc) {
    batch[size++] = doc;
    if (size == kBatch) {
      flush();
    }
  });
  flush();
  return added;
}

void DocMaps::Absorb(DocMapList maps) {
  for (const auto& map : maps) {
    _sources[map->source] = {};
  }
  for (auto& map : maps) {
    _sources[map->source].maps.push_back(std::move(map));
  }
}

void DocMaps::Committed(uint32_t source, uint64_t seq) noexcept {
  if (const auto it = _sources.find(source);
      it != _sources.end() && it->second.absorbed_at == kUncommitted) {
    it->second.absorbed_at = seq;
  }
}

void DocMaps::CopyTo(std::span<const uint32_t> sources, DocMapList& out) const {
  containers::FlatHashSet<uint32_t> seen;
  std::vector<uint32_t> pending{sources.begin(), sources.end()};
  while (!pending.empty()) {
    const auto number = pending.back();
    pending.pop_back();
    if (!seen.insert(number).second) {
      continue;
    }
    for (const auto& map : Of(number)) {
      out.push_back(map);
      pending.push_back(map->target);
    }
  }
}

std::span<const std::shared_ptr<const DocMap>> DocMaps::Of(
  uint32_t source) const noexcept {
  const auto it = _sources.find(source);
  if (it == _sources.end()) {
    return {};
  }
  return it->second.maps;
}

void DocMaps::Retire(uint64_t oldest_reader,
                     const DocRemovalResolver& removals) {
  for (auto it = _sources.begin(); it != _sources.end();) {
    auto& [number, source] = *it;
    if (oldest_reader < source.absorbed_at || removals.Used(number)) {
      source.retiring = false;
      ++it;
    } else if (!source.retiring) {
      source.retiring = true;
      ++it;
    } else {
      _sources.erase(it++);
    }
  }
}

void DocRemovalResolver::Prepare(std::span<const DocRemoval* const> removals,
                                 std::span<const uint32_t> targets,
                                 const DocMaps& maps) {
  const containers::FlatHashSet<uint32_t> live{targets.begin(), targets.end()};
  for (const auto* removal : removals) {
    DocRemovalBuilder mapped;
    containers::FlatHashSet<uint32_t> reached;
    const auto follow = [&](this const auto& self, uint32_t segment,
                            const DocumentMask& docs) -> void {
      const auto map_docs = [&](const DocMap& map) {
        DocumentMaskBuilder moved;
        if (!MapDocs(map, docs, moved)) {
          return;
        }
        const auto mask = std::move(moved).Finish();
        if (!live.contains(map.target)) {
          self(map.target, mask);
          return;
        }
        reached.insert(map.target);
        mapped.Add(map.target, mask);
      };
      if (const auto absorbed = maps.Of(segment); !absorbed.empty()) {
        _used.insert(segment);
        for (const auto& map : absorbed) {
          map_docs(*map);
        }
      }
      if (removal->maps != nullptr) {
        for (const auto& map : *removal->maps) {
          if (map->source == segment) {
            map_docs(*map);
          }
        }
      }
    };
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      if (!live.contains(removal->segments[group])) {
        follow(removal->segments[group], removal->Docs(group));
      }
    }
    if (mapped.Empty()) {
      continue;
    }
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      if (reached.contains(removal->segments[group])) {
        mapped.Add(removal->segments[group], removal->Docs(group));
      }
    }
    _mapped.emplace(removal, std::move(mapped).Finish());
  }
}

const DocumentMask* DocRemovalResolver::PositionalDocs(
  const DocRemoval& removal, const SubReader& segment) const {
  const auto number = SegmentNumber(segment.Meta().name);
  if (number == DocRemoval::kNoSegment) {
    return nullptr;
  }
  const auto docs_of = [&](const DocRemoval& groups) -> const DocumentMask* {
    const auto group = groups.Find(number);
    return group == groups.segments.size() ? nullptr : &groups.Docs(group);
  };
  if (const auto it = _mapped.find(&removal); it != _mapped.end()) {
    if (const auto* docs = docs_of(it->second)) {
      return docs;
    }
  }
  return docs_of(removal);
}

}  // namespace irs
