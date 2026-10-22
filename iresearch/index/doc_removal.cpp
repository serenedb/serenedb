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

#include <utility>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

DocRemoval DocRemoval::Build(std::span<const Row> rows) {
  containers::FlatHashMap<uint32_t, uint32_t> cursors;
  uint32_t* cursor = nullptr;
  uint32_t cursor_segment = kNoSegment;
  const auto cursor_of = [&](uint32_t segment) -> uint32_t& {
    SDB_ASSERT(segment != kNoSegment);
    if (cursor == nullptr || segment != cursor_segment) {
      cursor = &cursors[segment];
      cursor_segment = segment;
    }
    return *cursor;
  };
  for (const auto& row : rows) {
    ++cursor_of(row.segment);
  }

  DocRemoval removal;
  removal.segments.reserve(cursors.size());
  for (const auto& [segment, count] : cursors) {
    removal.segments.emplace_back(segment);
  }
  absl::c_sort(removal.segments);
  removal.offsets.reserve(removal.segments.size() + 1);
  uint32_t offset = 0;
  for (const auto segment : removal.segments) {
    removal.offsets.emplace_back(offset);
    offset += std::exchange(cursors[segment], offset);
  }
  removal.offsets.emplace_back(offset);

  removal.docs.resize(rows.size());
  for (const auto& row : rows) {
    removal.docs[cursor_of(row.segment)++] = row.doc;
  }
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

std::optional<doc_id_t> AbsorbedDoc(std::span<const AbsorbedStep> steps,
                                    doc_id_t doc) noexcept {
  for (const auto& step : steps) {
    if (!doc_limits::valid(doc) || (step.mask && step.mask->Contains(doc))) {
      return std::nullopt;
    }
    const doc_id_t rank =
      doc - doc_limits::min() - (step.mask ? step.mask->CountBelow(doc) : 0);
    if (rank < step.rank_begin || rank >= step.rank_end) {
      return std::nullopt;
    }
    doc = step.base + (rank - step.rank_begin);
  }
  return doc;
}

void DocRemovalResolver::Prepare(std::span<const DocRemoval* const> removals,
                                 std::span<const RemovalTarget> targets) {
  containers::FlatHashSet<uint32_t> live;
  containers::FlatHashMap<
    uint32_t, std::vector<std::pair<uint32_t, const AbsorbedSegment*>>>
    absorbed;
  for (const auto& target : targets) {
    live.insert(target.number);
    for (const auto& entry : target.meta->absorbed) {
      absorbed[entry.number].emplace_back(target.number, &entry);
    }
  }

  std::vector<DocRemoval::Row> rows;
  for (const auto* removal : removals) {
    if (absl::c_all_of(removal->segments, [&](uint32_t segment) {
          return live.contains(segment);
        })) {
      continue;
    }
    rows.clear();
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      const auto segment = removal->segments[group];
      const auto docs = removal->Docs(group);
      if (live.contains(segment)) {
        for (const auto doc : docs) {
          rows.emplace_back(segment, doc);
        }
        continue;
      }
      const auto it = absorbed.find(segment);
      if (it == absorbed.end()) {
        continue;
      }
      _used.insert(segment);
      for (const auto& [target, entry] : it->second) {
        for (const auto doc : docs) {
          if (const auto mapped = AbsorbedDoc(entry->steps, doc)) {
            rows.emplace_back(target, *mapped);
          }
        }
      }
    }
    _resolved.emplace(removal, DocRemoval::Build(rows));
  }
}

std::span<const doc_id_t> DocRemovalResolver::PositionalDocs(
  const DocRemoval& removal, const SubReader& segment) const {
  const auto number = SegmentNumber(segment.Meta().name);
  if (number == DocRemoval::kNoSegment) {
    return {};
  }
  const auto it = _resolved.find(&removal);
  const auto& resolved = it == _resolved.end() ? removal : it->second;
  const auto group = resolved.Find(number);
  if (group == resolved.segments.size()) {
    return {};
  }
  return resolved.Docs(group);
}

}  // namespace irs
