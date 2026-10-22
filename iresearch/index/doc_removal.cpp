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
#include <absl/base/internal/endian.h>

#include <utility>

#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace {

[[noreturn]] void Malformed(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("Malformed doc map: ", what));
}

void Put32(std::string& out, uint32_t value) {
  char buf[sizeof(uint32_t)];
  absl::little_endian::Store32(buf, value);
  out.append(buf, sizeof(buf));
}

class MapReader {
 public:
  explicit MapReader(std::string_view bytes) noexcept : _bytes{bytes} {}

  uint32_t Get32() {
    return absl::little_endian::Load32(Take(sizeof(uint32_t)).data());
  }

  std::string_view Take(size_t size) {
    if (_bytes.size() < size) [[unlikely]] {
      Malformed("it ends early");
    }
    const auto taken = _bytes.substr(0, size);
    _bytes.remove_prefix(size);
    return taken;
  }

  bool Done() const noexcept { return _bytes.empty(); }

 private:
  std::string_view _bytes;
};

}  // namespace

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

std::string WriteDocMaps(const DocMapList& maps) {
  std::string out;
  if (maps.empty()) {
    return out;
  }
  Put32(out, static_cast<uint32_t>(maps.size()));
  for (const auto& map : maps) {
    Put32(out, map->source);
    Put32(out, map->target);
    Put32(out, static_cast<uint32_t>(map->steps.size()));
    for (const auto& step : map->steps) {
      Put32(out, step.rank_begin);
      Put32(out, step.rank_end);
      Put32(out, step.base);
      const auto mask_size =
        step.mask ? static_cast<uint32_t>(step.mask->FrozenSize()) : 0;
      Put32(out, mask_size);
      if (mask_size != 0) {
        const auto offset = out.size();
        out.resize(offset + mask_size);
        step.mask->WriteFrozen(out.data() + offset);
      }
    }
  }
  return out;
}

DocMapList ReadDocMaps(std::string_view bytes) {
  DocMapList maps;
  if (bytes.empty()) {
    return maps;
  }
  MapReader in{bytes};
  const auto count = in.Get32();
  for (uint32_t i = 0; i < count; ++i) {
    DocMap map;
    map.source = in.Get32();
    map.target = in.Get32();
    if (map.source == DocRemoval::kNoSegment ||
        map.target == DocRemoval::kNoSegment) {
      Malformed("a segment number is out of range");
    }
    const auto steps = in.Get32();
    if (steps == 0) {
      Malformed("a map has no steps");
    }
    for (uint32_t j = 0; j < steps; ++j) {
      auto& step = map.steps.emplace_back();
      step.rank_begin = in.Get32();
      step.rank_end = in.Get32();
      step.base = in.Get32();
      if (step.rank_begin > step.rank_end || !doc_limits::valid(step.base) ||
          step.rank_end - step.rank_begin > doc_limits::eof() - step.base) {
        Malformed("a step is out of range");
      }
      if (const auto mask_size = in.Get32(); mask_size != 0) {
        BytesViewInput mask{ViewCast<byte_type>(in.Take(mask_size))};
        step.mask = std::make_shared<const DocumentMask>(
          DocumentMask::ReadFrozen(mask, mask_size));
      }
    }
    maps.push_back(std::make_shared<const DocMap>(std::move(map)));
  }
  if (!in.Done()) {
    Malformed("it has trailing bytes");
  }
  return maps;
}

void DocMaps::Absorb(std::span<const DocMap> direct) {
  DocMapList composed;
  for (const auto& map : direct) {
    for (const auto& [number, source] : _sources) {
      for (const auto& earlier : source.maps) {
        if (earlier->target == map.source) {
          auto steps = earlier->steps;
          steps.insert(steps.end(), map.steps.begin(), map.steps.end());
          composed.push_back(
            std::make_shared<const DocMap>(DocMap{.source = earlier->source,
                                                  .target = map.target,
                                                  .steps = std::move(steps)}));
        }
      }
    }
  }
  for (const auto& map : direct) {
    _sources[map.source] = {};
  }
  for (const auto& map : direct) {
    _sources[map.source].maps.push_back(std::make_shared<const DocMap>(map));
  }
  for (auto& map : composed) {
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
  for (const auto number : sources) {
    if (const auto it = _sources.find(number); it != _sources.end()) {
      out.insert(out.end(), it->second.maps.begin(), it->second.maps.end());
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
      const auto map_docs = [&](const DocMap& map) {
        if (!live.contains(map.target)) {
          return;
        }
        for (const auto doc : docs) {
          if (const auto mapped = AbsorbedDoc(map.steps, doc)) {
            rows.emplace_back(map.target, *mapped);
          }
        }
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
