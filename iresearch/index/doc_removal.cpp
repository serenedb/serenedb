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

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
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

constexpr duckdb::field_id_t kFieldList = 0;
constexpr duckdb::field_id_t kFieldSegment = 0;
constexpr duckdb::field_id_t kFieldDocs = 1;
constexpr duckdb::field_id_t kFieldSource = 0;
constexpr duckdb::field_id_t kFieldTarget = 1;
constexpr duckdb::field_id_t kFieldRankBegin = 2;
constexpr duckdb::field_id_t kFieldRankEnd = 3;
constexpr duckdb::field_id_t kFieldBase = 4;
constexpr duckdb::field_id_t kFieldMask = 5;

[[noreturn]] void Malformed(std::string_view what, std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("Malformed ", what, ": ", why));
}

void WriteMask(duckdb::BinarySerializer& out, duckdb::field_id_t field,
               const char* tag, const DocumentMask& mask) {
  const auto size = mask.FrozenSize();
  auto frozen = std::make_unique_for_overwrite<char[]>(size);
  mask.WriteFrozen(frozen.get());
  out.WriteProperty(
    field, tag, reinterpret_cast<duckdb::const_data_ptr_t>(frozen.get()), size);
}

std::shared_ptr<const DocumentMask> ReadMask(std::string_view frozen) {
  BytesViewInput in{ViewCast<byte_type>(frozen)};
  return std::make_shared<const DocumentMask>(
    DocumentMask::ReadFrozen(in, frozen.size()));
}

std::string ToBytes(const auto& write) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer out{stream};
  out.Begin();
  write(out);
  out.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

auto FromBytes(std::string_view bytes, std::string_view what,
               const auto& read) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer in{stream};
  try {
    in.Begin();
    auto result = read(in);
    in.End();
    if (stream.GetPosition() != bytes.size()) {
      Malformed(what, "it has trailing bytes");
    }
    return result;
  } catch (const duckdb::SerializationException& e) {
    Malformed(what, duckdb::ErrorData{e}.RawMessage());
  }
}

}  // namespace

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

void MapDocs(const DocMap& map, std::vector<doc_id_t>& docs) {
  std::vector<uint64_t> masked;
  if (map.mask != nullptr) {
    masked.resize(docs.size());
    map.mask->CountUpTo(docs, masked);
  }
  size_t kept = 0;
  for (size_t i = 0; i < docs.size(); ++i) {
    const auto doc = docs[i];
    if (!doc_limits::valid(doc) ||
        (map.mask != nullptr && map.mask->Contains(doc))) {
      continue;
    }
    const auto rank = static_cast<doc_id_t>(
      doc - doc_limits::min() - (map.mask != nullptr ? masked[i] : 0));
    if (rank < map.rank_begin || rank >= map.rank_end) {
      continue;
    }
    docs[kept++] = map.base + (rank - map.rank_begin);
  }
  docs.resize(kept);
}

std::string WriteDocMaps(const DocMapList& maps) {
  if (maps.empty()) {
    return {};
  }
  return ToBytes([&](duckdb::BinarySerializer& out) {
    out.WriteList(
      kFieldList, "maps", maps.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
        const auto& map = *maps[i];
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<uint32_t>(kFieldSource, "source", map.source);
          obj.WriteProperty<uint32_t>(kFieldTarget, "target", map.target);
          obj.WriteProperty<doc_id_t>(kFieldRankBegin, "rank_begin",
                                      map.rank_begin);
          obj.WriteProperty<doc_id_t>(kFieldRankEnd, "rank_end", map.rank_end);
          obj.WriteProperty<doc_id_t>(kFieldBase, "base", map.base);
          if (map.mask != nullptr) {
            WriteMask(obj, kFieldMask, "mask", *map.mask);
          }
        });
      });
  });
}

DocMapList ReadDocMaps(std::string_view bytes) {
  if (bytes.empty()) {
    return {};
  }
  static constexpr std::string_view kWhat = "doc map";
  return FromBytes(bytes, kWhat, [](duckdb::BinaryDeserializer& in) {
    DocMapList maps;
    in.ReadList(
      kFieldList, "maps",
      [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
        list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
          DocMap map;
          map.source = obj.ReadProperty<uint32_t>(kFieldSource, "source");
          map.target = obj.ReadProperty<uint32_t>(kFieldTarget, "target");
          if (map.source == DocRemoval::kNoSegment ||
              map.target == DocRemoval::kNoSegment) {
            Malformed(kWhat, "a segment number is out of range");
          }
          map.rank_begin =
            obj.ReadProperty<doc_id_t>(kFieldRankBegin, "rank_begin");
          map.rank_end = obj.ReadProperty<doc_id_t>(kFieldRankEnd, "rank_end");
          map.base = obj.ReadProperty<doc_id_t>(kFieldBase, "base");
          if (map.rank_begin > map.rank_end || !doc_limits::valid(map.base) ||
              map.rank_end - map.rank_begin > doc_limits::eof() - map.base) {
            Malformed(kWhat, "a window is out of range");
          }
          if (const auto mask =
                obj.ReadPropertyWithExplicitDefault<std::string>(kFieldMask,
                                                                 "mask", {});
              !mask.empty()) {
            map.mask = ReadMask(mask);
          }
          maps.push_back(std::make_shared<const DocMap>(std::move(map)));
        });
      });
    return maps;
  });
}

std::string WriteDocRemoval(const DocRemoval& removal) {
  SDB_ASSERT(!removal.all);
  return ToBytes([&](duckdb::BinarySerializer& out) {
    out.WriteList(
      kFieldList, "groups", removal.segments.size(),
      [&](duckdb::BinarySerializer::List& list, duckdb::idx_t group) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<uint32_t>(kFieldSegment, "segment",
                                      removal.segments[group]);
          WriteMask(obj, kFieldDocs, "docs", removal.Docs(group));
        });
      });
  });
}

DocRemoval ReadDocRemoval(std::string_view bytes) {
  static constexpr std::string_view kWhat = "doc removal";
  auto removal = FromBytes(bytes, kWhat, [](duckdb::BinaryDeserializer& in) {
    DocRemoval removal;
    in.ReadList(
      kFieldList, "groups",
      [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
        list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
          const auto segment =
            obj.ReadProperty<uint32_t>(kFieldSegment, "segment");
          if (segment == DocRemoval::kNoSegment) {
            Malformed(kWhat, "a segment number is out of range");
          }
          if (!removal.segments.empty() && removal.segments.back() >= segment) {
            Malformed(kWhat, "its segments are out of order");
          }
          const auto docs = obj.ReadProperty<std::string>(kFieldDocs, "docs");
          if (docs.empty()) {
            Malformed(kWhat, "a segment names no rows");
          }
          removal.segments.emplace_back(segment);
          removal.docs.emplace_back(ReadMask(docs));
        });
      });
    return removal;
  });
  if (removal.segments.empty()) {
    Malformed(kWhat, "it names no segment");
  }
  return removal;
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
                            const std::vector<doc_id_t>& docs) -> void {
      const auto map_docs = [&](const DocMap& map) {
        auto moved = docs;
        MapDocs(map, moved);
        if (moved.empty()) {
          return;
        }
        if (!live.contains(map.target)) {
          self(map.target, moved);
          return;
        }
        reached.insert(map.target);
        for (const auto doc : moved) {
          mapped.Add(map.target, doc);
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
    };
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      if (!live.contains(removal->segments[group])) {
        follow(removal->segments[group], removal->Docs(group).Docs());
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
