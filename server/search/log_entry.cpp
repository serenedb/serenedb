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

#include "search/log_entry.h"

#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "connector/column_id.h"
#include "search/store.h"

namespace sdb::search {
namespace {

enum class LogEntryType : uint8_t {
  Truncate,
  AddSegments,
  Rows,
  Removal,
  Replace,
  State,
  Merge,
};

constexpr std::align_val_t kFrozenAlignment{32};

template<typename Write>
std::string Encode(LogEntryType type, Write&& write) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteProperty(100, "type", static_cast<uint8_t>(type));
  write(serializer);
  serializer.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

std::string Frozen(const irs::DocumentMask* mask) {
  if (!mask) {
    return {};
  }
  const auto size = mask->FrozenSize();
  std::unique_ptr<char, decltype([](char* frozen) {
                    ::operator delete(frozen, kFrozenAlignment);
                  })>
    aligned{static_cast<char*>(::operator new(size, kFrozenAlignment))};
  mask->WriteFrozen(aligned.get());
  return std::string{aligned.get(), size};
}

std::shared_ptr<const irs::DocumentMask> Thawed(std::string_view bytes) {
  if (bytes.empty()) {
    return nullptr;
  }
  std::shared_ptr<irs::byte_type> buffer{
    static_cast<irs::byte_type*>(
      ::operator new(bytes.size(), kFrozenAlignment)),
    [](irs::byte_type* data) { ::operator delete(data, kFrozenAlignment); }};
  std::memcpy(buffer.get(), bytes.data(), bytes.size());
  return std::make_shared<const irs::DocumentMask>(std::move(buffer),
                                                   bytes.size());
}

void WriteSegment(duckdb::Serializer& object, const ReplacedSegment& segment) {
  object.WriteProperty<uint64_t>(100, "segment", segment.id);
  object.WriteProperty(101, "meta_file", segment.meta_file);
  object.WriteProperty<uint32_t>(102, "docs", segment.docs);
}

ReplacedSegment ReadSegment(duckdb::Deserializer& object) {
  ReplacedSegment segment;
  segment.id = object.ReadProperty<uint64_t>(100, "segment");
  segment.meta_file = object.ReadProperty<std::string>(101, "meta_file");
  segment.docs = object.ReadProperty<uint32_t>(102, "docs");
  return segment;
}

void WriteSegments(duckdb::Serializer& serializer, duckdb::field_id_t field,
                   const char* tag, std::span<const ReplacedSegment> segments) {
  serializer.WriteList(field, tag, segments.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         list.WriteObject([&](duckdb::Serializer& object) {
                           WriteSegment(object, segments[i]);
                         });
                       });
}

std::vector<ReplacedSegment> ReadSegments(duckdb::Deserializer& deserializer,
                                          duckdb::field_id_t field,
                                          const char* tag) {
  std::vector<ReplacedSegment> segments;
  deserializer.ReadList(field, tag,
                        [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                          list.ReadObject([&](duckdb::Deserializer& object) {
                            segments.push_back(ReadSegment(object));
                          });
                        });
  return segments;
}

irs::DocRemoval ReadRemoval(duckdb::Deserializer& deserializer) {
  irs::DocRemoval removal;
  deserializer.ReadList(
    101, "groups", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        const auto segment = object.ReadProperty<uint64_t>(100, "segment");
        const auto docs = object.ReadProperty<std::string>(101, "docs");
        auto thawed = Thawed(docs);
        if (!thawed) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                          ERR_MSG("search log: a removal without documents"));
        }
        removal.groups.push_back(
          {.segment = segment,
           .docs = irs::DocumentMaskBuilder{*thawed}.Finish(
             irs::DocumentMaskBuilder::kCanonical)});
      });
    });
  return removal;
}

std::vector<irs::DocMap> ReadMaps(duckdb::Deserializer& deserializer) {
  std::vector<irs::DocMap> maps;
  deserializer.ReadList(
    103, "maps", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        irs::DocMap map;
        map.source = object.ReadProperty<uint64_t>(100, "source");
        map.target = object.ReadProperty<uint64_t>(101, "target");
        map.base = object.ReadProperty<uint32_t>(102, "base");
        map.rank_begin = object.ReadProperty<uint32_t>(103, "rank_begin");
        map.rank_end = object.ReadProperty<uint32_t>(104, "rank_end");
        map.mask = Thawed(object.ReadProperty<std::string>(105, "mask"));
        maps.push_back(std::move(map));
      });
    });
  return maps;
}

}  // namespace

std::string LogTruncate() {
  return Encode(LogEntryType::Truncate, [](duckdb::Serializer&) {});
}

std::string LogAddSegments(std::span<const OwnSegment> segments) {
  std::vector<ReplacedSegment> named;
  named.reserve(segments.size());
  for (const auto& segment : segments) {
    named.push_back({.id = segment.reader->Id(),
                     .meta_file = segment.segment.filename,
                     .docs = segment.reader->Meta().docs_count});
  }
  return Encode(LogEntryType::AddSegments, [&](duckdb::Serializer& serializer) {
    WriteSegments(serializer, 101, "segments", named);
  });
}

std::string LogRows(uint64_t segment, irs::doc_id_t first,
                    duckdb::DataChunk& chunk) {
  SDB_ASSERT(chunk.size() > 0);
  return Encode(LogEntryType::Rows, [&](duckdb::Serializer& serializer) {
    serializer.WriteProperty<uint64_t>(101, "segment", segment);
    serializer.WriteProperty<uint32_t>(102, "first", first);
    serializer.WriteProperty(103, "chunk", chunk);
  });
}

std::string LogRemoval(const irs::DocRemoval& removal) {
  SDB_ASSERT(!removal.groups.empty());
  return Encode(LogEntryType::Removal, [&](duckdb::Serializer& serializer) {
    serializer.WriteList(
      101, "groups", removal.groups.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& group = removal.groups[i];
        list.WriteObject([&](duckdb::Serializer& object) {
          object.WriteProperty<uint64_t>(100, "segment", group.segment);
          object.WriteProperty(101, "docs", Frozen(&group.docs));
        });
      });
  });
}

std::string LogReplace(std::span<const uint64_t> replaced,
                       std::span<const ReplacedSegment> segments,
                       std::span<const irs::DocMap> maps) {
  return Encode(LogEntryType::Replace, [&](duckdb::Serializer& serializer) {
    serializer.WriteList(101, "replaced", replaced.size(),
                         [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                           list.WriteElement<uint64_t>(replaced[i]);
                         });
    WriteSegments(serializer, 102, "segments", segments);
    serializer.WriteList(
      103, "maps", maps.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& map = maps[i];
        list.WriteObject([&](duckdb::Serializer& object) {
          object.WriteProperty<uint64_t>(100, "source", map.source);
          object.WriteProperty<uint64_t>(101, "target", map.target);
          object.WriteProperty<uint32_t>(102, "base", map.base);
          object.WriteProperty<uint32_t>(103, "rank_begin", map.rank_begin);
          object.WriteProperty<uint32_t>(104, "rank_end", map.rank_end);
          object.WriteProperty(105, "mask", Frozen(map.mask.get()));
        });
      });
  });
}

std::string LogMerge(uint64_t target, std::span<const uint64_t> sources) {
  return Encode(LogEntryType::Merge, [&](duckdb::Serializer& serializer) {
    serializer.WriteProperty<uint64_t>(101, "target", target);
    serializer.WriteList(102, "sources", sources.size(),
                         [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                           list.WriteElement<uint64_t>(sources[i]);
                         });
  });
}

std::string LogState(std::span<const StateUnit> units) {
  return Encode(LogEntryType::State, [&](duckdb::Serializer& serializer) {
    serializer.WriteList(101, "units", units.size(),
                         [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                           const auto& unit = units[i];
                           list.WriteObject([&](duckdb::Serializer& object) {
                             WriteSegment(object, unit.segment);
                             object.WriteProperty(103, "deleted",
                                                  Frozen(unit.deleted.get()));
                           });
                         });
  });
}

void ReplayLogEntry(duckdb::ClientContext& context,
                    catalog::SearchTableEntry& table, std::string& entry,
                    Lsn lsn) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(entry.data()), entry.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  auto& store = *table.Storage();
  switch (static_cast<LogEntryType>(
    deserializer.ReadProperty<uint8_t>(100, "type"))) {
    case LogEntryType::Truncate:
      store.ReplayTruncate(lsn);
      break;
    case LogEntryType::AddSegments:
      for (const auto& segment : ReadSegments(deserializer, 101, "segments")) {
        store.ReplayAddSegment(segment, lsn);
      }
      break;
    case LogEntryType::Rows: {
      const auto segment = deserializer.ReadProperty<uint64_t>(101, "segment");
      const auto first = deserializer.ReadProperty<uint32_t>(102, "first");
      auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
      deserializer.ReadObject(103, "chunk", [&](duckdb::Deserializer& object) {
        chunk->Deserialize(object);
      });
      std::vector<connector::ColumnId> column_ids;
      for (const auto& column : table.GetColumns().Physical()) {
        column_ids.emplace_back(connector::TableColumnId(column));
      }
      store.ReplayRows(table.ParentCatalog(), std::move(column_ids), segment,
                       first, std::move(chunk), lsn);
      break;
    }
    case LogEntryType::Removal:
      store.ReplayRemoval(ReadRemoval(deserializer), lsn);
      break;
    case LogEntryType::Replace: {
      std::vector<uint64_t> replaced;
      deserializer.ReadList(
        101, "replaced", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
          replaced.push_back(list.ReadElement<uint64_t>());
        });
      const auto segments = ReadSegments(deserializer, 102, "segments");
      const auto maps = ReadMaps(deserializer);
      store.ReplayReplace(replaced, segments, maps, lsn);
      break;
    }
    case LogEntryType::State: {
      std::vector<StateUnit> units;
      deserializer.ReadList(
        101, "units", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::Deserializer& object) {
            auto& unit = units.emplace_back();
            unit.segment = ReadSegment(object);
            unit.deleted =
              Thawed(object.ReadProperty<std::string>(103, "deleted"));
          });
        });
      store.ReplayState(units, lsn);
      break;
    }
    case LogEntryType::Merge: {
      const auto target = deserializer.ReadProperty<uint64_t>(101, "target");
      std::vector<uint64_t> sources;
      deserializer.ReadList(
        102, "sources", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
          sources.push_back(list.ReadElement<uint64_t>());
        });
      store.ReplayMerge(target, sources, lsn);
      break;
    }
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                      ERR_MSG("unknown search log entry"));
  }
  deserializer.End();
}

}  // namespace sdb::search
