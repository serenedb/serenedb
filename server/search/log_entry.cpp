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

#include <absl/algorithm/container.h>

#include <array>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/formats/column/column_patch.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/column_id.h"
#include "connector/file_manifest.h"

namespace sdb::search {
namespace {

using Writer = duckdb::BinarySerializer;
using Reader = duckdb::BinaryDeserializer;

constexpr std::align_val_t kFrozenAlignment{32};

[[noreturn]] void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("search log: ", what));
}

void WriteMask(Writer& object, catalog::RecordBuffer& out,
               duckdb::field_id_t field, const char* tag,
               const irs::DocumentMask& mask) {
  const auto size = mask.FrozenSize();
  object.OnPropertyBegin(field, tag);
  object.WriteValue(uint64_t{size});
  mask.WriteFrozen(reinterpret_cast<char*>(out.Alloc(size)));
  object.OnPropertyEnd();
}

std::shared_ptr<const irs::DocumentMask> ReadMask(Reader& object) {
  const auto size = object.ReadUnsignedInt64();
  if (size == 0) {
    Corrupted("an empty mask");
  }
  std::shared_ptr<irs::byte_type> frozen{
    static_cast<irs::byte_type*>(::operator new(size, kFrozenAlignment)),
    [](irs::byte_type* data) { ::operator delete(data, kFrozenAlignment); }};
  object.GetStream().ReadData(frozen.get(), size);
  return std::make_shared<const irs::DocumentMask>(std::move(frozen), size);
}

void WriteValues(Writer& column, const irs::ColumnPatch& patch) {
  std::vector<const duckdb::Vector*> parts;
  patch.ForEachChunk([&](uint64_t, const irs::ColumnPatch::Chunk& chunk) {
    parts.push_back(&chunk.values);
  });
  column.WriteObject(102, "values", [&](Writer& values) {
    duckdb::Vector::SerializeParts(values, patch.Type(), parts);
  });
}

void WritePatches(Writer& unit, catalog::RecordBuffer& out,
                  std::span<const irs::FieldPatch> patches) {
  std::vector<std::vector<const irs::FieldPatch*>> groups;
  for (const auto& field : patches) {
    const auto it = absl::c_find_if(groups, [&](const auto& group) {
      return group.front()->patch->SameRows(*field.patch);
    });
    if (it == groups.end()) {
      groups.push_back({&field});
    } else {
      it->push_back(&field);
    }
  }
  unit.WriteList(
    102, "patches", groups.size(), [&](Writer::List& list, duckdb::idx_t g) {
      const auto& group = groups[g];
      list.WriteObject([&](Writer& object) {
        irs::DocumentMaskBuilder rows;
        group.front()->patch->AddRows(rows);
        WriteMask(object, out, 100, "rows",
                  std::move(rows).Finish(irs::DocumentMaskBuilder::kCanonical));
        object.WriteList(101, "columns", group.size(),
                         [&](Writer::List& columns, duckdb::idx_t c) {
                           const auto& [field, patch] = *group[c];
                           columns.WriteObject([&](Writer& column) {
                             column.WriteProperty<uint64_t>(100, "field",
                                                            field);
                             column.WriteProperty(101, "type", patch->Type());
                             WriteValues(column, *patch);
                           });
                         });
      });
    });
}

void WriteUnits(Writer& object, catalog::RecordBuffer& out,
                duckdb::field_id_t field, const char* tag,
                std::span<const UnitDelta> units) {
  if (units.empty()) {
    return;
  }
  object.WriteList(field, tag, units.size(),
                   [&](Writer::List& list, duckdb::idx_t i) {
                     const auto& changed = units[i];
                     list.WriteObject([&](Writer& unit) {
                       unit.WriteProperty<uint64_t>(100, "unit", changed.unit);
                       if (changed.deleted) {
                         WriteMask(unit, out, 101, "deleted", *changed.deleted);
                       }
                       if (!changed.patches.empty()) {
                         WritePatches(unit, out, changed.patches);
                       }
                     });
                   });
}

void WriteRows(Writer& run, const LoggedRows& rows) {
  run.WriteProperty<uint64_t>(100, "first", rows.first);
  run.WriteList(101, "columns", rows.columns.size(),
                [&](Writer::List& columns, duckdb::idx_t c) {
                  columns.WriteElement<uint64_t>(rows.columns[c]);
                });
  run.OnPropertyBegin(102, "chunks");
  run.OnListBegin(rows.chunks);
  size_t written = 0;
  if (rows.chunks != 0) {
    rows.scan([&](duckdb::DataChunk& chunk) {
      run.OnObjectBegin();
      chunk.Serialize(run);
      run.OnObjectEnd();
      ++written;
    });
  }
  SDB_ASSERT(written == rows.chunks);
  run.OnListEnd();
  run.OnPropertyEnd();
}

void ReadPatchGroup(Reader& group, uint64_t first,
                    std::vector<irs::FieldPatch>& patches) {
  group.OnPropertyBegin(100, "rows");
  const auto mask = ReadMask(group);
  group.OnPropertyEnd();
  std::vector<uint64_t> rows;
  rows.reserve(mask->Count());
  {
    irs::DocumentMask::Cursor cursor{*mask};
    std::array<irs::doc_id_t, 1024> docs;
    while (const auto count = cursor.Read(docs)) {
      for (const auto doc : std::span{docs}.first(count)) {
        rows.push_back(first + doc - irs::doc_limits::min());
      }
    }
  }
  if (rows.empty()) {
    Corrupted("a column patch without rows");
  }
  group.ReadList(101, "columns", [&](Reader::List& columns, duckdb::idx_t) {
    columns.ReadObject([&](Reader& column) {
      const auto field =
        static_cast<irs::field_id>(column.ReadProperty<uint64_t>(100, "field"));
      const auto type = column.ReadProperty<duckdb::LogicalType>(101, "type");
      duckdb::Vector values{type, rows.size()};
      column.ReadObject(102, "values", [&](Reader& vector) {
        values.Deserialize(vector, rows.size());
      });
      if (values.GetVectorType() == duckdb::VectorType::FLAT_VECTOR ||
          values.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        duckdb::FlatVector::SetSize(values, rows.size());
      }
      irs::ColumnPatch::Builder builder{type};
      builder.Add(rows, values);
      patches.push_back({.field = field, .patch = std::move(builder).Finish()});
    });
  });
}

template<typename Apply>
void ReadUnits(Reader& object, duckdb::field_id_t field, const char* tag,
               bool piece, const Apply& apply) {
  object.ReadOptionalList(field, tag, [&](Reader::List& list, duckdb::idx_t) {
    list.ReadObject([&](Reader& unit) {
      const auto id = unit.ReadProperty<uint64_t>(100, "unit");
      std::shared_ptr<const irs::DocumentMask> deleted;
      const auto has_deleted = unit.OnOptionalPropertyBegin(101, "deleted");
      if (has_deleted) {
        deleted = ReadMask(unit);
      }
      unit.OnOptionalPropertyEnd(has_deleted);
      std::vector<irs::FieldPatch> patches;
      unit.ReadOptionalList(102, "patches",
                            [&](Reader::List& groups, duckdb::idx_t) {
                              groups.ReadObject([&](Reader& group) {
                                ReadPatchGroup(group, piece ? id : 0, patches);
                              });
                            });
      apply(id, deleted.get(), patches);
    });
  });
}

}  // namespace

void WriteTableDelta(duckdb::BinarySerializer& object,
                     catalog::RecordBuffer& out, const TableDelta& delta) {
  object.WriteOptionalList(100, "drop", delta.drop.size(),
                           [&](Writer::List& list, duckdb::idx_t i) {
                             list.WriteElement<uint64_t>(delta.drop[i]);
                           });
  object.WritePropertyWithDefault<uint64_t>(101, "drop_memory",
                                            delta.drop_memory, 0);
  object.WriteOptionalList(
    102, "add", delta.add.size(), [&](Writer::List& list, duckdb::idx_t i) {
      const auto& added = delta.add[i];
      list.WriteObject([&](Writer& segment) {
        segment.WriteProperty<uint64_t>(100, "segment", added.id);
        segment.WriteProperty<uint8_t>(101, "kinds", added.kinds);
        segment.WriteProperty<uint32_t>(102, "rows", added.rows);
      });
    });
  object.WriteOptionalList(
    103, "rows", delta.rows.size(), [&](Writer::List& list, duckdb::idx_t i) {
      list.WriteObject([&](Writer& run) { WriteRows(run, delta.rows[i]); });
    });
  WriteUnits(object, out, 104, "segments", delta.segments);
  WriteUnits(object, out, 105, "pieces", delta.pieces);
  if (delta.manifest) {
    object.OnPropertyBegin(106, "manifest");
    delta.manifest->Write(object);
    object.OnPropertyEnd();
  }
}

void ReplayTableDelta(duckdb::ClientContext& context, TableReplay* replay,
                      duckdb::Catalog* catalog,
                      duckdb::BinaryDeserializer& object) {
  std::vector<uint64_t> drop;
  object.ReadOptionalList(100, "drop", [&](Reader::List& list, duckdb::idx_t) {
    drop.push_back(list.ReadElement<uint64_t>());
  });
  const auto drop_memory =
    object.ReadPropertyWithExplicitDefault<uint64_t>(101, "drop_memory", 0);
  if (replay && (!drop.empty() || drop_memory != 0)) {
    replay->Drop(drop, drop_memory);
  }
  object.ReadOptionalList(102, "add", [&](Reader::List& list, duckdb::idx_t) {
    list.ReadObject([&](Reader& segment) {
      LoggedSegment added;
      added.id = segment.ReadProperty<uint64_t>(100, "segment");
      added.kinds = segment.ReadProperty<uint8_t>(101, "kinds");
      added.rows = segment.ReadProperty<uint32_t>(102, "rows");
      if (replay) {
        replay->Add(added);
      }
    });
  });
  object.ReadOptionalList(103, "rows", [&](Reader::List& list, duckdb::idx_t) {
    list.ReadObject([&](Reader& run) {
      auto row = run.ReadProperty<uint64_t>(100, "first");
      auto columns = std::make_shared<std::vector<connector::ColumnId>>();
      run.ReadList(101, "columns", [&](Reader::List& ids, duckdb::idx_t) {
        columns->push_back(
          static_cast<connector::ColumnId>(ids.ReadElement<uint64_t>()));
      });
      run.ReadList(102, "chunks", [&](Reader::List& chunks, duckdb::idx_t) {
        auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
        chunks.ReadObject([&](Reader& read) { chunk->Deserialize(read); });
        if (!replay) {
          return;
        }
        const auto size = chunk->size();
        replay->Append(*catalog, row, columns, std::move(chunk));
        row += size;
      });
    });
  });
  ReadUnits(object, 104, "segments", false,
            [&](uint64_t segment, const irs::DocumentMask* deleted,
                std::span<const irs::FieldPatch> patches) {
              if (!replay) {
                return;
              }
              if (deleted) {
                replay->Delete(segment, *deleted);
              }
              if (!patches.empty()) {
                replay->Update(segment, patches);
              }
            });
  ReadUnits(object, 105, "pieces", true,
            [&](uint64_t first, const irs::DocumentMask* deleted,
                std::span<const irs::FieldPatch> patches) {
              if (!replay) {
                return;
              }
              if (deleted) {
                replay->DeleteMemory(first, *deleted);
              }
              if (!patches.empty()) {
                replay->UpdateMemory(patches);
              }
            });
  const auto has_manifest = object.OnOptionalPropertyBegin(106, "manifest");
  if (has_manifest) {
    auto manifest = FileManifest::Read(object);
    if (replay) {
      replay->SetManifest(std::move(manifest));
    }
  }
  object.OnOptionalPropertyEnd(has_manifest);
}

}  // namespace sdb::search
