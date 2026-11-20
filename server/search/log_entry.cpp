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

constexpr std::align_val_t kFrozenAlignment{32};

[[noreturn]] void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("search log: ", what));
}

void WriteMask(duckdb::BinarySerializer& object, catalog::RecordBuffer& out,
               duckdb::field_id_t field, const char* tag,
               const irs::DocumentMask& mask) {
  const auto size = mask.FrozenSize();
  object.OnPropertyBegin(field, tag);
  object.WriteValue(uint64_t{size});
  mask.WriteFrozen(reinterpret_cast<char*>(out.Alloc(size)));
  object.OnPropertyEnd();
}

std::shared_ptr<const irs::DocumentMask> ReadMask(
  duckdb::BinaryDeserializer& object) {
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

void WriteValues(duckdb::BinarySerializer& column,
                 const irs::ColumnPatch& patch) {
  std::vector<const duckdb::Vector*> parts;
  patch.ForEachChunk([&](uint64_t, const irs::ColumnPatch::Chunk& chunk) {
    parts.push_back(&chunk.values);
  });
  column.WriteObject(102, "values", [&](duckdb::BinarySerializer& values) {
    duckdb::Vector::SerializeParts(values, patch.Type(), parts);
  });
}

void WritePatches(duckdb::BinarySerializer& unit, catalog::RecordBuffer& out,
                  std::span<const irs::FieldPatch> patches,
                  const irs::DocumentMask* patched) {
  std::vector<std::vector<const irs::FieldPatch*>> groups;
  for (const auto& field : patches) {
    const auto it = absl::c_find_if(groups, [&](const auto& group) {
      return patched || group.front()->patch->SameRows(*field.patch);
    });
    if (it == groups.end()) {
      groups.push_back({&field});
    } else {
      it->push_back(&field);
    }
  }
  unit.WriteOptionalList(
    102, "patches", groups.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t g) {
      const auto& group = groups[g];
      list.WriteObject([&](duckdb::BinarySerializer& object) {
        if (patched) {
          WriteMask(object, out, 100, "rows", *patched);
        } else {
          irs::DocumentMaskBuilder rows;
          group.front()->patch->AddRows(rows);
          WriteMask(
            object, out, 100, "rows",
            std::move(rows).Finish(irs::DocumentMaskBuilder::kCanonical));
        }
        object.WriteList(
          101, "columns", group.size(),
          [&](duckdb::BinarySerializer::List& columns, duckdb::idx_t c) {
            const auto& [field, patch] = *group[c];
            columns.WriteObject([&](duckdb::BinarySerializer& column) {
              column.WriteProperty<uint64_t>(100, "field", field);
              column.WriteProperty(101, "type", patch->Type());
              WriteValues(column, *patch);
            });
          });
      });
    });
}

void WriteUnits(duckdb::BinarySerializer& object, catalog::RecordBuffer& out,
                duckdb::field_id_t field, const char* tag,
                std::span<const UnitDelta> units) {
  object.WriteOptionalList(
    field, tag, units.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      const auto& changed = units[i];
      list.WriteObject([&](duckdb::BinarySerializer& unit) {
        unit.WriteProperty<uint64_t>(100, "unit", changed.unit);
        if (changed.deleted) {
          WriteMask(unit, out, 101, "deleted", *changed.deleted);
        }
        WritePatches(unit, out, changed.patches, changed.patched.get());
      });
    });
}

void WriteRows(duckdb::BinarySerializer& run, const LoggedRows& rows) {
  run.WriteProperty<uint64_t>(100, "first", rows.first);
  run.WriteList(101, "columns", rows.columns.size(),
                [&](duckdb::BinarySerializer::List& columns, duckdb::idx_t c) {
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

void ReadPatchGroup(duckdb::BinaryDeserializer& group, uint64_t first,
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
  group.ReadList(
    101, "columns",
    [&](duckdb::BinaryDeserializer::List& columns, duckdb::idx_t) {
      columns.ReadObject([&](duckdb::BinaryDeserializer& column) {
        const auto field = static_cast<irs::field_id>(
          column.ReadProperty<uint64_t>(100, "field"));
        const auto type = column.ReadProperty<duckdb::LogicalType>(101, "type");
        duckdb::Vector values{type, rows.size()};
        column.ReadObject(102, "values",
                          [&](duckdb::BinaryDeserializer& vector) {
                            values.Deserialize(vector, rows.size());
                          });
        if (values.GetVectorType() == duckdb::VectorType::FLAT_VECTOR ||
            values.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
          duckdb::FlatVector::SetSize(values, rows.size());
        }
        irs::ColumnPatch::Builder builder{type};
        builder.Add(rows, values);
        patches.push_back(
          {.field = field, .patch = std::move(builder).Finish()});
      });
    });
}

template<typename Apply>
void ReadUnits(duckdb::BinaryDeserializer& object, duckdb::field_id_t field,
               const char* tag, bool piece, const Apply& apply) {
  object.ReadOptionalList(
    field, tag, [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& unit) {
        const auto id = unit.ReadProperty<uint64_t>(100, "unit");
        std::shared_ptr<const irs::DocumentMask> deleted;
        const auto has_deleted = unit.OnOptionalPropertyBegin(101, "deleted");
        if (has_deleted) {
          deleted = ReadMask(unit);
        }
        unit.OnOptionalPropertyEnd(has_deleted);
        std::vector<irs::FieldPatch> patches;
        unit.ReadOptionalList(
          102, "patches",
          [&](duckdb::BinaryDeserializer::List& groups, duckdb::idx_t) {
            groups.ReadObject([&](duckdb::BinaryDeserializer& group) {
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
  object.WriteOptionalList(
    100, "drop", delta.drop.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      list.WriteElement<uint64_t>(delta.drop[i]);
    });
  object.WritePropertyWithDefault<uint64_t>(101, "drop_memory",
                                            delta.drop_memory, 0);
  object.WriteOptionalList(
    102, "add", delta.add.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      const auto& added = delta.add[i];
      list.WriteObject([&](duckdb::BinarySerializer& segment) {
        segment.WriteProperty<uint64_t>(100, "segment", added.id);
        segment.WriteProperty<uint8_t>(101, "kinds", added.kinds);
        segment.WriteProperty<uint32_t>(102, "rows", added.rows);
        segment.WriteOptionalList(
          103, "attachments", added.attachments.size(),
          [&](duckdb::BinarySerializer::List& files, duckdb::idx_t f) {
            files.WriteObject([&](duckdb::BinarySerializer& attachment) {
              attachment.WriteProperty<uint64_t>(100, "attachment",
                                                 added.attachments[f].id);
              attachment.WriteProperty<uint8_t>(101, "kinds",
                                                added.attachments[f].kinds);
            });
          });
      });
    });
  object.WriteOptionalList(
    103, "rows", delta.rows.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      list.WriteObject(
        [&](duckdb::BinarySerializer& run) { WriteRows(run, delta.rows[i]); });
    });
  WriteUnits(object, out, 104, "segments", delta.segments);
  WriteUnits(object, out, 105, "pieces", delta.pieces);
  if (delta.manifest) {
    object.OnPropertyBegin(106, "manifest");
    delta.manifest->Write(object);
    object.OnPropertyEnd();
  }
  object.WriteOptionalList(
    107, "add_attachments", delta.add_attachments.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      const auto& attached = delta.add_attachments[i];
      list.WriteObject([&](duckdb::BinarySerializer& attachment) {
        attachment.WriteProperty<uint64_t>(100, "segment", attached.segment);
        attachment.WriteProperty<uint64_t>(101, "attachment",
                                           attached.files.id);
        attachment.WriteProperty<uint8_t>(102, "kinds", attached.files.kinds);
      });
    });
}

void ReplayTableDelta(duckdb::ClientContext& context, TableReplay* replay,
                      duckdb::Catalog* catalog,
                      duckdb::BinaryDeserializer& object) {
  std::vector<uint64_t> drop;
  object.ReadOptionalList(
    100, "drop", [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      drop.push_back(list.ReadElement<uint64_t>());
    });
  const auto drop_memory =
    object.ReadPropertyWithExplicitDefault<uint64_t>(101, "drop_memory", 0);
  if (replay && (!drop.empty() || drop_memory != 0)) {
    replay->Drop(drop, drop_memory);
  }
  object.ReadOptionalList(
    102, "add", [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& segment) {
        LoggedSegment added;
        added.id = segment.ReadProperty<uint64_t>(100, "segment");
        added.kinds = segment.ReadProperty<uint8_t>(101, "kinds");
        added.rows = segment.ReadProperty<uint32_t>(102, "rows");
        segment.ReadOptionalList(
          103, "attachments",
          [&](duckdb::BinaryDeserializer::List& files, duckdb::idx_t) {
            files.ReadObject([&](duckdb::BinaryDeserializer& attachment) {
              auto& file = added.attachments.emplace_back();
              file.id = attachment.ReadProperty<uint64_t>(100, "attachment");
              file.kinds = attachment.ReadProperty<uint8_t>(101, "kinds");
            });
          });
        if (replay) {
          replay->Add(added);
        }
      });
    });
  object.ReadOptionalList(
    103, "rows", [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& run) {
        auto row = run.ReadProperty<uint64_t>(100, "first");
        auto columns = std::make_shared<std::vector<connector::ColumnId>>();
        run.ReadList(101, "columns",
                     [&](duckdb::BinaryDeserializer::List& ids, duckdb::idx_t) {
                       columns->push_back(static_cast<connector::ColumnId>(
                         ids.ReadElement<uint64_t>()));
                     });
        run.ReadList(
          102, "chunks",
          [&](duckdb::BinaryDeserializer::List& chunks, duckdb::idx_t) {
            auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
            chunks.ReadObject([&](duckdb::BinaryDeserializer& read) {
              chunk->Deserialize(read);
            });
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
  object.ReadOptionalList(
    107, "add_attachments",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& attachment) {
        const auto segment = attachment.ReadProperty<uint64_t>(100, "segment");
        irs::SegmentAttachment files;
        files.id = attachment.ReadProperty<uint64_t>(101, "attachment");
        files.kinds = attachment.ReadProperty<uint8_t>(102, "kinds");
        if (replay) {
          replay->AddAttachment(segment, files);
        }
      });
    });
}

}  // namespace sdb::search
