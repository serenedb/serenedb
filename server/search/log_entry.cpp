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
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
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
#include "search/parallel.h"

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

void WriteValues(duckdb::BinarySerializer& values,
                 const irs::ColumnPatch& patch) {
  std::vector<const duckdb::Vector*> parts;
  patch.ForEachChunk([&](uint64_t, const irs::ColumnPatch::Chunk& chunk) {
    parts.push_back(&chunk.values);
  });
  duckdb::Vector::SerializeParts(values, patch.Type(), parts);
}

struct Property {
  duckdb::field_id_t field = 0;
  const char* tag = nullptr;
};

void WriteStaged(duckdb::BinarySerializer& object, catalog::RecordBuffer& out,
                 Property plain, Property packed,
                 const irs::compression::Compressed& staged) {
  if (irs::compression::IsPlain(staged)) {
    object.OnPropertyBegin(plain.field, plain.tag);
    out.WriteData(staged.slots.get(), staged.frames.raw_bytes);
    object.OnPropertyEnd();
    return;
  }
  object.WriteObject(packed.field, packed.tag,
                     [&](duckdb::BinarySerializer& frames) {
                       irs::compression::WritePacked(frames, staged);
                     });
}

void WritePatches(duckdb::BinarySerializer& unit, catalog::RecordBuffer& out,
                  const UnitDelta& changed) {
  const auto& patches = changed.patches;
  const auto* patched = changed.patched.get();
  std::vector<std::vector<size_t>> groups;
  for (size_t f = 0; f != patches.size(); ++f) {
    const auto it = absl::c_find_if(groups, [&](const auto& group) {
      return patched ||
             patches[group.front()].patch->SameRows(*patches[f].patch);
    });
    if (it == groups.end()) {
      groups.push_back({f});
    } else {
      it->push_back(f);
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
          patches[group.front()].patch->AddRows(rows);
          WriteMask(
            object, out, 100, "rows",
            std::move(rows).Finish(irs::DocumentMaskBuilder::kCanonical));
        }
        object.WriteList(
          101, "columns", group.size(),
          [&](duckdb::BinarySerializer::List& columns, duckdb::idx_t c) {
            const auto f = group[c];
            const auto& [field, patch] = patches[f];
            const auto* staged =
              f < changed.staged.size() ? changed.staged[f].get() : nullptr;
            columns.WriteObject([&](duckdb::BinarySerializer& column) {
              column.WriteProperty<uint64_t>(100, "field", field);
              column.WriteProperty(101, "type", patch->Type());
              if (staged) {
                WriteStaged(column, out, {.field = 102, .tag = "values"},
                            {.field = 103, .tag = "packed_values"}, *staged);
              } else {
                column.WriteObject(102, "values",
                                   [&](duckdb::BinarySerializer& values) {
                                     WriteValues(values, *patch);
                                   });
              }
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
        WritePatches(unit, out, changed);
      });
    });
}

struct PackJob {
  const irs::ColumnPatch* patch = nullptr;
  std::shared_ptr<const irs::compression::Compressed>* staged = nullptr;
};

void AddPackJobs(std::span<const irs::FieldPatch> fields, StagedValues& staged,
                 std::vector<PackJob>& jobs) {
  if (fields.empty()) {
    return;
  }
  staged.resize(fields.size());
  for (size_t f = 0; f != fields.size(); ++f) {
    if (!staged[f]) {
      jobs.push_back({.patch = fields[f].patch.get(), .staged = &staged[f]});
    }
  }
}

void WriteChunks(duckdb::BinarySerializer& object, const LoggedRows& rows) {
  object.OnListBegin(rows.chunks);
  size_t written = 0;
  if (rows.chunks != 0) {
    rows.scan([&](duckdb::DataChunk& chunk) {
      object.OnObjectBegin();
      chunk.Serialize(object);
      object.OnObjectEnd();
      ++written;
    });
  }
  SDB_ASSERT(written == rows.chunks);
  object.OnListEnd();
}

void PackBlobs(std::span<LoggedRows* const> runs,
               std::span<const PackJob> values, uint32_t frame_bytes,
               irs::ParallelFor parallel) {
  const auto blobs = runs.size() + values.size();
  if (blobs == 0) {
    return;
  }
  std::vector<irs::compression::FrameWriter> raws;
  raws.reserve(blobs);
  for (size_t i = 0; i != blobs; ++i) {
    raws.emplace_back(frame_bytes);
  }
  parallel(blobs, [&](size_t i) {
    duckdb::BinarySerializer serializer{raws[i]};
    if (i < runs.size()) {
      WriteChunks(serializer, *runs[i]);
    } else {
      serializer.Begin();
      WriteValues(serializer, *values[i - runs.size()].patch);
      serializer.End();
    }
  });
  auto staged =
    irs::compression::Compress(irs::compression::kZstd1, raws, parallel);
  for (size_t i = 0; i != blobs; ++i) {
    if (!irs::compression::Pays(staged[i])) {
      irs::compression::KeepPlain(staged[i], raws[i]);
    }
  }
  for (size_t i = 0; i != runs.size(); ++i) {
    runs[i]->staged = std::move(staged[i]);
    runs[i]->scan = nullptr;
  }
  for (size_t j = 0; j != values.size(); ++j) {
    *values[j].staged = std::make_shared<const irs::compression::Compressed>(
      std::move(staged[runs.size() + j]));
  }
}

void WriteRows(duckdb::BinarySerializer& run, catalog::RecordBuffer& out,
               const LoggedRows& rows) {
  run.WriteProperty<uint64_t>(100, "first", rows.first);
  run.WriteList(101, "columns", rows.columns.size(),
                [&](duckdb::BinarySerializer::List& columns, duckdb::idx_t c) {
                  columns.WriteElement<uint64_t>(rows.columns[c]);
                });
  if (rows.staged.slots) {
    WriteStaged(run, out, {.field = 102, .tag = "chunks"},
                {.field = 103, .tag = "packed_chunks"}, rows.staged);
    return;
  }
  run.OnPropertyBegin(102, "chunks");
  WriteChunks(run, rows);
  run.OnPropertyEnd();
}

duckdb::unique_ptr<duckdb::DataChunk> ReadChunk(
  duckdb::BinaryDeserializer::List& chunks) {
  auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
  chunks.ReadObject(
    [&](duckdb::BinaryDeserializer& read) { chunk->Deserialize(read); });
  return chunk;
}

class RowsReader {
 public:
  RowsReader(duckdb::ClientContext& context, TableReplay* replay,
             duckdb::Catalog* catalog, duckdb::MemoryStream& stream)
    : _context{context},
      _replay{replay},
      _catalog{catalog},
      _stream{stream},
      _window{
        std::max<size_t>(1, duckdb::TaskScheduler::QueryThreads(context))} {}

  void Read(duckdb::BinaryDeserializer& object) {
    Run run{.first = object.ReadProperty<uint64_t>(100, "first"),
            .columns = std::make_shared<std::vector<connector::ColumnId>>()};
    object.ReadList(101, "columns",
                    [&](duckdb::BinaryDeserializer::List& ids, duckdb::idx_t) {
                      run.columns->push_back(static_cast<connector::ColumnId>(
                        ids.ReadElement<uint64_t>()));
                    });
    auto row = run.first;
    const auto plain = object.ReadOptionalList(
      102, "chunks",
      [&](duckdb::BinaryDeserializer::List& chunks, duckdb::idx_t) {
        auto chunk = ReadChunk(chunks);
        if (_replay) {
          Flush();
          Append(row, run.columns, std::move(chunk));
        }
      });
    if (plain) {
      return;
    }
    object.ReadObject(
      103, "packed_chunks", [&](duckdb::BinaryDeserializer& packed) {
        run.packed = irs::compression::ReadPacked(packed, _stream);
      });
    if (!_replay) {
      return;
    }
    _pending.push_back(std::move(run));
    if (_pending.size() >= _window) {
      Flush();
    }
  }

  void Flush() {
    if (_pending.empty()) {
      return;
    }
    const auto parallel = ParallelOn(_context);
    std::vector<std::unique_ptr<irs::byte_type[]>> raw(_pending.size());
    parallel(_pending.size(), [&](size_t i) {
      raw[i] = irs::compression::Decompress(_pending[i].packed, parallel);
    });
    for (size_t i = 0; i != _pending.size(); ++i) {
      const auto& run = _pending[i];
      auto row = run.first;
      duckdb::MemoryStream stream{raw[i].get(), run.packed.frames.raw_bytes};
      duckdb::BinaryDeserializer deserializer{stream};
      deserializer.ReadListInternal(
        [&](duckdb::BinaryDeserializer::List& chunks, duckdb::idx_t) {
          Append(row, run.columns, ReadChunk(chunks));
        });
      if (stream.GetPosition() != run.packed.frames.raw_bytes) {
        Corrupted("rows longer than their chunks");
      }
      raw[i].reset();
    }
    _pending.clear();
  }

 private:
  struct Run {
    uint64_t first = 0;
    std::shared_ptr<std::vector<connector::ColumnId>> columns;
    irs::compression::Packed packed;
  };

  void Append(uint64_t& row,
              const std::shared_ptr<std::vector<connector::ColumnId>>& columns,
              duckdb::unique_ptr<duckdb::DataChunk> chunk) {
    const auto size = chunk->size();
    _replay->Append(*_catalog, row, columns, std::move(chunk));
    row += size;
  }

  duckdb::ClientContext& _context;
  TableReplay* _replay;
  duckdb::Catalog* _catalog;
  duckdb::MemoryStream& _stream;
  size_t _window;
  std::vector<Run> _pending;
};

std::shared_ptr<const irs::ColumnPatch> ReadValues(
  duckdb::BinaryDeserializer& object, const duckdb::LogicalType& type,
  std::span<const uint64_t> rows) {
  duckdb::Vector values{type, rows.size()};
  values.Deserialize(object, rows.size());
  if (values.GetVectorType() == duckdb::VectorType::FLAT_VECTOR ||
      values.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    duckdb::FlatVector::SetSize(values, rows.size());
  }
  irs::ColumnPatch::Builder builder{type};
  builder.Add(rows, values);
  return std::move(builder).Finish();
}

struct PackedColumn {
  size_t at = 0;
  duckdb::LogicalType type;
  std::shared_ptr<const std::vector<uint64_t>> rows;
  irs::compression::Packed packed;
};

void ReadPatchGroup(duckdb::BinaryDeserializer& group,
                    duckdb::MemoryStream& stream, uint64_t first,
                    std::vector<irs::FieldPatch>& patches,
                    std::vector<PackedColumn>& packed) {
  group.OnPropertyBegin(100, "rows");
  const auto mask = ReadMask(group);
  group.OnPropertyEnd();
  auto rows = std::make_shared<std::vector<uint64_t>>();
  rows->reserve(mask->Count());
  {
    irs::DocumentMask::Cursor cursor{*mask};
    std::array<irs::doc_id_t, 1024> docs;
    while (const auto count = cursor.Read(docs)) {
      for (const auto doc : std::span{docs}.first(count)) {
        rows->push_back(first + doc - irs::doc_limits::min());
      }
    }
  }
  if (rows->empty()) {
    Corrupted("a column patch without rows");
  }
  group.ReadList(
    101, "columns",
    [&](duckdb::BinaryDeserializer::List& columns, duckdb::idx_t) {
      columns.ReadObject([&](duckdb::BinaryDeserializer& column) {
        const auto field = static_cast<irs::field_id>(
          column.ReadProperty<uint64_t>(100, "field"));
        auto type = column.ReadProperty<duckdb::LogicalType>(101, "type");
        const auto at = patches.size();
        patches.push_back({.field = field});
        const auto plain = column.ReadOptionalObject(
          102, "values", [&](duckdb::BinaryDeserializer& values) {
            patches[at].patch = ReadValues(values, type, *rows);
          });
        if (plain) {
          return;
        }
        column.ReadObject(
          103, "packed_values", [&](duckdb::BinaryDeserializer& object) {
            packed.push_back(
              {.at = at,
               .type = std::move(type),
               .rows = rows,
               .packed = irs::compression::ReadPacked(object, stream)});
          });
      });
    });
}

template<typename Apply>
void ReadUnits(duckdb::BinaryDeserializer& object, duckdb::MemoryStream& stream,
               duckdb::field_id_t field, const char* tag, bool piece,
               const TableReplay* replay, irs::ParallelFor parallel,
               const Apply& apply) {
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
        std::vector<PackedColumn> packed;
        unit.ReadOptionalList(
          102, "patches",
          [&](duckdb::BinaryDeserializer::List& groups, duckdb::idx_t) {
            groups.ReadObject([&](duckdb::BinaryDeserializer& group) {
              ReadPatchGroup(group, stream, piece ? id : 0, patches, packed);
            });
          });
        if (!replay) {
          return;
        }
        parallel(packed.size(), [&](size_t p) {
          const auto& column = packed[p];
          const auto raw =
            irs::compression::Decompress(column.packed, parallel);
          duckdb::MemoryStream values{raw.get(),
                                      column.packed.frames.raw_bytes};
          duckdb::BinaryDeserializer deserializer{values};
          deserializer.Begin();
          patches[column.at].patch =
            ReadValues(deserializer, column.type, *column.rows);
          deserializer.End();
        });
        apply(id, deleted.get(), patches);
      });
    });
}

}  // namespace

void PackRows(LoggedRows& rows, uint32_t frame_bytes,
              irs::ParallelFor parallel) {
  LoggedRows* const runs[] = {&rows};
  PackBlobs(runs, {}, frame_bytes, parallel);
}

void PackPatches(std::span<UnitPatch> patches, uint32_t frame_bytes,
                 irs::ParallelFor parallel) {
  std::vector<PackJob> jobs;
  for (auto& patch : patches) {
    AddPackJobs(patch.fields, patch.staged, jobs);
  }
  PackBlobs({}, jobs, frame_bytes, parallel);
}

void PackDelta(TableDelta& delta, uint32_t frame_bytes,
               irs::ParallelFor parallel) {
  std::vector<LoggedRows*> runs;
  runs.reserve(delta.rows.size());
  for (auto& rows : delta.rows) {
    runs.push_back(&rows);
  }
  std::vector<PackJob> jobs;
  for (auto& unit : delta.segments) {
    AddPackJobs(unit.patches, unit.staged, jobs);
  }
  PackBlobs(runs, jobs, frame_bytes, parallel);
}

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
      });
    });
  object.WriteOptionalList(
    103, "rows", delta.rows.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      list.WriteObject([&](duckdb::BinarySerializer& run) {
        WriteRows(run, out, delta.rows[i]);
      });
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
                      duckdb::BinaryDeserializer& object,
                      duckdb::MemoryStream& stream) {
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
        if (replay) {
          replay->Add(added);
        }
      });
    });
  RowsReader rows{context, replay, catalog, stream};
  object.ReadOptionalList(
    103, "rows", [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& run) { rows.Read(run); });
    });
  rows.Flush();
  const auto parallel = ParallelOn(context);
  ReadUnits(object, stream, 104, "segments", false, replay, parallel,
            [&](uint64_t segment, const irs::DocumentMask* deleted,
                std::span<const irs::FieldPatch> patches) {
              if (deleted) {
                replay->Delete(segment, *deleted);
              }
              if (!patches.empty()) {
                replay->Update(segment, patches);
              }
            });
  ReadUnits(object, stream, 105, "pieces", true, replay, parallel,
            [&](uint64_t first, const irs::DocumentMask* deleted,
                std::span<const irs::FieldPatch> patches) {
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
