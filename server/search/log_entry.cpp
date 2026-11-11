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
#include "search/table_replay.h"

namespace sdb::search {
namespace {

constexpr std::align_val_t kFrozenAlignment{32};

std::string Frozen(const irs::DocumentMask& mask) {
  const auto size = mask.FrozenSize();
  std::unique_ptr<char, decltype([](char* frozen) {
                    ::operator delete(frozen, kFrozenAlignment);
                  })>
    aligned{static_cast<char*>(::operator new(size, kFrozenAlignment))};
  mask.WriteFrozen(aligned.get());
  return std::string{aligned.get(), size};
}

std::shared_ptr<const irs::DocumentMask> Thawed(std::string_view bytes) {
  if (bytes.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("search log: deletes without rows"));
  }
  std::shared_ptr<irs::byte_type> buffer{
    static_cast<irs::byte_type*>(
      ::operator new(bytes.size(), kFrozenAlignment)),
    [](irs::byte_type* data) { ::operator delete(data, kFrozenAlignment); }};
  std::memcpy(buffer.get(), bytes.data(), bytes.size());
  return std::make_shared<const irs::DocumentMask>(std::move(buffer),
                                                   bytes.size());
}

void WritePatch(duckdb::Serializer& object, const irs::FieldPatch& field) {
  std::vector<uint64_t> rows;
  std::vector<uint64_t> ticks;
  duckdb::Vector values{field.patch->Type(), field.patch->Count()};
  field.patch->Entries(rows, values, ticks);
  object.WriteProperty<uint64_t>(100, "field", field.field);
  object.WriteProperty(101, "type", field.patch->Type());
  object.WriteList(102, "rows", rows.size(),
                   [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                     list.WriteElement<uint64_t>(rows[i]);
                   });
  object.WriteObject(103, "values", [&](duckdb::Serializer& vector) {
    values.Serialize(vector);
  });
}

irs::FieldPatch ReadPatch(duckdb::Deserializer& object) {
  const auto field =
    static_cast<irs::field_id>(object.ReadProperty<uint64_t>(100, "field"));
  const auto type = object.ReadProperty<duckdb::LogicalType>(101, "type");
  std::vector<uint64_t> rows;
  object.ReadList(102, "rows",
                  [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                    rows.push_back(list.ReadElement<uint64_t>());
                  });
  duckdb::Vector values{type, rows.size()};
  object.ReadObject(103, "values", [&](duckdb::Deserializer& vector) {
    values.Deserialize(vector, rows.size());
  });
  if (values.GetVectorType() == duckdb::VectorType::FLAT_VECTOR ||
      values.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    duckdb::FlatVector::SetSize(values, rows.size());
  }
  if (rows.empty() || !absl::c_is_sorted(rows, std::less_equal<>{}) ||
      values.size() != rows.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("search log: a malformed column patch"));
  }
  return {.field = field,
          .patch = irs::ColumnPatch::Merge(nullptr, type, rows, values, 0)};
}

std::string LogManifest(const FileManifest& manifest) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.OnPropertyBegin(100, "manifest");
  manifest.Write(serializer);
  serializer.OnPropertyEnd();
  serializer.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

std::shared_ptr<const FileManifest> ReadManifest(std::string& bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  deserializer.OnPropertyBegin(100, "manifest");
  auto manifest = FileManifest::Read(deserializer);
  deserializer.OnPropertyEnd();
  deserializer.End();
  return manifest;
}

duckdb::unique_ptr<duckdb::DataChunk> ReadChunk(duckdb::ClientContext& context,
                                                std::string& bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
  deserializer.ReadObject(100, "chunk", [&](duckdb::Deserializer& object) {
    chunk->Deserialize(object);
  });
  deserializer.End();
  return chunk;
}

}  // namespace

std::string LogChunk(duckdb::DataChunk& chunk) {
  SDB_ASSERT(chunk.size() > 0);
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteProperty(100, "chunk", chunk);
  serializer.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

void WriteTableDelta(duckdb::Serializer& object, const TableDelta& delta) {
  if (!delta.drop.empty()) {
    object.WriteList(100, "drop", delta.drop.size(),
                     [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                       list.WriteElement<uint64_t>(delta.drop[i]);
                     });
  }
  object.WritePropertyWithDefault<uint64_t>(101, "drop_memory",
                                            delta.drop_memory, 0);
  if (!delta.add.empty()) {
    object.WriteList(
      102, "add", delta.add.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& added = delta.add[i];
        list.WriteObject([&](duckdb::Serializer& segment) {
          segment.WriteProperty<uint64_t>(100, "segment", added.id);
          segment.WriteProperty<uint8_t>(101, "kinds", added.kinds);
          segment.WriteProperty<uint32_t>(102, "rows", added.rows);
        });
      });
  }
  if (!delta.rows.empty()) {
    object.WriteList(
      103, "rows", delta.rows.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& rows = delta.rows[i];
        list.WriteObject([&](duckdb::Serializer& run) {
          run.WriteProperty<uint64_t>(100, "first", rows.first);
          run.WriteList(
            101, "columns", rows.columns.size(),
            [&](duckdb::Serializer::List& columns, duckdb::idx_t c) {
              columns.WriteElement<uint64_t>(rows.columns[c]);
            });
          run.WriteList(102, "chunks", rows.chunks.size(),
                        [&](duckdb::Serializer::List& chunks, duckdb::idx_t c) {
                          chunks.WriteElement(rows.chunks[c]);
                        });
        });
      });
  }
  if (!delta.deletes.empty()) {
    object.WriteList(
      104, "deletes", delta.deletes.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& deleted = delta.deletes[i];
        list.WriteObject([&](duckdb::Serializer& deletes) {
          deletes.WriteProperty<uint64_t>(100, "segment", deleted.segment);
          deletes.WriteProperty(101, "docs", Frozen(*deleted.docs));
        });
      });
  }
  if (delta.memory_deletes) {
    object.WriteProperty<uint64_t>(105, "memory_base", delta.memory_base);
    object.WriteProperty(106, "memory_deletes", Frozen(*delta.memory_deletes));
  }
  if (!delta.updates.empty()) {
    object.WriteList(
      107, "updates", delta.updates.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& updated = delta.updates[i];
        list.WriteObject([&](duckdb::Serializer& update) {
          update.WriteProperty<uint64_t>(100, "segment", updated.segment);
          update.WriteObject(101, "patch", [&](duckdb::Serializer& patch) {
            WritePatch(patch, updated.patch);
          });
        });
      });
  }
  if (!delta.memory_updates.empty()) {
    object.WriteList(108, "memory_updates", delta.memory_updates.size(),
                     [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                       list.WriteObject([&](duckdb::Serializer& patch) {
                         WritePatch(patch, delta.memory_updates[i]);
                       });
                     });
  }
  if (delta.manifest) {
    object.WriteProperty(109, "manifest", LogManifest(*delta.manifest));
  }
}

void ReplayTableDelta(duckdb::ClientContext& context, TableReplay* replay,
                      duckdb::Catalog* catalog, duckdb::Deserializer& object) {
  std::vector<uint64_t> drop;
  object.ReadOptionalList(100, "drop",
                          [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                            drop.push_back(list.ReadElement<uint64_t>());
                          });
  const auto drop_memory =
    object.ReadPropertyWithExplicitDefault<uint64_t>(101, "drop_memory", 0);
  if (replay && (!drop.empty() || drop_memory != 0)) {
    replay->Drop(drop, drop_memory);
  }
  object.ReadOptionalList(
    102, "add", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& segment) {
        LoggedSegment added;
        added.id = segment.ReadProperty<uint64_t>(100, "segment");
        added.kinds = segment.ReadProperty<uint8_t>(101, "kinds");
        added.rows = segment.ReadProperty<uint32_t>(102, "rows");
        if (replay) {
          replay->Add(added);
        }
      });
    });
  object.ReadOptionalList(
    103, "rows", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& run) {
        auto row = run.ReadProperty<uint64_t>(100, "first");
        auto columns = std::make_shared<std::vector<connector::ColumnId>>();
        run.ReadList(
          101, "columns", [&](duckdb::Deserializer::List& ids, duckdb::idx_t) {
            columns->push_back(
              static_cast<connector::ColumnId>(ids.ReadElement<uint64_t>()));
          });
        run.ReadList(102, "chunks",
                     [&](duckdb::Deserializer::List& chunks, duckdb::idx_t) {
                       auto bytes = chunks.ReadElement<std::string>();
                       if (!replay) {
                         return;
                       }
                       auto chunk = ReadChunk(context, bytes);
                       const auto size = chunk->size();
                       replay->Append(*catalog, row, columns, std::move(chunk));
                       row += size;
                     });
      });
    });
  object.ReadOptionalList(
    104, "deletes", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& deletes) {
        const auto segment = deletes.ReadProperty<uint64_t>(100, "segment");
        const auto docs =
          Thawed(deletes.ReadProperty<std::string>(101, "docs"));
        if (replay) {
          replay->Delete(segment, *docs);
        }
      });
    });
  const auto memory_base =
    object.ReadPropertyWithExplicitDefault<uint64_t>(105, "memory_base", 0);
  const auto memory_deletes =
    object.ReadPropertyWithExplicitDefault<std::string>(106, "memory_deletes",
                                                        {});
  if (replay && !memory_deletes.empty()) {
    replay->DeleteMemory(memory_base, *Thawed(memory_deletes));
  }
  object.ReadOptionalList(
    107, "updates", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& update) {
        const auto segment = update.ReadProperty<uint64_t>(100, "segment");
        irs::FieldPatch patch;
        update.ReadObject(101, "patch", [&](duckdb::Deserializer& object) {
          patch = ReadPatch(object);
        });
        if (replay) {
          replay->Update(segment, patch);
        }
      });
    });
  object.ReadOptionalList(108, "memory_updates",
                          [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                            list.ReadObject([&](duckdb::Deserializer& patch) {
                              const auto read = ReadPatch(patch);
                              if (replay) {
                                replay->UpdateMemory(read);
                              }
                            });
                          });
  auto manifest =
    object.ReadPropertyWithExplicitDefault<std::string>(109, "manifest", {});
  if (replay && !manifest.empty()) {
    replay->SetManifest(ReadManifest(manifest));
  }
}

}  // namespace sdb::search
