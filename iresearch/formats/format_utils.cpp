////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "format_utils.hpp"

#include <absl/base/internal/endian.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <limits>

#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::format_utils {
namespace {

class EmptyMapping final : public duckdb::MemoryMappedFile {
 public:
  explicit EmptyMapping(std::string path)
    : duckdb::MemoryMappedFile{
        std::move(path), duckdb::FileFlags::FILE_FLAGS_READ, nullptr, 0} {}

  void Sync() final {}
  bool Trim(duckdb::idx_t, duckdb::idx_t) final { return false; }
  bool OnDiskFile() const final { return false; }
  void Close() final {}
};

constexpr duckdb::field_id_t kFieldDataCrc32c = 0;
constexpr duckdb::field_id_t kFieldMeta = 1;

void WritePackedMeta(duckdb::BinarySerializer& meta, duckdb::WriteStream& out,
                     FooterWriter write) {
  duckdb::MemoryStream raw;
  duckdb::BinarySerializer serializer{raw};
  serializer.Begin();
  write(serializer);
  serializer.End();
  const auto packed = compression::Compress(compression::kLz4,
                                            {raw.GetData(), raw.GetPosition()});
  const auto plain = raw.GetPosition() - sizeof(duckdb::field_id_t);
  if (sizeof(duckdb::field_id_t) + compression::PackedBytes(packed) >= plain) {
    out.WriteData(raw.GetData(), plain);
    return;
  }
  meta.WriteObject(kMetaFieldPacked, "packed",
                   [&](duckdb::BinarySerializer& object) {
                     compression::WritePacked(object, packed);
                   });
}

void ReadMeta(duckdb::BinaryDeserializer& meta, duckdb::InputFileReader& stream,
              FooterReader read, uint64_t data_len) {
  compression::Packed packed;
  const auto present = meta.ReadOptionalObject(
    kMetaFieldPacked, "packed", [&](duckdb::BinaryDeserializer& object) {
      packed = compression::ReadPacked(object, stream);
    });
  if (!present) {
    read(meta, data_len);
    return;
  }
  const auto raw = compression::Decompress(packed);
  duckdb::MemoryStream unpacked{raw.get(), packed.frames.raw_bytes};
  duckdb::BinaryDeserializer deserializer{unpacked};
  deserializer.Begin();
  read(deserializer, data_len);
  deserializer.End();
}

uint32_t WriteFooterImpl(duckdb::BufferedFileWriter& out,
                         const FooterWriter* write) {
  const uint32_t data_crc32c = out.Checksum();
  const auto begin = out.Position();
  duckdb::BinarySerializer serializer{out};
  serializer.Begin();
  serializer.WritePropertyWithDefault<uint32_t>(kFieldDataCrc32c, "data_crc32c",
                                                data_crc32c, 0);
  if (write) {
    serializer.WriteObject(kFieldMeta, "meta",
                           [&](duckdb::BinarySerializer& meta) {
                             SDB_IF_FAILURE("irs::FooterMetaUnpacked") {
                               (*write)(meta);
                               return;
                             }
                             WritePackedMeta(meta, out, *write);
                           });
  }
  serializer.End();
  const auto size = out.Position() - begin;
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(), "footer of ", size,
             " bytes does not fit its 32-bit length");
  out.Write<uint32_t>(out.Checksum());
  return static_cast<uint32_t>(size);
}

void CheckSize(const duckdb::InputFile& file, std::string_view name,
               FileSizes sizes) {
  if (file.GetSize() != sizes.file) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("index file '", name, "' is ", file.GetSize(),
              " bytes, but the log recorded it as ", sizes.file, " bytes"));
  }
}

Footer ReadFooterImpl(const duckdb::InputFile& file, std::string_view name,
                      FileSizes sizes, const FooterReader* read) {
  CheckSize(file, name, sizes);
  const auto length = file.GetSize();
  const uint64_t footer_len = sizes.footer;
  if (length < kFooterCrcLen || footer_len > length - kFooterCrcLen) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("footer: '", name, "' of ", length,
              " bytes cannot hold a footer of ", footer_len, " bytes"));
  }
  const uint64_t footer_and_crc = footer_len + kFooterCrcLen;
  Footer footer{.data_len = length - footer_and_crc};
  if (footer.data_len / duckdb::PageResidency::PAGE_SIZE !=
      (length - 1) / duckdb::PageResidency::PAGE_SIZE) {
    file.Prefetch(footer.data_len, footer_and_crc);
  }
  const auto footer_bytes = file.Pin(footer.data_len, footer_and_crc);
  const auto* footer_data = footer_bytes.Ptr();
  if (static_cast<uint32_t>(absl::ComputeCrc32c(
        {reinterpret_cast<const char*>(footer_data), footer_len})) !=
      absl::little_endian::Load32(footer_data + footer_len)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("footer: '", name, "' does not match its checksum"));
  }
  duckdb::InputFileReader stream{std::span{footer_data, footer_len}};
  duckdb::BinaryDeserializer deserializer{stream};
  try {
    deserializer.Begin();
    footer.data_expected_crc32c =
      deserializer.ReadPropertyWithExplicitDefault<uint32_t>(kFieldDataCrc32c,
                                                             "data_crc32c", 0);
    if (read) {
      deserializer.ReadObject(kFieldMeta, "meta",
                              [&](duckdb::BinaryDeserializer& meta) {
                                ReadMeta(meta, stream, *read, footer.data_len);
                              });
    }
    deserializer.End();
  } catch (const duckdb::SerializationException& e) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("footer: '", name, "' cannot be read: ",
                            duckdb::ErrorData{e}.RawMessage(),
                            "; it was written by a newer release of SereneDB"));
  }
  file.Evict(footer.data_len, footer_and_crc);
  return footer;
}

}  // namespace

uint32_t WriteFooter(duckdb::BufferedFileWriter& out) {
  return WriteFooterImpl(out, nullptr);
}

uint32_t WriteFooter(duckdb::BufferedFileWriter& out, FooterWriter write) {
  return WriteFooterImpl(out, &write);
}

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name,
                  FileSizes sizes) {
  return ReadFooterImpl(file, name, sizes, nullptr);
}

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name,
                  FileSizes sizes, FooterReader read) {
  return ReadFooterImpl(file, name, sizes, &read);
}

duckdb::shared_ptr<duckdb::InputFile> EmptyInput(
  duckdb::DatabaseInstance& db, std::string path,
  const duckdb::InputFile* sibling) {
  if (sibling && !sibling->IsMapped()) {
    return duckdb::ExternalFileCache::Get(db).CreateUnbackedFile(path);
  }
  return duckdb::make_shared_ptr<duckdb::InputFile>(
    duckdb::make_uniq<EmptyMapping>(std::move(path)));
}

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(duckdb::FileSystem& fs,
                                                       std::string_view root,
                                                       std::string_view name) {
  auto path = fs.JoinPath(root, name);
  auto writer = fs.OpenFileWriter(path);
  return std::make_unique<duckdb::BufferedFileWriter>(fs, std::move(path),
                                                      std::move(writer));
}

std::unique_ptr<duckdb::BufferedFileWriter> PrepareOutput(FileTracker& tracker,
                                                          std::string_view ext,
                                                          uint8_t kind) {
  auto out =
    OpenOutput(tracker.Fs(), tracker.Root(), FileName(tracker.Name(), ext));
  tracker.Add(kind);
  return out;
}

duckdb::shared_ptr<duckdb::InputFile> CloseOutput(
  duckdb::BufferedFileWriter& out) {
  out.Close();
  return out.fs.OpenInputFile(out.path, duckdb::MappedFileAccess::RANDOM);
}

duckdb::shared_ptr<duckdb::InputFile> CloseOutput(
  duckdb::BufferedFileWriter& out, FileTracker& tracker, uint8_t kind,
  uint32_t footer) {
  tracker.Closed(kind, {.file = out.Position(), .footer = footer});
  return CloseOutput(out);
}

void LazyOutput::Open() {
  SDB_ASSERT(_tracker);
  _out = PrepareOutput(*_tracker, _ext, _kind);
}

duckdb::shared_ptr<duckdb::InputFile> LazyOutput::Close(uint32_t footer) {
  SDB_ASSERT(_out);
  auto file = CloseOutput(*_out, *_tracker, _kind, footer);
  _out.reset();
  return file;
}

}  // namespace irs::format_utils
