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
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <limits>

#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::format_utils {
namespace {

constexpr duckdb::field_id_t kFieldDataCrc32c = 0;
constexpr duckdb::field_id_t kFieldMeta = 1;

void WritePackedMeta(duckdb::BinarySerializer& meta, duckdb::WriteStream& out,
                     FooterWriter write, TrackedAllocator<> alloc) {
  auto allocator = DuckAllocator(alloc);
  duckdb::MemoryStream raw{allocator};
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

void WriteFooterImpl(duckdb::BufferedFileWriter& out, const FooterWriter* meta) {
  const uint32_t data_crc32c = out.Checksum();
  const auto begin = out.Position();
  duckdb::BinarySerializer serializer{out};
  serializer.Begin();
  serializer.WritePropertyWithDefault<uint32_t>(kFieldDataCrc32c, "data_crc32c",
                                                data_crc32c, 0);
  if (meta) {
    serializer.WriteObject(kFieldMeta, "meta", *meta);
  }
  serializer.End();
  const auto size = out.Position() - begin;
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(), "footer of ", size,
             " bytes does not fit its 32-bit length");
  out.Write<uint32_t>(out.Checksum());
  out.Write<uint32_t>(static_cast<uint32_t>(size));
}

Footer ReadFooterImpl(const duckdb::InputFile& file, std::string_view name,
                      const FooterReader* read) {
  const auto length = file.GetSize();
  if (length < kTrailerLen) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("footer: '", name, "' of ", length,
                            " bytes is too short for a trailer"));
  }
  const uint64_t trailer_offset = length - kTrailerLen;
  const auto trailer = file.Pin(trailer_offset, kTrailerLen);
  const auto footer_expected_crc32c =
    absl::little_endian::Load32(trailer.Ptr());
  const auto footer_len =
    absl::little_endian::Load32(trailer.Ptr() + sizeof(uint32_t));
  if (footer_len > trailer_offset) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("footer: '", name, "' of ", length, " bytes claims a footer of ",
              footer_len, " bytes"));
  }
  Footer footer{.data_len = trailer_offset - footer_len};
  const bool in_trailer_page =
    footer.data_len / duckdb::PageResidency::PAGE_SIZE ==
    trailer_offset / duckdb::PageResidency::PAGE_SIZE;
  if (!in_trailer_page) {
    file.Prefetch(footer.data_len, footer_len);
  }
  const auto footer_bytes = file.Pin(footer.data_len, footer_len);
  const auto* footer_data = footer_bytes.Ptr();
  if (static_cast<uint32_t>(absl::ComputeCrc32c(
        {reinterpret_cast<const char*>(footer_data), footer_len})) !=
      footer_expected_crc32c) {
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
  file.Evict(footer.data_len, footer_len);
  return footer;
}

}  // namespace

void WriteFooter(duckdb::BufferedFileWriter& out) {
  WriteFooterImpl(out, nullptr);
}

void WriteFooter(duckdb::BufferedFileWriter& out, TrackedAllocator<> alloc,
                 FooterWriter write) {
  const auto pack = [&](duckdb::BinarySerializer& meta) {
    SDB_IF_FAILURE("irs::FooterMetaUnpacked") {
      write(meta);
      return;
    }
    WritePackedMeta(meta, out, write, alloc);
  };
  const FooterWriter meta{pack};
  WriteFooterImpl(out, &meta);
}

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name) {
  return ReadFooterImpl(file, name, nullptr);
}

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name,
                  FooterReader read) {
  return ReadFooterImpl(file, name, &read);
}

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(duckdb::FileSystem& fs,
                                                       std::string_view root,
                                                       std::string_view name,
                                                       TrackedAllocator<> alloc) {
  auto path = fs.JoinPath(root, name);
  auto writer = fs.OpenFileWriter(path);
  writer->FundFrom(alloc.Budget().Lend(duckdb::MemoryTag::EXTERNAL_FILE_CACHE,
                                       kOutputBlockBytes));
  return std::make_unique<duckdb::BufferedFileWriter>(fs, std::move(path),
                                                      std::move(writer));
}

std::unique_ptr<duckdb::BufferedFileWriter> PrepareOutput(
  const FlushState& state, std::string_view ext, uint8_t kind) {
  auto& tracker = state.tracker;
  auto out = OpenOutput(tracker.Fs(), tracker.Root(),
                        FileName(tracker.Name(), ext), state.alloc);
  tracker.Add(kind);
  return out;
}

duckdb::shared_ptr<duckdb::InputFile> CloseOutput(
  duckdb::BufferedFileWriter& out) {
  out.Close();
  return out.fs.OpenInputFile(out.path, duckdb::MappedFileAccess::RANDOM);
}

}  // namespace irs::format_utils
