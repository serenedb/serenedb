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

#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <limits>

#include "iresearch/error/error.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/serialization.hpp"

namespace irs::format_utils {
namespace {

constexpr duckdb::field_id_t kFieldDataCrc32c = 0;
constexpr duckdb::field_id_t kFieldMeta = 1;

void WriteFooterImpl(IndexOutput& out, const FooterWriter* write) {
  const uint32_t data_crc32c = out.Checksum();
  duckdb::MemoryStream footer;
  duckdb::BinarySerializer serializer{footer, duckdb::VersionStorageOptions()};
  serializer.Begin();
  serializer.WritePropertyWithDefault<uint32_t>(kFieldDataCrc32c, "data_crc32c",
                                                data_crc32c, 0);
  if (write != nullptr) {
    serializer.WriteObject(kFieldMeta, "meta", *write);
  }
  serializer.End();
  const auto size = footer.GetPosition();
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(), "footer of ", size,
             " bytes does not fit its 32-bit length");
  out.WriteData(footer.GetData(), size);
  out.WriteU32(out.Checksum());
  out.WriteU32(static_cast<uint32_t>(size));
}

Footer ReadFooterImpl(IndexInput& in, std::string_view name,
                      const FooterReader* read) {
  const uint64_t length = in.Length();
  if (length < kTrailerLen) {
    throw IndexError{absl::StrCat("footer: '", name, "' of ", length,
                                  " bytes is too short for a trailer")};
  }
  const uint64_t trailer_offset = length - kTrailerLen;
  in.Seek(trailer_offset);
  const auto footer_expected_crc32c = static_cast<uint32_t>(in.ReadI32());
  const auto footer_len = static_cast<uint32_t>(in.ReadI32());
  if (footer_len > trailer_offset) {
    throw IndexError{absl::StrCat("footer: '", name, "' of ", length,
                                  " bytes claims a footer of ", footer_len,
                                  " bytes")};
  }
  Footer footer{.data_len = trailer_offset - footer_len};
  bstring footer_buf;
  const auto* footer_data = in.ReadStable(footer.data_len, footer_len);
  if (!footer_data) {
    footer_buf.resize(footer_len);
    in.ReadData(footer.data_len, footer_buf.data(), footer_len);
    footer_data = footer_buf.data();
  }
  Crc32c footer_actual_crc32c;
  footer_actual_crc32c.process_bytes(footer_data, footer_len);
  if (footer_actual_crc32c.checksum() != footer_expected_crc32c) {
    throw IndexError{
      absl::StrCat("footer: '", name, "' does not match its checksum")};
  }
  duckdb::MemoryStream stream{const_cast<byte_type*>(footer_data), footer_len};
  duckdb::BinaryDeserializer deserializer{stream};
  try {
    deserializer.Begin();
    footer.data_expected_crc32c =
      deserializer.ReadPropertyWithExplicitDefault<uint32_t>(kFieldDataCrc32c,
                                                             "data_crc32c", 0);
    if (read != nullptr) {
      deserializer.ReadObject(kFieldMeta, "meta",
                              [&](duckdb::BinaryDeserializer& meta) {
                                (*read)(meta, footer.data_len);
                              });
    }
    deserializer.End();
  } catch (const duckdb::SerializationException& e) {
    throw IndexError{
      absl::StrCat("footer: '", name,
                   "' cannot be read: ", duckdb::ErrorData{e}.RawMessage(),
                   "; it was written by a newer release of SereneDB")};
  }
  return footer;
}

}  // namespace

void WriteFooter(IndexOutput& out) { WriteFooterImpl(out, nullptr); }

void WriteFooter(IndexOutput& out, FooterWriter write) {
  WriteFooterImpl(out, &write);
}

Footer ReadFooter(IndexInput& in, std::string_view name) {
  return ReadFooterImpl(in, name, nullptr);
}

Footer ReadFooter(IndexInput& in, std::string_view name, FooterReader read) {
  return ReadFooterImpl(in, name, &read);
}

void PrepareOutput(std::string& str, IndexOutput::ptr& out,
                   const FlushState& state, std::string_view ext) {
  SDB_ASSERT(!out);

  FileName(str, state.name, ext);
  out = state.dir->create(str);

  if (!out) {
    throw IoError{absl::StrCat("Failed to create file, path: ", str)};
  }
}

}  // namespace irs::format_utils
