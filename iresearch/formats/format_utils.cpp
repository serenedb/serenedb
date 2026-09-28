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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <limits>

#include "iresearch/index/file_names.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/serialization.hpp"

namespace irs::format_utils {
namespace {

constexpr duckdb::field_id_t kFieldDataCrc32c = 0;
constexpr duckdb::field_id_t kFieldMeta = 1;

}  // namespace

void WriteFooter(IndexOutput& out,
                 absl::FunctionRef<void(duckdb::Serializer&)> write) {
  const uint32_t data_crc32c = out.Checksum();
  duckdb::MemoryStream footer;
  duckdb::BinarySerializer serializer{footer, duckdb::VersionStorageOptions()};
  serializer.Begin();
  serializer.WritePropertyWithDefault<uint32_t>(kFieldDataCrc32c, "data_crc32c",
                                                data_crc32c, 0);
  serializer.WriteObject(kFieldMeta, "meta", write);
  serializer.End();
  const auto size = footer.GetPosition();
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(), "footer of ", size,
             " bytes does not fit its 32-bit length");
  Crc32c crc;
  crc.process_bytes(footer.GetData(), size);
  out.WriteData(footer.GetData(), size);
  out.WriteU32(crc.checksum());
  out.WriteU32(static_cast<uint32_t>(size));
}

Footer ReadFooter(
  IndexInput& in, std::string_view name,
  absl::FunctionRef<void(duckdb::Deserializer&, uint64_t)> read) {
  const uint64_t length = in.Length();
  if (length < kTrailerLen) {
    throw IndexError{absl::StrCat("footer: '", name, "' of ", length,
                                  " bytes is too short for a trailer")};
  }
  const uint64_t trailer = length - kTrailerLen;
  in.Seek(trailer);
  const auto crc32c = static_cast<uint32_t>(in.ReadI32());
  const auto size = static_cast<uint32_t>(in.ReadI32());
  if (size > trailer) {
    throw IndexError{absl::StrCat("footer: '", name, "' of ", length,
                                  " bytes claims a footer of ", size,
                                  " bytes")};
  }
  Footer footer{.data_size = trailer - size};
  bstring owned;
  const auto* data = in.ReadStable(footer.data_size, size);
  if (data == nullptr) {
    owned.resize(size);
    in.ReadData(footer.data_size, owned.data(), size);
    data = owned.data();
  }
  Crc32c crc;
  crc.process_bytes(data, size);
  if (crc.checksum() != crc32c) {
    throw IndexError{
      absl::StrCat("footer: '", name, "' does not match its checksum")};
  }
  duckdb::MemoryStream stream{const_cast<byte_type*>(data), size};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  footer.data_crc32c = deserializer.ReadPropertyWithExplicitDefault<uint32_t>(
    kFieldDataCrc32c, "data_crc32c", 0);
  deserializer.ReadObject(kFieldMeta, "meta", [&](duckdb::Deserializer& meta) {
    read(meta, footer.data_size);
  });
  deserializer.End();
  return footer;
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
