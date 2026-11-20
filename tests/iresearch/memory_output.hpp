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

#pragma once

#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <iresearch/utils/string.hpp>

namespace tests {

inline irs::bstring Bytes(const duckdb::MemoryStream& out) {
  return irs::bstring{out.GetData(), out.GetPosition()};
}

class BytesMapping final : public duckdb::MemoryMappedFile {
 public:
  explicit BytesMapping(irs::bytes_view bytes)
    : MemoryMappedFile{{},
                       duckdb::FileOpenFlags::FILE_FLAGS_READ,
                       const_cast<duckdb::data_ptr_t>(bytes.data()),
                       static_cast<duckdb::idx_t>(bytes.size())} {}

  void Sync() final {}
  bool Trim(duckdb::idx_t /*offset*/, duckdb::idx_t /*length*/) final {
    return false;
  }
  bool OnDiskFile() const final { return false; }
  void Close() final {}
};

inline duckdb::shared_ptr<duckdb::InputFile> OpenBytes(irs::bytes_view bytes) {
  return duckdb::make_shared_ptr<duckdb::InputFile>(
    duckdb::make_uniq<BytesMapping>(bytes));
}

}  // namespace tests
