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

#include <absl/functional/function_ref.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/main/database.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "iresearch/store/data_input.hpp"

namespace irs {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

StorageKind StorageKindOf(std::string_view location);

std::string PathOf(std::string_view root, std::string_view name);

std::unique_ptr<InputFile> OpenInput(duckdb::FileSystem& fs,
                                     const std::string& path,
                                     duckdb::MappedFileAccess access);

duckdb::unique_ptr<duckdb::FileHandle> OpenWhole(duckdb::FileSystem& fs,
                                                 const std::string& path);

template<typename Bytes>
Bytes ReadWhole(duckdb::QueryContext context, duckdb::FileSystem& fs,
                const std::string& path) {
  const auto handle = OpenWhole(fs, path);
  Bytes bytes(duckdb::NumericCast<size_t>(handle->GetFileSize()),
              typename Bytes::value_type{});
  if (!bytes.empty()) {
    handle->Read(context, bytes.data(), bytes.size(), 0);
  }
  return bytes;
}

void ForEach(duckdb::DatabaseInstance& db, std::string_view root, size_t count,
             absl::FunctionRef<void(size_t)> visit);

}  // namespace irs
