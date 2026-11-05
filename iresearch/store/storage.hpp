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
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

StorageKind StorageKindOf(std::string_view location);

std::string PathOf(std::string_view root, std::string_view name);

std::unique_ptr<InputFile> OpenInput(duckdb::BlockCache& cache,
                                     duckdb::FileSystem& fs,
                                     const std::string& path,
                                     duckdb::MappedFileAccess access);

bstring ReadWhole(duckdb::FileSystem& fs, const std::string& path);

void SyncFiles(duckdb::FileSystem& fs, std::string_view root,
               std::span<const std::string_view> names);

void SyncDirectory(duckdb::FileSystem& fs, std::string_view root);

void ForEach(duckdb::DatabaseInstance& db, std::string_view root, size_t count,
             absl::FunctionRef<void(size_t)> visit);

}  // namespace irs
