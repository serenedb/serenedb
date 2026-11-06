////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "file_utils.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/query_context.hpp>
#include <exception>
#include <filesystem>
#include <iresearch/utils/log.hpp>
#include <utility>

namespace sdb::utils::file_utils {

void WriteFile(duckdb::FileSystem& fs, const std::string& path,
               std::string_view content, bool sync) {
  fs.TryRemoveFile(path);
  const auto handle =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
                        duckdb::FileFlags::FILE_FLAGS_PRIVATE);
  handle->Write(duckdb::QueryContext{}, const_cast<char*>(content.data()),
                content.size(), 0);
  if (sync) {
    handle->Sync();
  }
  handle->Close();
}

void ReplaceFile(duckdb::FileSystem& fs, const std::string& path,
                 std::string_view content) {
  const auto temp = absl::StrCat(path, ".tmp");
  WriteFile(fs, temp, content, true);
  absl::Cleanup drop_temp = [&]() noexcept {
    try {
      fs.TryRemoveFile(temp);
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "could not remove '", temp,
               "': ", duckdb::ErrorData{e}.RawMessage());
    }
  };
  fs.MoveFile(temp, path);
  std::move(drop_temp).Cancel();
  fs.SyncDirectory(std::filesystem::path{path}.parent_path().string());
}

}  // namespace sdb::utils::file_utils
