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

#include <duckdb/common/file_system.hpp>

#include "connector/zip_reader.hpp"

namespace sdb::connector {

// Opens and reads members of local ZIP archives through zip:// paths
class ZipFileSystem final : public duckdb::FileSystem {
 public:
  std::string GetName() const final { return "ZipFileSystem"; }

  bool CanHandleFile(const std::string& path) final;

  bool CanSeek() final { return true; }

  bool OnDiskFile(duckdb::FileHandle& handle) final { return true; }

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    const std::string& path, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  int64_t Read(duckdb::FileHandle& handle, void* buffer, int64_t count) final;

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t count,
            duckdb::idx_t offset) final;

  void Seek(duckdb::FileHandle& handle, duckdb::idx_t offset) final;

  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) final;

  int64_t GetFileSize(duckdb::FileHandle& handle) final;

  duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle& handle) final;

  duckdb::FileType GetFileType(duckdb::FileHandle& handle) final;

  bool FileExists(const std::string& path,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  duckdb::vector<duckdb::OpenFileInfo> Glob(const std::string& path,
                                            duckdb::FileOpener* opener) final;
};

}  // namespace sdb::connector
