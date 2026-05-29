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

namespace sdb::message {

class Buffer;
}

namespace sdb::pg {

class CopyMessagesQueue;
}

namespace sdb::connector {

// Bridges the PostgreSQL COPY wire protocol into DuckDB's FileSystem.
// Intercepts "/dev/stdin" (read) and "/dev/stdout" (write); the SQL
// identifiers STDIN and STDOUT are mapped to these paths by the PEG
// transformer. Registered only on the server-side DuckDB instance, so
// shell/psql mode falls through to the OS filesystem.
class SereneDBCopyFileSystem final : public duckdb::FileSystem {
 public:
  std::string GetName() const final { return "SereneDBCopyFileSystem"; }

  bool CanHandleFile(const std::string& fpath) final;
  bool IsPipe(const std::string& filename,
              duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    const std::string& path, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  int64_t Read(duckdb::FileHandle& handle, void* buffer,
               int64_t nr_bytes) final;
  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            duckdb::idx_t location) final;

  int64_t Write(duckdb::FileHandle& handle, void* buffer,
                int64_t nr_bytes) final;
  void Write(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
             duckdb::idx_t location) final;

  bool FileExists(const std::string& filename,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) final;
  duckdb::vector<duckdb::OpenFileInfo> Glob(const std::string& path,
                                            duckdb::FileOpener* opener) final;

  int64_t GetFileSize(duckdb::FileHandle& handle) final;
  bool CanSeek() final { return false; }
  void Seek(duckdb::FileHandle& handle, duckdb::idx_t location) final;
  void Reset(duckdb::FileHandle& handle) final;
  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) final;
  duckdb::FileType GetFileType(duckdb::FileHandle& handle) final;
};

}  // namespace sdb::connector
