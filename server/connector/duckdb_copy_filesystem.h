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

// FileSystem that intercepts "/dev/stdin" and reads from the PG COPY protocol
// queue instead of the OS stdin. Registered as a sub-filesystem with DuckDB's
// VirtualFileSystem.
//
// When psql sends "COPY FROM STDIN", libpgquery transforms STDIN to
// "/dev/stdin". This filesystem handles that path and bridges PG CopyData
// messages into DuckDB's file I/O interface.
// Send PG CopyInResponse message to tell the client to start sending data.
void SendCopyInResponse(message::Buffer& send_buffer, size_t column_count);

class SereneDBCopyFileSystem final : public duckdb::FileSystem {
 public:
  std::string GetName() const final { return "SereneDBCopyFileSystem"; }

  bool CanHandleFile(const std::string& fpath) final;
  bool IsPipe(const std::string& filename,
              duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    const std::string& path, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;

  // Sequential read -- reads from CopyMessagesQueue
  int64_t Read(duckdb::FileHandle& handle, void* buffer,
               int64_t nr_bytes) final;
  // Positional read -- not supported for streaming
  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
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
