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

#include "connector/duckdb_copy_filesystem.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>

#include "network/send_gate.h"
#include "pg/connection_context.h"
#include "pg/copy_in_batches.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kDevStdin = "/dev/stdin";

ConnectionContext& GetConnection(duckdb::FileOpener* opener,
                                 std::string_view path) {
  auto client_context = duckdb::FileOpener::TryGetClientContext(opener);
  if (!client_context) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY ", path,
              " is only valid on a server-side PostgreSQL wire connection"));
  }
  return ConnectionContext::Get(*client_context);
}

}  // namespace

// Reads the non-seekable COPY FROM STDIN wire stream SINGLE-PASS. Every COPY
// format opens it exactly once (csv/json sniff+scan share one buffer-manager
// pass), so the ctor rejects an unexpected re-open -- a non-seekable stream has
// nothing to replay. The session has already sent CopyInResponse by the time
// the handle exists.
struct CopyInFileHandle final : public duckdb::FileHandle {
  CopyInFileHandle(duckdb::FileSystem& fs, ConnectionContext& state,
                   pg::CopyInBatches& batches)
    : duckdb::FileHandle(
        fs, std::string{kDevStdin},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_READ)),
      _state{state},
      _batches{batches} {
    const auto open_idx = _state.copy_stdin_open_count++;
    if (open_idx != 0) {
      // A re-open would need to replay from offset 0, but the wire stream is
      // non-seekable and already drained past that point. No COPY format does
      // this (csv/json sniff+scan share a single buffer-manager pass), so fail
      // loudly rather than feed the reader bytes from the wrong offset.
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(
          "COPY FROM STDIN: unexpected re-open of a non-seekable stream"));
    }
  }

  ~CopyInFileHandle() final = default;
  void Close() final {}

  int64_t DoRead(void* buf, int64_t nr_bytes) {
    // Short-circuit once CopyDone/EOF has been seen instead of re-blocking.
    if (_state.copy_stdin_done) {
      return 0;
    }
    auto* out = static_cast<char*>(buf);
    int64_t read = 0;
    while (read < nr_bytes) {
      if (!_batch) {
        _batch = _batches.Next();
        if (!_batch) {
          break;
        }
        _segment = 0;
        _offset = 0;
      }
      const auto segment = _batch->segments[_segment].substr(_offset);
      const auto take =
        std::min<size_t>(segment.size(), static_cast<size_t>(nr_bytes - read));
      std::memcpy(out + read, segment.data(), take);
      read += static_cast<int64_t>(take);
      _offset += take;
      if (_offset == _batch->segments[_segment].size()) {
        _offset = 0;
        if (++_segment == _batch->segments.size()) {
          _batch.Reset();
        }
      }
    }
    if (read == 0) {
      _state.copy_stdin_done = true;
    }
    return read;
  }

 private:
  ConnectionContext& _state;
  pg::CopyInBatches& _batches;
  pg::CopyBatchLease _batch;
  size_t _segment = 0;
  size_t _offset = 0;
};

struct CopyOutFileHandle final : public duckdb::FileHandle {
  CopyOutFileHandle(duckdb::FileSystem& fs, CopyOutput& output,
                    network::SendGate& gate)
    : duckdb::FileHandle(
        fs, std::string{kDevStdout},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_WRITE)),
      _output{output},
      _gate{gate} {}

  void Close() final {}

  void DoWrite(const void* buf, int64_t nr_bytes) {
    const auto size = static_cast<size_t>(nr_bytes);
    _largest_write = std::max(_largest_write, size);
    _gate.Wrote(_output.Write({static_cast<const char*>(buf), size}),
                _largest_write);
  }

 private:
  CopyOutput& _output;
  network::SendGate& _gate;
  size_t _largest_write = 0;
};

bool SereneDBCopyFileSystem::CanHandleFile(const std::string& fpath) {
  return fpath == kDevStdin || fpath == kDevStdout;
}

bool SereneDBCopyFileSystem::IsPipe(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return filename == kDevStdin || filename == kDevStdout;
}

duckdb::unique_ptr<duckdb::FileHandle> SereneDBCopyFileSystem::OpenFile(
  const std::string& path, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  if (path == kDevStdin) {
    auto& connection = GetConnection(opener.get(), path);
    auto* batches = connection.GetSideChannel<pg::CopyInBatches>();
    if (!batches) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ", path,
                " requires a PG wire connection (transport not attached)"));
    }
    return duckdb::make_uniq<CopyInFileHandle>(*this, connection, *batches);
  }
  if (path == kDevStdout) {
    auto& connection = GetConnection(opener.get(), path);
    auto* output = connection.GetSideChannel<CopyOutput>();
    auto* gate = connection.GetSendGate();
    if (output && gate) {
      return duckdb::make_uniq<CopyOutFileHandle>(*this, *output, *gate);
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY ", path,
              " requires a PG wire connection (transport not attached)"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INTERNAL_ERROR),
    ERR_MSG("SereneDBCopyFileSystem cannot open path '", path, "'"));
}

int64_t SereneDBCopyFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                                     int64_t nr_bytes) {
  return handle.Cast<CopyInFileHandle>().DoRead(buffer, nr_bytes);
}

void SereneDBCopyFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                                  int64_t nr_bytes, duckdb::idx_t location) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Positional read not supported for COPY FROM STDIN"));
}

int64_t SereneDBCopyFileSystem::Write(duckdb::FileHandle& handle, void* buffer,
                                      int64_t nr_bytes) {
  handle.Cast<CopyOutFileHandle>().DoWrite(buffer, nr_bytes);
  return nr_bytes;
}

void SereneDBCopyFileSystem::Write(duckdb::FileHandle& handle, void* buffer,
                                   int64_t nr_bytes, duckdb::idx_t location) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Positional write not supported for COPY TO STDOUT"));
}

duckdb::FileExistence SereneDBCopyFileSystem::TryFileExists(
  const std::string& filename, duckdb::optional_ptr<duckdb::FileOpener> opener,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  return filename == kDevStdin || filename == kDevStdout
           ? duckdb::FileExistence::EXISTS
           : duckdb::FileExistence::MISSING;
}

duckdb::vector<duckdb::OpenFileInfo> SereneDBCopyFileSystem::Glob(
  const std::string& path, duckdb::FileOpener* opener) {
  if (path == kDevStdin) {
    return {{std::string{kDevStdin}}};
  }
  if (path == kDevStdout) {
    return {{std::string{kDevStdout}}};
  }
  return {};
}

int64_t SereneDBCopyFileSystem::GetFileSize(duckdb::FileHandle& handle) {
  // Mirror PipeFileSystem: a non-seekable stream reports 0, not -1 -- size is
  // read as unsigned idx_t downstream, where -1 becomes idx_t(-1) and blows up
  // MaxThreads (file_size/bytes_per_thread) into a huge over-spawn.
  return 0;
}

void SereneDBCopyFileSystem::Seek(duckdb::FileHandle& handle,
                                  duckdb::idx_t location) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Seek not supported for COPY STDIN/STDOUT"));
}

void SereneDBCopyFileSystem::Reset(duckdb::FileHandle& handle) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Reset not supported for COPY STDIN/STDOUT"));
}

duckdb::idx_t SereneDBCopyFileSystem::SeekPosition(duckdb::FileHandle& handle) {
  return 0;
}

duckdb::FileType SereneDBCopyFileSystem::GetFileType(
  duckdb::FileHandle& handle) {
  return duckdb::FileType::FILE_TYPE_FIFO;
}

void DrainToHandle(message::Buffer& buffer, duckdb::FileHandle& handle) {
  auto chain = buffer.ReleaseChain();
  for (auto* chunk = chain.head; chunk; chunk = chunk->Next()) {
    const auto data = chunk->Data(chunk->GetEnd());
    if (!data.empty()) {
      handle.Write(const_cast<uint8_t*>(data.data()),
                   static_cast<duckdb::idx_t>(data.size()));
    }
  }
}

}  // namespace sdb::connector
