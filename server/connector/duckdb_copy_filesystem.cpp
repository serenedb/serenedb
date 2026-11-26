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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/formats/row_stream.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>

#include "pg/connection_context.h"
#include "pg/copy_in_batches.h"
#include "pg/protocol.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kDevStdin = "/dev/stdin";
constexpr std::string_view kDevStdout = "/dev/stdout";

struct ConnectionPlumbing {
  pg::CopyInBatches* batches;
  duckdb::shared_ptr<duckdb::RowSource> stream;
  message::Buffer* send_buffer;
  ConnectionContext& state;
};

ConnectionPlumbing GetPlumbing(duckdb::FileOpener* opener,
                               std::string_view path) {
  auto client_context = duckdb::FileOpener::TryGetClientContext(opener);
  if (!client_context) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY ", path,
              " is only valid on a server-side PostgreSQL wire connection"));
  }
  auto& conn = ConnectionContext::Get(*client_context);
  return {conn.GetSideChannel<pg::CopyInBatches>(),
          duckdb::FindRowStream(*client_context, std::string{path}),
          conn.GetSendBuffer(), conn};
}

}  // namespace

// Reads the COPY FROM STDIN wire stream in order: through the session's
// registered row stream when the format's reader scans it directly (csv,
// json), else SINGLE-PASS from the batches, where a re-opened handle refuses
// to read -- a non-seekable stream has nothing to replay. The session has
// already sent CopyInResponse by the time the handle exists.
struct CopyInFileHandle final : public duckdb::FileHandle {
  CopyInFileHandle(duckdb::FileSystem& fs, ConnectionContext& state,
                   pg::CopyInBatches& batches,
                   duckdb::shared_ptr<duckdb::RowSource> stream)
    : duckdb::FileHandle(
        fs, std::string{kDevStdin},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_READ)),
      _state{state},
      _batches{batches},
      _stream{std::move(stream)},
      _reopened{_state.copy_stdin_open_count++ != 0} {}

  ~CopyInFileHandle() final = default;
  void Close() final {}

  int64_t DoRead(void* buf, int64_t nr_bytes) {
    if (_stream) {
      const auto read =
        _stream->Read(static_cast<char*>(buf),
                      static_cast<duckdb::idx_t>(nr_bytes), _position);
      _position += read;
      return static_cast<int64_t>(read);
    }
    if (_reopened) {
      // A re-open would need to replay from offset 0, but the wire stream is
      // non-seekable and already drained past that point, so fail loudly
      // rather than feed the reader bytes from the wrong offset.
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(
          "COPY FROM STDIN: unexpected re-open of a non-seekable stream"));
    }
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
  duckdb::shared_ptr<duckdb::RowSource> _stream;
  bool _reopened;
  duckdb::idx_t _position = 0;
  pg::CopyBatchLease _batch;
  size_t _segment = 0;
  size_t _offset = 0;
};

// A dumb byte sink: each Write becomes one CopyData frame, nothing more. The
// pg-wire session owns the stream boundaries -- it emits CopyOutResponse (with
// the real column count) before the COPY runs and CopyDone only after it
// succeeds -- so a mid-COPY error can no longer leak a spurious CopyDone from a
// destructor, and the response is no longer stuck with column count 0. This is
// format-agnostic: csv/json/parquet/... all just stream bytes through here.
struct CopyOutFileHandle final : public duckdb::FileHandle {
  CopyOutFileHandle(duckdb::FileSystem& fs, message::Buffer& send_buffer)
    : duckdb::FileHandle(
        fs, std::string{kDevStdout},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_WRITE)),
      _send_buffer{send_buffer} {}

  void Close() final {}

  void DoWrite(const void* buf, int64_t nr_bytes) {
    message::Writer writer{_send_buffer};
    auto* prefix = writer.Alloc(5);
    writer.Write(
      {static_cast<const char*>(buf), static_cast<size_t>(nr_bytes)});
    prefix[0] = PQ_MSG_COPY_DATA;
    absl::big_endian::Store32(prefix + 1,
                              static_cast<int32_t>(writer.Written() - 1));
    writer.Commit(false);
  }

 private:
  message::Buffer& _send_buffer;
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
    auto plumbing = GetPlumbing(opener.get(), path);
    if (!plumbing.batches) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ", path,
                " requires a PG wire connection (transport not attached)"));
    }
    return duckdb::make_uniq<CopyInFileHandle>(
      *this, plumbing.state, *plumbing.batches, std::move(plumbing.stream));
  }
  if (path == kDevStdout) {
    auto plumbing = GetPlumbing(opener.get(), path);
    if (!plumbing.send_buffer) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ", path,
                " requires a PG wire connection (transport not attached)"));
    }
    return duckdb::make_uniq<CopyOutFileHandle>(*this, *plumbing.send_buffer);
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

}  // namespace sdb::connector
