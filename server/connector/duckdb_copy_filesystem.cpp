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

#include <cstdint>
#include <cstring>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/main/client_context.hpp>
#include <optional>
#include <span>

#include "basics/assert.h"
#include "basics/message_buffer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/copy_messages_queue.h"
#include "pg/errcodes.h"
#include "pg/protocol.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kDevStdin = "/dev/stdin";
constexpr std::string_view kDevStdout = "/dev/stdout";

// Column count is 0: we don't know it at OpenFile time, and for FORMAT
// TEXT the field is informational -- clients assume text per column.
void SendCopyResponse(message::Buffer& send_buffer, uint8_t msg_type) {
  constexpr size_t kColumnCount = 0;
  constexpr size_t kMsgSize = sizeof(uint8_t) + sizeof(int32_t) +
                              sizeof(uint8_t) + sizeof(int16_t) +
                              (kColumnCount * sizeof(int16_t));
  message::Writer writer{send_buffer};
  std::span data{writer.Alloc(kMsgSize), kMsgSize};
  data[0] = msg_type;
  absl::big_endian::Store32(data.data() + 1,
                            static_cast<int32_t>(kMsgSize - 1));
  data[5] = 0;
  absl::big_endian::Store16(data.data() + 6,
                            static_cast<int16_t>(kColumnCount));
  writer.Commit(true);
}

struct ConnectionPlumbing {
  pg::CopyMessagesQueue* queue;
  pg::CopyInBridge* bridge;
  message::Buffer* send_buffer;
  SereneDBClientState& state;
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
  auto* state = client_context->registered_state
                  ->Get<SereneDBClientState>(kSereneDBClientStateKey)
                  .get();
  if (!state) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("COPY ", path,
              " requires SereneDB client state (not registered)"));
  }
  auto& conn = state->GetConnectionContext();
  auto* send = conn.GetSendBuffer();
  if (!send) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY ", path,
              " requires a PG wire connection (transport not attached)"));
  }
  return {conn.GetCopyQueue(), conn.GetCopyInBridge(), send, *state};
}

}  // namespace

// Reads the non-seekable COPY FROM STDIN wire stream SINGLE-PASS, off the
// bridge (new server) or the message queue (old server). Every COPY format
// opens it exactly once (csv/json sniff+scan share one buffer-manager pass), so
// the ctor rejects an unexpected re-open -- a non-seekable stream has nothing
// to replay.
struct CopyInFileHandle final : public duckdb::FileHandle {
  // `bridge` (new coroutine server) and `queue` (old server) are mutually
  // exclusive: exactly one is non-null. With the bridge, the session has
  // already sent CopyInResponse, so the handle doesn't.
  CopyInFileHandle(duckdb::FileSystem& fs, SereneDBClientState& state,
                   pg::CopyMessagesQueue* queue, pg::CopyInBridge* bridge,
                   message::Buffer& send_buffer)
    : duckdb::FileHandle(
        fs, std::string{kDevStdin},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_READ)),
      _state{state},
      _bridge{bridge} {
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
    if (queue) {
      queue->StartListening();
      SendCopyResponse(send_buffer, PQ_MSG_COPY_IN_RESPONSE);
      _iterator.emplace(*queue);
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
    const auto read = _bridge ? _bridge->Read(out, nr_bytes)
                              : static_cast<int64_t>(_iterator->Next(
                                  out, static_cast<uint64_t>(nr_bytes)));
    if (read == 0) {
      _state.copy_stdin_done = true;
    }
    return read;
  }

 private:
  SereneDBClientState& _state;
  pg::CopyInBridge* _bridge;
  std::optional<pg::CopyMessagesQueueIterator> _iterator;
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
    if (!plumbing.queue && !plumbing.bridge) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COPY ", path,
                " requires a PG wire connection (transport not attached)"));
    }
    return duckdb::make_uniq<CopyInFileHandle>(*this, plumbing.state,
                                               plumbing.queue, plumbing.bridge,
                                               *plumbing.send_buffer);
  }
  if (path == kDevStdout) {
    auto plumbing = GetPlumbing(opener.get(), path);
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

bool SereneDBCopyFileSystem::FileExists(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return filename == kDevStdin || filename == kDevStdout;
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
