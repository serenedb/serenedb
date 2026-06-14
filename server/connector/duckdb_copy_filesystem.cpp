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
  std::span data{send_buffer.GetContiguousData(kMsgSize), kMsgSize};
  data[0] = msg_type;
  absl::big_endian::Store32(data.data() + 1,
                            static_cast<int32_t>(kMsgSize - 1));
  data[5] = 0;
  absl::big_endian::Store16(data.data() + 6,
                            static_cast<int16_t>(kColumnCount));
  send_buffer.Commit(true);
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

// DuckDB may open /dev/stdin more than once per query (CSV sniff then
// real read); the shared copy_stdin_buffer lets later opens replay the
// prefix that the earlier open already drained from the wire.
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
    if (open_idx == 0) {
      if (queue) {
        queue->StartListening();
        SendCopyResponse(send_buffer, PQ_MSG_COPY_IN_RESPONSE);
      }
      // The replay buffer only bridges a re-open (the CSV sniff). Binary COPY
      // opens once, so skipping it avoids duplicating the whole input; DoRead
      // then streams straight from the bridge (_buffer stays null).
      if (!_state.copy_stdin_no_replay) {
        _state.copy_stdin_buffer = std::make_shared<std::string>();
      }
    }
    _buffer = _state.copy_stdin_buffer;
    if (queue) {
      _iterator.emplace(*queue);
    }
  }

  ~CopyInFileHandle() final = default;
  void Close() final {}

  int64_t DoRead(void* buf, int64_t nr_bytes) {
    auto* out = static_cast<char*>(buf);
    int64_t total = 0;

    if (_buffer && _buffer_offset < _buffer->size()) {
      auto avail = std::min(
        static_cast<int64_t>(_buffer->size() - _buffer_offset), nr_bytes);
      std::memcpy(out, _buffer->data() + _buffer_offset, avail);
      _buffer_offset += avail;
      out += avail;
      total += avail;
      nr_bytes -= avail;
      if (nr_bytes == 0) {
        return total;
      }
    }

    // Each handle replays the shared buffer first; once one handle has seen
    // EOF (CopyDone) the others must short-circuit instead of re-blocking.
    if (_state.copy_stdin_done) {
      return total;
    }

    auto read = _bridge ? _bridge->Read(out, nr_bytes)
                        : static_cast<int64_t>(_iterator->Next(
                            out, static_cast<uint64_t>(nr_bytes)));
    total += read;
    if (_buffer && read > 0) {
      _buffer->append(out, static_cast<size_t>(read));
      // This handle is the producer for these bytes; advance past them so a
      // later DoRead on it doesn't replay its own fresh reads (only a *new*
      // handle, with offset 0, should replay the buffer for the sniff re-open).
      _buffer_offset = _buffer->size();
    }
    if (read == 0) {
      _state.copy_stdin_done = true;
    }
    return total;
  }

 private:
  SereneDBClientState& _state;
  pg::CopyInBridge* _bridge;
  std::optional<pg::CopyMessagesQueueIterator> _iterator;
  std::shared_ptr<std::string> _buffer;
  size_t _buffer_offset = 0;
};

// Wire protocol: CopyOutResponse (once, on open) -> N x CopyData (each
// Write) -> CopyDone (on Close).
struct CopyOutFileHandle final : public duckdb::FileHandle {
  CopyOutFileHandle(duckdb::FileSystem& fs, message::Buffer& send_buffer)
    : duckdb::FileHandle(
        fs, std::string{kDevStdout},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_WRITE)),
      _send_buffer{send_buffer} {
    SendCopyResponse(_send_buffer, PQ_MSG_COPY_OUT_RESPONSE);
  }

  // DuckDB's copy-to operator destroys the handle at end of execution without
  // an explicit Close(), so emit the terminating CopyDone here. Idempotent via
  // _closed, so an explicit Close() followed by destruction still sends it once.
  ~CopyOutFileHandle() final { Close(); }

  void Close() final {
    if (_closed) {
      return;
    }
    _closed = true;
    static constexpr uint8_t kCopyDone[5] = {PQ_MSG_COPY_DONE, 0, 0, 0, 4};
    _send_buffer.Write({reinterpret_cast<const char*>(kCopyDone), 5}, true);
  }

  void DoWrite(const void* buf, int64_t nr_bytes) {
    auto before = _send_buffer.GetUncommittedSize();
    auto* prefix = _send_buffer.GetContiguousData(5);
    _send_buffer.WriteUncommitted(
      {static_cast<const char*>(buf), static_cast<size_t>(nr_bytes)});
    prefix[0] = PQ_MSG_COPY_DATA;
    absl::big_endian::Store32(
      prefix + 1,
      static_cast<int32_t>(_send_buffer.GetUncommittedSize() - before - 1));
    _send_buffer.Commit(false);
  }

 private:
  message::Buffer& _send_buffer;
  bool _closed = false;
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
  return -1;  // unknown (streaming)
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
