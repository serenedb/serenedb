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

#include <duckdb/common/file_opener.hpp>
#include <duckdb/main/client_context.hpp>
#include <iostream>

#include "basics/assert.h"
#include "basics/message_buffer.h"
#include "connector/duckdb_client_state.h"
#include "pg/copy_messages_queue.h"
#include "pg/protocol.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kDevStdin = "/dev/stdin";

}  // namespace

// Sends CopyInResponse to the PG client.
void SendCopyInResponse(message::Buffer& send_buffer, size_t column_count) {
  const size_t msg_size = sizeof(uint8_t) + sizeof(int32_t) + sizeof(uint8_t) +
                          sizeof(int16_t) + (column_count * sizeof(int16_t));
  auto* data = send_buffer.GetContiguousData(msg_size);
  data[0] = PQ_MSG_COPY_IN_RESPONSE;
  absl::big_endian::Store32(data + 1, static_cast<int32_t>(msg_size - 1));
  data[5] = 0;  // text format
  absl::big_endian::Store16(data + 6, static_cast<int16_t>(column_count));
  for (size_t i = 0; i < column_count; ++i) {
    absl::big_endian::Store16(data + 8 + i * 2, 0);
  }
  send_buffer.Commit(true);
}

// FileHandle that reads from the PG CopyMessagesQueue.
//
// DuckDB opens /dev/stdin twice -- once for CSV sniffing (during Prepare),
// once for actual data reading (during Execute). The first open sends
// CopyInResponse, reads from the queue, and buffers everything.
// The second open replays from the buffer then continues from the queue.
struct CopyQueueFileHandle final : public duckdb::FileHandle {
  CopyQueueFileHandle(duckdb::FileSystem& fs, SereneDBClientState& client_state)
    : duckdb::FileHandle(
        fs, std::string{kDevStdin},
        duckdb::FileOpenFlags(duckdb::FileOpenFlags::FILE_FLAGS_READ)),
      state(client_state),
      iterator(*client_state.copy_queue) {
    auto open_idx = state.copy_stdin_open_count++;
    if (open_idx == 0) {
      // Very first open: CopyInResponse + StartListening already done by PG
      // layer.
      state.copy_stdin_buffer = std::make_shared<std::string>();
    }
    buffer = state.copy_stdin_buffer;
    is_buffering = true;  // all opens buffer reads for later replays
  }

  ~CopyQueueFileHandle() final = default;
  void Close() final {}

  int64_t DoRead(void* buf, int64_t nr_bytes) {
    char* out = static_cast<char*>(buf);
    int64_t total = 0;

    // First serve from replay buffer
    if (buffer && buffer_offset < buffer->size()) {
      auto avail = std::min(
        static_cast<int64_t>(buffer->size() - buffer_offset), nr_bytes);
      std::memcpy(out, buffer->data() + buffer_offset, avail);
      buffer_offset += avail;
      out += avail;
      total += avail;
      nr_bytes -= avail;
      if (nr_bytes == 0) {
        return total;
      }
    }

    // Then read from queue
    auto read = iterator.Next(out, static_cast<uint64_t>(nr_bytes));
    total += static_cast<int64_t>(read);

    // Buffer what we read so later opens can replay
    if (is_buffering && buffer && read > 0) {
      buffer->append(out, read);
    }

    return total;
  }

  SereneDBClientState& state;
  pg::CopyMessagesQueueIterator iterator;
  bool is_buffering = false;
  std::shared_ptr<std::string> buffer;
  size_t buffer_offset = 0;
};

bool SereneDBCopyFileSystem::CanHandleFile(const std::string& fpath) {
  return fpath == kDevStdin;
}

bool SereneDBCopyFileSystem::IsPipe(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return filename == kDevStdin;
}

duckdb::unique_ptr<duckdb::FileHandle> SereneDBCopyFileSystem::OpenFile(
  const std::string& path, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  // Get the CopyMessagesQueue from the connection's client state.
  // FileOpener gives us ClientContext, which has SereneDBClientState,
  // which holds the queue pointer set by the PG layer.
  auto client_context = duckdb::FileOpener::TryGetClientContext(opener);

  // Print backtrace-style info to see who is opening the file
  std::cerr << ">>> CopyFS::OpenFile path=" << path << " open#="
            << (client_context ? std::to_string(client_context->registered_state
                                                  ->Get<SereneDBClientState>(
                                                    kSereneDBClientStateKey)
                                                  ->copy_stdin_open_count)
                               : "?")
            << std::endl;
  SDB_ASSERT(client_context, "No ClientContext available for COPY FROM STDIN");

  auto sdb_state = client_context->registered_state->Get<SereneDBClientState>(
    kSereneDBClientStateKey);
  SDB_ASSERT(sdb_state, "SereneDB client state not registered");
  SDB_ASSERT(sdb_state->copy_queue,
             "CopyMessagesQueue not set for COPY FROM STDIN");
  SDB_ASSERT(sdb_state->send_buffer, "Send buffer not set for COPY FROM STDIN");

  return duckdb::make_uniq<CopyQueueFileHandle>(*this, *sdb_state);
}

int64_t SereneDBCopyFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                                     int64_t nr_bytes) {
  auto& cq_handle = handle.Cast<CopyQueueFileHandle>();
  auto result = cq_handle.DoRead(buffer, nr_bytes);
  if (result > 0) {
    std::cerr << ">>> CopyFS::Read " << result << " bytes: [";
    auto* p = static_cast<char*>(buffer);
    for (int64_t i = 0; i < std::min(result, int64_t(80)); ++i) {
      if (p[i] >= 32 && p[i] < 127) {
        std::cerr << p[i];
      } else {
        std::cerr << "\\x" << std::hex << (int)(unsigned char)p[i] << std::dec;
      }
    }
    std::cerr << "]" << std::endl;
  }
  return result;
}

void SereneDBCopyFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                                  int64_t nr_bytes, duckdb::idx_t location) {
  throw duckdb::NotImplementedException(
    "Random read not supported for COPY FROM STDIN");
}

bool SereneDBCopyFileSystem::FileExists(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return filename == kDevStdin;
}

duckdb::vector<duckdb::OpenFileInfo> SereneDBCopyFileSystem::Glob(
  const std::string& path, duckdb::FileOpener* opener) {
  return {{std::string{kDevStdin}}};
}

int64_t SereneDBCopyFileSystem::GetFileSize(duckdb::FileHandle& handle) {
  return -1;  // unknown (streaming)
}

void SereneDBCopyFileSystem::Seek(duckdb::FileHandle& handle,
                                  duckdb::idx_t location) {
  throw duckdb::NotImplementedException(
    "Seek not supported for COPY FROM STDIN");
}

void SereneDBCopyFileSystem::Reset(duckdb::FileHandle& handle) {
  throw duckdb::NotImplementedException(
    "Reset not supported for COPY FROM STDIN");
}

duckdb::idx_t SereneDBCopyFileSystem::SeekPosition(duckdb::FileHandle& handle) {
  return 0;
}

duckdb::FileType SereneDBCopyFileSystem::GetFileType(
  duckdb::FileHandle& handle) {
  return duckdb::FileType::FILE_TYPE_FIFO;
}

}  // namespace sdb::connector
