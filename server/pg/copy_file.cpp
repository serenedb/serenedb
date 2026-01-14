////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/copy_file.h"

#include <absl/base/internal/endian.h>
#include <velox/type/Type.h>

#include <span>

#include "pg/pg_comm_task.h"
#include "pg/protocol.h"
#include "pg/serialize.h"

namespace sdb::pg {

CopyOutWriteFile::CopyOutWriteFile(message::Buffer& buffer,
                                   const size_t column_count)
  : _buffer{buffer} {
  const size_t message_size = sizeof(uint8_t) + sizeof(int32_t) +
                              sizeof(uint8_t) + sizeof(int16_t) +
                              (column_count * sizeof(int16_t));
  std::span data{_buffer.GetContiguousData(message_size), message_size};

  data[0] = PQ_MSG_COPY_OUT_RESPONSE;
  data = data.subspan(1);

  const int32_t length = static_cast<int32_t>(message_size - 1);
  absl::big_endian::Store32(data.data(), length);
  data = data.subspan(4);

  data[0] = static_cast<uint8_t>(VarFormat::Text);
  data = data.subspan(1);

  absl::big_endian::Store16(data.data(), column_count);
  data = data.subspan(2);

  for (int16_t i = 0; i < column_count; ++i) {
    absl::big_endian::Store16(data.data(),
                              static_cast<uint8_t>(VarFormat::Text));
    data = data.subspan(2);
  }

  _buffer.Commit(true);
}

// TODO: don't send many messages for small writes
// instead, buffer them and send fewer larger messages
void CopyOutWriteFile::append(std::string_view data) {
  const auto uncommitted_size = _buffer.GetUncommittedSize();
  auto* prefix_data = _buffer.GetContiguousData(5);
  _buffer.WriteUncommitted(data);

  prefix_data[0] = PQ_MSG_COPY_DATA;
  absl::big_endian::Store32(
    prefix_data + 1, _buffer.GetUncommittedSize() - uncommitted_size - 1);
  _buffer.Commit(false);

  _size += data.size();
}

void CopyOutWriteFile::flush() { _buffer.Commit(true); }

void CopyOutWriteFile::close() { _buffer.Write(ToBuffer(kCopyDone), true); }

uint64_t CopyOutWriteFile::size() const { return _size; }

CopyInReadFile::CopyInReadFile(message::Buffer& buffer,
                               CopyMessagesQueue& copy_queue,
                               const size_t column_count)
  : _buffer{buffer}, _copy_queue{copy_queue} {
  const size_t message_size = sizeof(uint8_t) + sizeof(int32_t) +
                              sizeof(uint8_t) + sizeof(int16_t) +
                              (column_count * sizeof(int16_t));

  std::span data{_buffer.GetContiguousData(message_size), message_size};

  data[0] = PQ_MSG_COPY_IN_RESPONSE;
  data = data.subspan(1);

  const int32_t length = static_cast<int32_t>(message_size - 1);
  absl::big_endian::Store32(data.data(), length);
  data = data.subspan(4);

  data[0] = static_cast<uint8_t>(VarFormat::Text);
  data = data.subspan(1);

  absl::big_endian::Store16(data.data(), column_count);
  data = data.subspan(2);
  for (int16_t i = 0; i < column_count; ++i) {
    absl::big_endian::Store16(data.data(),
                              static_cast<uint8_t>(VarFormat::Text));
    data = data.subspan(2);
  }

  _buffer.Commit(true);
}

uint64_t CopyInReadFile::ReadInternal(char* pos, uint64_t length) const {
  auto msg = _copy_queue.GetMsg();
  if (msg.IsDone()) {
    return 0;
  }
  // TODO: error malformed packet if data.size() < 5
  std::string_view data{msg.data};
  data.remove_prefix(5);  // Skip message type and length
  std::memcpy(pos, data.data(), data.size());
  return data.size();
}

std::string_view CopyInReadFile::pread(
  uint64_t /*offset*/, uint64_t length, void* buf,
  const velox::FileStorageContext& /*fileStorageContext*/) const {
  const auto bytes_read = ReadInternal(static_cast<char*>(buf), length);
  return {static_cast<char*>(buf), bytes_read};
}

uint64_t CopyInReadFile::size() const {
  return std::numeric_limits<uint64_t>::max();
}

uint64_t CopyInReadFile::memoryUsage() const { return 0; }

bool CopyInReadFile::shouldCoalesce() const { return false; }

std::string CopyInReadFile::getName() const { return "CopyInReadFile"; }

uint64_t CopyInReadFile::getNaturalReadSize() const {
  // 8192 is a common default buffer size for the copydata packet in psql,
  // subtracting 5 bytes for message type and length
  return 8192 - 5;
}

}  // namespace sdb::pg
