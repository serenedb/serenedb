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

CopyInReadFile::CopyInReadFile(message::Buffer& buffer,
                               CopyMessagesQueue& copy_queue,
                               size_t column_count)
  : _buffer{buffer}, _copy_queue{copy_queue}, _copy_queue_it{copy_queue} {
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

  _copy_queue.StartListening();
  _buffer.Commit(true);
}

std::string_view CopyInReadFile::pread(
  uint64_t offset, uint64_t length, void* buf,
  const velox::FileIoContext& /*context*/) const {
  SDB_ASSERT(offset >= _prev_offset, "CopyIn does not support seeking back");
  _prev_offset = offset;
  const auto bytes_read = _copy_queue_it.Next(static_cast<char*>(buf), length);
  return {static_cast<char*>(buf), bytes_read};
}

}  // namespace sdb::pg
