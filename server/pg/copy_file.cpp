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
  _buffer.WriteContiguousData(message_size, [&](uint8_t* data) {
    uint8_t* ptr = data;

    *ptr++ = PQ_MSG_COPY_OUT_RESPONSE;

    const int32_t length = static_cast<int32_t>(message_size - 1);
    absl::big_endian::Store32(ptr, length);
    ptr += 4;

    *ptr++ = static_cast<uint8_t>(VarFormat::Text);  // text format

    absl::big_endian::Store16(ptr, column_count);
    ptr += 2;

    for (int16_t i = 0; i < column_count; ++i) {
      absl::big_endian::Store16(ptr, 0);
      ptr += 2;
    }

    return message_size;
  });

  _buffer.Commit(true);
}

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

void CopyOutWriteFile::close() {
  SDB_ASSERT(!_closed);
  _buffer.Write(ToBuffer(kCopyDone), true);
  _closed = true;
}

uint64_t CopyOutWriteFile::size() const { return _size; }

// CopyInReadFile implementation
CopyInReadFile::CopyInReadFile(message::Buffer& buffer,
                               const size_t column_count)
  : _buffer{buffer} {
  const size_t message_size = sizeof(uint8_t) + sizeof(int32_t) +
                              sizeof(uint8_t) + sizeof(int16_t) +
                              (column_count * sizeof(int16_t));

  _buffer.WriteContiguousData(message_size, [&](uint8_t* data) {
    uint8_t* ptr = data;

    *ptr++ = PQ_MSG_COPY_IN_RESPONSE;

    const int32_t length = static_cast<int32_t>(message_size - 1);
    absl::big_endian::Store32(ptr, length);
    ptr += 4;

    *ptr++ = static_cast<uint8_t>(VarFormat::Text);

    absl::big_endian::Store16(ptr, column_count);
    ptr += 2;

    for (int16_t i = 0; i < column_count; ++i) {
      absl::big_endian::Store16(ptr, 0);
      ptr += 2;
    }

    return message_size;
  });

  _buffer.Commit(true);
}

std::string_view CopyInReadFile::pread(
  uint64_t offset, uint64_t length, void* buf,
  const velox::FileStorageContext& fileStorageContext) const {
  throw std::runtime_error("pread not supported for COPY FROM STDIN");
}

std::string CopyInReadFile::pread(
  uint64_t offset, uint64_t length,
  const velox::FileStorageContext& fileStorageContext) const {
  throw std::runtime_error("pread not supported for COPY FROM STDIN");
}

uint64_t CopyInReadFile::preadv(
  uint64_t offset, const std::vector<folly::Range<char*>>& buffers,
  const velox::FileStorageContext& fileStorageContext) const {
  throw std::runtime_error("preadv not supported for COPY FROM STDIN");
}

uint64_t CopyInReadFile::size() const { return 0; }

uint64_t CopyInReadFile::memoryUsage() const { return 0; }

bool CopyInReadFile::shouldCoalesce() const { return false; }

std::string CopyInReadFile::getName() const { return "CopyInReadFile"; }

uint64_t CopyInReadFile::getNaturalReadSize() const { return 8192; }

}  // namespace sdb::pg
