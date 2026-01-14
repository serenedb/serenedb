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

#pragma once

#include <velox/common/file/File.h>
#include <velox/type/Type.h>

#include "basics/fwd.h"
#include "basics/message_buffer.h"
#include "pg/copy_messages_queue.h"

namespace sdb::pg {

class CopyOutWriteFile final : public velox::WriteFile {
 public:
  CopyOutWriteFile(message::Buffer& buffer, const size_t column_count);

  void append(std::string_view data) final;
  void flush() final;
  void close() final;
  uint64_t size() const final;

 private:
  message::Buffer& _buffer;
  uint64_t _size = 0;
};

class CopyInReadFile final : public velox::ReadFile {
 public:
  CopyInReadFile(message::Buffer& buffer, CopyMessagesQueue& copy_queue,
                 size_t column_count);

  std::string_view pread(
    uint64_t offset, uint64_t length, void* buf,
    const velox::FileStorageContext& fileStorageContext = {}) const final;

  uint64_t size() const final;
  uint64_t memoryUsage() const final;
  bool shouldCoalesce() const final;
  std::string getName() const final;
  uint64_t getNaturalReadSize() const final;

 private:
  uint64_t ReadInternal(char* pos, uint64_t length) const;

  message::Buffer& _buffer;
  CopyMessagesQueue& _copy_queue;
};

}  // namespace sdb::pg
