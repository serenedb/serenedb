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

#include "basics/message_chunk.h"

#include <new>

#include "basics/assert.h"

namespace sdb::message {

Chunk::Chunk(size_t capacity)
  : _data{static_cast<uint8_t*>(::operator new(capacity))},
    _capacity{capacity} {}

size_t Chunk::TryWrite(std::string_view data) {
  const auto write_length = std::min(data.size(), FreeSpace());
  if (write_length == 0) [[unlikely]] {
    return 0;
  }
  std::memcpy(_data + _end, data.data(), write_length);
  _end += write_length;
  SDB_ASSERT(_end <= _capacity);
  return write_length;
}

}  // namespace sdb::message
