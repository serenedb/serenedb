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

#include "replication/spill_buffer.h"

#include <algorithm>
#include <cstring>

namespace sdb::replication {

void SpillBuffer::Append(std::string_view message) {
  const auto size = message.size() + 4;
  if (_blocks.empty() || _blocks.back().capacity - _blocks.back().used < size) {
    const auto capacity = std::max<size_t>(_buffers.GetBlockSize(), size);
    _tail = _buffers.Allocate(duckdb::MemoryTag::EXTENSION, capacity, false);
    _blocks.push_back(
      {.handle = _tail.GetBlockHandle(), .capacity = capacity, .start = _size});
  } else if (!_tail.IsValid()) {
    _tail = _buffers.Pin(_blocks.back().handle);
  }
  auto& block = _blocks.back();
  auto* data = _tail.GetDataMutable() + block.used;
  absl::big_endian::Store32(data, static_cast<uint32_t>(message.size()));
  std::memcpy(data + 4, message.data(), message.size());
  block.used += size;
  _size += size;
}

void SpillBuffer::Truncate(size_t offset) {
  _tail = {};
  while (!_blocks.empty() && _blocks.back().start >= offset) {
    _size = _blocks.back().start;
    _blocks.pop_back();
  }
  if (!_blocks.empty() && offset < _size) {
    _blocks.back().used = offset - _blocks.back().start;
    _size = offset;
  }
}

bool SpillBuffer::Reader::Next(std::string_view& message) {
  while (_block < _buffer._blocks.size()) {
    auto& block = _buffer._blocks[_block];
    if (_pos < block.used) {
      if (!_pinned.IsValid()) {
        _pinned = _buffer._buffers.Pin(block.handle);
      }
      const auto* data = _pinned.Ptr() + _pos;
      const auto size = absl::big_endian::Load32(data);
      message = {reinterpret_cast<const char*>(data) + 4, size};
      _pos += 4 + size;
      return true;
    }
    _pinned = {};
    ++_block;
    _pos = 0;
  }
  return false;
}

}  // namespace sdb::replication
