////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/store/memory_file.hpp"

#include <algorithm>
#include <cstring>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/crc.hpp"

namespace irs {

const byte_type* MemoryIndexInput::ReadStable(uint64_t count) noexcept {
  const auto* begin = _begin + count;

  if (begin > _end) {
    return nullptr;
  }

  std::swap(begin, _begin);
  return begin;
}

const byte_type* MemoryIndexInput::ReadStable(uint64_t offset,
                                              uint64_t count) noexcept {
  const auto idx = _file->buffer_offset(offset);
  SDB_ASSERT(idx < _file->buffer_count());
  const auto& buf = _file->get_buffer(idx);
  const auto begin = buf.data + offset - buf.offset;
  const auto end = begin + count;
  const auto buf_end =
    buf.data + std::min(buf.size, _file->Length() - buf.offset);

  if (end <= buf_end) {
    _buf = buf.data;
    _begin = end;
    _end = buf_end;
    _start = buf.offset;
    return begin;
  }

  return nullptr;
}

void MemoryIndexInput::ReadData(byte_type* b, uint64_t count) {
  while (count) {
    if (_begin >= _end) {
      if (IsEOF()) {
        break;
      }
      SwitchBuffer(Position());
    }

    auto copied = std::min<size_t>(std::distance(_begin, _end), count);
    std::memcpy(b, _begin, sizeof(byte_type) * copied);

    count -= copied;
    _begin += copied;
    b += copied;
  }
}

void MemoryIndexInput::Seek(uint64_t pos) {
  // allow seeking past eof(), set to eof
  if (pos >= _file->Length()) {
    _buf = nullptr;
    _begin = nullptr;
    _start = _file->Length();
    return;
  }

  SwitchBuffer(pos);
}

uint32_t MemoryIndexInput::Checksum(uint64_t offset) const {
  if (!_file->Length()) {
    return 0;
  }

  Crc32c crc;

  auto buffer_idx = _file->buffer_offset(Position());
  size_t to_process;

  // process current buffer if exists
  if (_begin) {
    to_process = std::min(offset, Remain());
    crc.process_bytes(_begin, to_process);
    offset -= to_process;
    ++buffer_idx;
  }

  // process intermediate buffers
  auto last_buffer_idx = _file->buffer_count();

  if (last_buffer_idx) {
    --last_buffer_idx;
  }

  for (; offset && buffer_idx < last_buffer_idx; ++buffer_idx) {
    auto& buf = _file->get_buffer(buffer_idx);
    to_process = std::min<size_t>(offset, buf.size);
    crc.process_bytes(buf.data, to_process);
    offset -= to_process;
  }

  // process the last buffer
  if (offset && buffer_idx == last_buffer_idx) {
    auto& buf = _file->get_buffer(last_buffer_idx);
    to_process = std::min<size_t>(offset, _file->Length() - buf.offset);
    crc.process_bytes(buf.data, to_process);
  }

  return crc.checksum();
}

void MemoryIndexInput::SwitchBuffer(size_t pos) {
  auto idx = _file->buffer_offset(pos);
  SDB_ASSERT(idx < _file->buffer_count());
  auto& buf = _file->get_buffer(idx);

  if (buf.data != _buf) {
    _buf = buf.data;
    _start = buf.offset;
    _end = _buf + std::min(buf.size, _file->Length() - _start);
  }

  SDB_ASSERT(_start <= pos && pos < _start + std::distance(_buf, _end));
  _begin = _buf + (pos - _start);
}

void MemoryIndexOutput::FlushBuffer() {
  SDB_ASSERT(Remain() == 0);
  auto idx = _file.buffer_offset(Position());
  auto buf =
    idx < _file.buffer_count() ? _file.get_buffer(idx) : _file.push_buffer();
  _offset = buf.offset;
  _buf = buf.data;
  _pos = _buf;
  _end = _buf + buf.size;
}

MemoryIndexOutput::MemoryIndexOutput(MemoryFile& file) noexcept
  : IndexOutput{nullptr, nullptr}, _file{file} {}

void MemoryIndexOutput::WriteDirect(const byte_type* b, size_t len) {
  Write(*this, b, len);
}

}  // namespace irs
