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

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

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
