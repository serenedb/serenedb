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

#include "iresearch/formats/column/codecs/segment_buffer.hpp"

#include <algorithm>
#include <cstring>
#include <duckdb/common/enums/memory_tag.hpp>

namespace irs::codecs::text {
namespace {

constexpr size_t kMinCapacity = 64 * 1024;

}  // namespace

void SegmentBuffer::Grow(size_t need) {
  const auto capacity = std::max({need, 2 * _capacity, kMinCapacity});
  auto handle =
    _buffers->Allocate(duckdb::MemoryTag::IN_MEMORY_TABLE, capacity, true);
  if (_size != 0) {
    std::memcpy(handle.GetDataMutable(), _handle.Ptr(), _size);
  }
  _handle = std::move(handle);
  _capacity = capacity;
}

}  // namespace irs::codecs::text
