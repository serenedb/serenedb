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

#include "iresearch/formats/column/codecs/text/segment_buffer.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace irs::codecs::text {
namespace {

constexpr size_t kMinCapacity = 64 * 1024;

}  // namespace

void SegmentBuffer::Grow(size_t need) {
  const size_t capacity = std::max({need, 2 * _data.size(), kMinCapacity});
  TrackedScratchVector<char> data(capacity, _data.get_allocator());
  if (_size != 0) {
    std::memcpy(data.data(), _data.data(), _size);
  }
  _data = std::move(data);
}

}  // namespace irs::codecs::text
