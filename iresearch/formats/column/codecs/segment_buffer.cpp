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

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <cstring>

namespace irs::codecs::text {
namespace {

constexpr size_t kMinCapacity = 64 * 1024;

}  // namespace

void SegmentBuffer::Grow(size_t need) {
  const size_t capacity = std::max({need, 2 * _capacity, kMinCapacity});
  _buffers->ReserveMemory(capacity - _capacity);
  absl::Cleanup release = [&] {
    _buffers->FreeReservedMemory(capacity - _capacity);
  };
  auto data = std::make_unique_for_overwrite<char[]>(capacity);
  std::move(release).Cancel();
  if (_size != 0) {
    std::memcpy(data.get(), _data.get(), _size);
  }
  _data = std::move(data);
  _capacity = capacity;
}

}  // namespace irs::codecs::text
