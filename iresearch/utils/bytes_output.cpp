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

#include "iresearch/utils/bytes_output.hpp"

#include <algorithm>
#include <cstring>

namespace irs {
namespace {

constexpr size_t kMinCapacity = 64;

}  // namespace

void BytesOutput::Overflow(const byte_type* data, size_t size) {
  const auto written = static_cast<size_t>(_pos - _begin);
  const auto capacity = static_cast<size_t>(_end - _begin);
  if (written + size > capacity) {
    const auto grown = std::max({2 * capacity, written + size, kMinCapacity});
    auto buf = std::make_unique_for_overwrite<byte_type[]>(grown);
    if (written != 0) {
      std::memcpy(buf.get(), _begin, written);
    }
    _buf = std::move(buf);
    _begin = _buf.get();
    _pos = _begin + written;
    _end = _begin + grown;
  }
  if (size != 0) {
    std::memcpy(_pos, data, size);
    _pos += size;
  }
}

}  // namespace irs
