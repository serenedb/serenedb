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

#pragma once

#include <cstddef>
#include <duckdb/storage/buffer_manager.hpp>
#include <memory>
#include <string_view>
#include <type_traits>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {

class SegmentBuffer {
 public:
  explicit SegmentBuffer(duckdb::BufferManager& buffers) noexcept
    : _buffers{&buffers} {}

  SegmentBuffer(const SegmentBuffer&) = delete;
  SegmentBuffer& operator=(const SegmentBuffer&) = delete;

  ~SegmentBuffer() { _buffers->FreeReservedMemory(_capacity); }

  template<typename Self>
  auto* Data(this Self& self) noexcept {
    if constexpr (std::is_const_v<Self>) {
      return static_cast<const char*>(self._data.get());
    } else {
      return self._data.get();
    }
  }

  char* Extend(size_t n) {
    if (_size + n > _capacity) {
      Grow(_size + n);
    }
    auto* at = Data() + _size;
    _size += n;
    return at;
  }

  void Shrink(size_t n) noexcept {
    SDB_ASSERT(n <= _size);
    _size -= n;
  }

  void Truncate(size_t size) noexcept {
    SDB_ASSERT(size <= _size);
    _size = size;
  }

  size_t Size() const noexcept { return _size; }

  std::string_view View() const noexcept { return {Data(), _size}; }

 private:
  void Grow(size_t need);

  duckdb::BufferManager* _buffers;
  std::unique_ptr<char[]> _data;
  size_t _capacity = 0;
  size_t _size = 0;
};

}  // namespace irs::codecs::text
