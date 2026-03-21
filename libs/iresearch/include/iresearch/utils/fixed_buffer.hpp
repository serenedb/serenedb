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

#include "basics/assert.h"

namespace irs::utils {

template<typename T, size_t Capacity, size_t Align = alignof(T)>
struct FixedBuffer {
  // NOLINTBEGIN
  auto* data(this auto& self) noexcept { return self._data; }
  size_t size() const noexcept { return _size; }
  bool empty() const noexcept { return _size == 0; }
  constexpr size_t capacity() const noexcept { return Capacity; }
  auto& operator[](this auto& self, size_t i) noexcept { return self._data[i]; }
  auto* begin(this auto& self) noexcept { return self._data; }
  auto* end(this auto& self) noexcept { return self.data() + self.size(); }
  auto& front(this auto& self) noexcept {
    SDB_ASSERT(!self.empty());
    return self[0];
  }
  auto& back(this auto& self) noexcept {
    SDB_ASSERT(!self.empty());
    return self[self._size - 1];
  }
  void clear() noexcept { _size = 0; }
  void push_back(T v) noexcept {
    SDB_ASSERT(_size < Capacity);
    _data[_size++] = v;
  }
  void resize(size_t n) noexcept {
    SDB_ASSERT(n <= Capacity);
    _size = n;
  }

  // NOLINTEND

 private:
  alignas(Align) T _data[Capacity];
  size_t _size = 0;
};

}  // namespace irs::utils
