////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdlib>
#include <cstring>
#include <string_view>

#include "basics/assert.h"
#include "pg/sql_exception_macro.h"

namespace sdb::basics {

using index_t = uint64_t;

// TODO(gnusi) use more specialized error codes?
template<typename T>
class Buffer {
  static_assert(sizeof(T) == 1);

 public:
  constexpr Buffer() noexcept : _buffer{_local}, _capacity{sizeof(_local)} {
    poison(_buffer, _capacity);
    initWithNone();
  }

  explicit Buffer(size_t capacity) : Buffer{} { reserve(capacity); }

  Buffer(const Buffer& other) : _buffer{_local}, _capacity{sizeof(_local)} {
    copy<false>(other);
  }

  Buffer& operator=(const Buffer& other) {
    if (this == &other) [[unlikely]] {
      return *this;
    }
    copy<true>(other);
    return *this;
  }

  Buffer(Buffer&& other) noexcept : _buffer{_local}, _capacity{sizeof(_local)} {
    move<false>(other);
  }

  Buffer& operator=(Buffer&& other) noexcept {
    if (this == &other) [[unlikely]] {
      return *this;
    }
    move<true>(other);
    return *this;
  }

  ~Buffer() {
    if (!usesLocalMemory()) {
      std::allocator<T>{}.deallocate(_buffer, _capacity);
    }
  }

  constexpr T* data() noexcept { return _buffer; }
  constexpr const T* data() const noexcept { return _buffer; }

  constexpr bool empty() const noexcept { return _size == 0; }
  constexpr auto size() const noexcept { return _size; }
  constexpr auto capacity() const noexcept { return _capacity; }

  constexpr std::string_view toString() const noexcept {
    return {reinterpret_cast<const char*>(_buffer), _size};
  }

  constexpr void reset() noexcept { _size = 0; }

  void resetTo(size_t position) {
    if (position > _capacity) [[unlikely]] {
      THROW_SQL_ERROR(ERR_MSG("index out of bounds"));
    }
    _size = position;
  }

  constexpr void advance(index_t value = 1) noexcept {
    SDB_ASSERT(static_cast<uint64_t>(_size) + value <= _capacity);
    _size += value;
  }

  constexpr void rollback(index_t value) noexcept {
    SDB_ASSERT(value <= _size);
    _size -= value;
  }

  void clear() noexcept {
    _size = 0;
    if (!usesLocalMemory()) {
      std::allocator<T>{}.deallocate(_buffer, _capacity);
      _buffer = _local;
      _capacity = sizeof(_local);
    }
    poison(_buffer, _capacity);
    initWithNone();
  }

  T* steal() noexcept {
    SDB_ASSERT(!usesLocalMemory());

    auto buffer = std::exchange(_buffer, _local);
    _capacity = sizeof(_local);
    clear();
    return buffer;
  }

  decltype(auto) operator[](this auto& self, index_t position) noexcept {
    SDB_ASSERT(position < self._size);
    return self._buffer[position];
  }

  decltype(auto) at(this auto& self, size_t position) {
    if (position >= self._size) [[unlikely]] {
      THROW_SQL_ERROR(ERR_MSG("index out of bounds"));
    }
    return self[position];
  }

  void push_back(char c) {
    reserve(1);
    _buffer[_size++] = c;
  }

  template<typename C>
  void append(const C* p, index_t len) {
    static_assert(sizeof(C) == 1);
    reserve(len);
    std::memcpy(_buffer + _size, p, len);
    _size += len;
  }

  void append(std::string_view value) {
    return append(value.data(), value.size());
  }

  void append(const Buffer& value) {
    return append(value.data(), value.size());
  }

  void reserve(size_t len) {
    SDB_ASSERT(_size <= _capacity);
    if (len > _capacity - _size) {
      grow(len);
    }
  }

  constexpr bool usesLocalMemory() const noexcept { return _buffer == _local; }

 private:
  template<bool Assign>
  void copy(const Buffer& other) {
    if (other._size > _capacity) {
#if _LIBCPP_STD_VER >= 23
      auto [buffer, capacity] =
        std::allocator<T>{}.allocate_at_least(other._size);
#else
      auto buffer = std::allocator<T>{}.allocate(other._size);
      auto capacity = other._size;
#endif
      // TODO(mbkkt) realloc?
      if (Assign && !usesLocalMemory()) {
        std::allocator<T>{}.deallocate(_buffer, _capacity);
      }

      _buffer = buffer;
      _capacity = capacity;
      poison(_buffer + other._size, _capacity - other._size);
    }
    _size = other._size;
    if (_size == 0) {
      initWithNone();
    } else {
      std::memcpy(_buffer, other._buffer, _size);
    }
  }

  template<bool Assign>
  void move(Buffer& other) noexcept {
    if (other.usesLocalMemory()) {
      std::memcpy(_buffer, other._buffer, other._size);
      poison(_buffer + other._size, other._capacity - other._size);
      if (other._size == 0) {
        initWithNone();
      }
    } else if (!Assign || usesLocalMemory()) {
      _buffer = std::exchange(other._buffer, other._local);
      _capacity = std::exchange(other._capacity, sizeof(other._local));
    } else {
      std::swap(_buffer, other._buffer);
      std::swap(_capacity, other._capacity);
    }

    _size = std::exchange(other._size, 0);
    poison(other._buffer, other._capacity);
    other.initWithNone();
  }

  constexpr void initWithNone() noexcept { _buffer[0] = '\x00'; }

  static constexpr void poison([[maybe_unused]] T* p,
                               [[maybe_unused]] index_t l) noexcept {
#ifdef SDB_DEBUG
    std::memset(p, 0xa5, l);
#endif
  }

  void grow(size_t len) {
    const auto new_len = std::max(
      static_cast<uint64_t>(_size) + len,
      // ensure the buffer grows sensibly and not by 1 byte only
      std::min<uint64_t>(_size * 1.5, std::numeric_limits<index_t>::max()));
    SDB_ASSERT(new_len > _size);

    // TODO(mbkkt) realloc?
    // intentionally do not initialize memory here
    // intentionally also do not care about alignments here,
    // as we expect T to be 1-byte-aignable
    SDB_ASSERT(new_len > 0);
#if _LIBCPP_STD_VER >= 23
    auto [buffer, capacity] = std::allocator<T>{}.allocate_at_least(new_len);
#else
    auto buffer = std::allocator<T>{}.allocate(new_len);
    auto capacity = new_len;
#endif
    if (capacity > std::numeric_limits<index_t>::max()) [[unlikely]] {
      THROW_SQL_ERROR(ERR_MSG("Buffer is unable to allocate"));
    }
    // copy existing data into buffer
    memcpy(buffer, _buffer, _size);
    if (!usesLocalMemory()) {
      std::allocator<T>{}.deallocate(_buffer, _capacity);
    }

    _buffer = buffer;
    _capacity = capacity;
    poison(_buffer + _size, _capacity - _size);
    if (_size == 0) [[unlikely]] {
      initWithNone();
    }

    SDB_ASSERT(_size <= _capacity);
  }

  T* _buffer = nullptr;
  index_t _size = 0;
  index_t _capacity = 0;

  T _local[256 - 24 - sizeof(T*) - 2 * sizeof(index_t)];
};

using BufferUInt8 = Buffer<uint8_t>;

}  // namespace sdb::basics
