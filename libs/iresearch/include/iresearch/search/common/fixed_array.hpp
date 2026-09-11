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

#include <array>
#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "basics/shared.hpp"

namespace irs::search {

template<typename T>
class FixedArray {
 public:
  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  FixedArray() = default;

  explicit FixedArray(size_t size)
    : FixedArray{size, [](T&, size_t) noexcept {}} {}

  template<typename Init>
  FixedArray(size_t size, Init&& init) {
    if (size == 0) {
      return;
    }
    std::allocator<T> alloc;
    T* const data = alloc.allocate(size);
    size_t built = 0;
    try {
      while (built != size) {
        std::construct_at(data + built);
        ++built;
        init(data[built - 1], built - 1);
      }
    } catch (...) {
      std::destroy_n(data, built);
      alloc.deallocate(data, size);
      throw;
    }
    _data = data;
    _size = size;
  }

  template<typename Args>
  FixedArray(size_t size, std::piecewise_construct_t, Args&& args) {
    if (size == 0) {
      return;
    }
    std::allocator<T> alloc;
    T* const data = alloc.allocate(size);
    size_t built = 0;
    try {
      while (built != size) {
        std::apply(
          [&](auto&&... one) {
            std::construct_at(data + built,
                              std::forward<decltype(one)>(one)...);
          },
          args(built));
        ++built;
      }
    } catch (...) {
      std::destroy_n(data, built);
      alloc.deallocate(data, size);
      throw;
    }
    _data = data;
    _size = size;
  }

  FixedArray(FixedArray&&) = delete;
  FixedArray& operator=(FixedArray&&) = delete;

  ~FixedArray() {
    if (_data == nullptr) {
      return;
    }
    std::destroy_n(_data, _size);
    std::allocator<T>{}.deallocate(_data, _size);
  }

  T* data() noexcept { return _data; }
  const T* data() const noexcept { return _data; }

  size_t size() const noexcept { return _size; }
  bool empty() const noexcept { return _size == 0; }

  T& operator[](size_t i) noexcept {
    SDB_ASSERT(i < _size);
    return _data[i];
  }
  const T& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _size);
    return _data[i];
  }

  T& front() noexcept {
    SDB_ASSERT(_size != 0);
    return _data[0];
  }
  const T& front() const noexcept {
    SDB_ASSERT(_size != 0);
    return _data[0];
  }

  T& back() noexcept {
    SDB_ASSERT(_size != 0);
    return _data[_size - 1];
  }
  const T& back() const noexcept {
    SDB_ASSERT(_size != 0);
    return _data[_size - 1];
  }

  T* begin() noexcept { return _data; }
  T* end() noexcept { return _data + _size; }
  const T* begin() const noexcept { return _data; }
  const T* end() const noexcept { return _data + _size; }

 private:
  T* _data = nullptr;
  size_t _size = 0;
};

template<typename T, size_t N>
class FixedRun {
 public:
  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  FixedRun() = default;

  explicit FixedRun(size_t size) { SDB_ASSERT(size == N); }

  template<typename Init>
  FixedRun(size_t size, Init&& init) {
    SDB_ASSERT(size == N);
    for (size_t i = 0; i != N; ++i) {
      init(_data[i], i);
    }
  }

  template<typename Args>
  FixedRun(size_t size, std::piecewise_construct_t, Args&& args)
    : FixedRun{std::piecewise_construct, std::forward<Args>(args),
               std::make_index_sequence<N>{}} {
    SDB_ASSERT(size == N);
  }

  template<typename... Args>
  explicit FixedRun(std::piecewise_construct_t, Args&&... args)
    : _data{std::make_from_tuple<T>(std::forward<Args>(args))...} {
    static_assert(sizeof...(Args) == N);
  }

  FixedRun(FixedRun&&) = delete;
  FixedRun& operator=(FixedRun&&) = delete;

  T* data() noexcept { return _data.data(); }
  const T* data() const noexcept { return _data.data(); }

  static constexpr size_t size() noexcept { return N; }
  static constexpr bool empty() noexcept { return N == 0; }

  T& operator[](size_t i) noexcept {
    SDB_ASSERT(i < N);
    return _data[i];
  }
  const T& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < N);
    return _data[i];
  }

  T& front() noexcept { return _data.front(); }
  const T& front() const noexcept { return _data.front(); }

  T& back() noexcept { return _data.back(); }
  const T& back() const noexcept { return _data.back(); }

  T* begin() noexcept { return _data.data(); }
  T* end() noexcept { return _data.data() + N; }
  const T* begin() const noexcept { return _data.data(); }
  const T* end() const noexcept { return _data.data() + N; }

 private:
  template<typename Args, size_t... I>
  FixedRun(std::piecewise_construct_t, Args&& args, std::index_sequence<I...>)
    : _data{std::make_from_tuple<T>(args(I))...} {}

  std::array<T, N> _data{};
};

template<typename T, size_t N>
using RunOf = std::conditional_t<N == 0, FixedArray<T>, FixedRun<T, N>>;

}  // namespace irs::search
