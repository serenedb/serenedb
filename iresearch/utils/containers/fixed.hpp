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

#include <absl/cleanup/cleanup.h>

#include <array>
#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/query_memory.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::containers {

template<typename T>
inline constexpr bool kDefaultInit = requires { requires T::kDefaultInit; };

template<typename T, size_t N = 0>
class Fixed {
 public:
  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  Fixed() = default;

  explicit Fixed(size_t size) { SDB_ASSERT(size == N); }

  template<typename Init>
  Fixed(size_t size, Init&& init) {
    SDB_ASSERT(size == N);
    for (size_t i = 0; i != N; ++i) {
      init(_data.values[i], i);
    }
  }

  template<typename Args>
  Fixed(size_t size, std::piecewise_construct_t, Args&& args)
    : Fixed{std::piecewise_construct, std::forward<Args>(args),
            std::make_index_sequence<N>{}} {
    SDB_ASSERT(size == N);
  }

  Fixed(MemoryArena&, size_t size) : Fixed{size} {}

  template<typename Init>
  Fixed(MemoryArena&, size_t size, Init&& init)
    : Fixed{size, std::forward<Init>(init)} {}

  template<typename Args>
  Fixed(MemoryArena&, size_t size, std::piecewise_construct_t, Args&& args)
    : Fixed{size, std::piecewise_construct, std::forward<Args>(args)} {}

  template<typename... Args>
  explicit Fixed(std::piecewise_construct_t, Args&&... args)
    : _data{{std::make_from_tuple<T>(std::forward<Args>(args))...}} {
    static_assert(sizeof...(Args) == N);
  }

  Fixed(Fixed&&) = delete;
  Fixed& operator=(Fixed&&) = delete;

  T* data() noexcept { return _data.values.data(); }
  const T* data() const noexcept { return _data.values.data(); }

  static constexpr size_t size() noexcept { return N; }
  static constexpr bool empty() noexcept { return N == 0; }

  T& operator[](size_t i) noexcept {
    SDB_ASSERT(i < N);
    return _data.values[i];
  }
  const T& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < N);
    return _data.values[i];
  }

  T& front() noexcept { return _data.values.front(); }
  const T& front() const noexcept { return _data.values.front(); }

  T& back() noexcept { return _data.values.back(); }
  const T& back() const noexcept { return _data.values.back(); }

  T* begin() noexcept { return _data.values.data(); }
  T* end() noexcept { return _data.values.data() + N; }
  const T* begin() const noexcept { return _data.values.data(); }
  const T* end() const noexcept { return _data.values.data() + N; }

 private:
  struct Zeroed {
    std::array<T, N> values{};
  };

  struct Raw {
    std::array<T, N> values;
  };

  template<typename Args, size_t... I>
  Fixed(std::piecewise_construct_t, Args&& args, std::index_sequence<I...>)
    : _data{{std::make_from_tuple<T>(args(I))...}} {}

  std::conditional_t<kDefaultInit<T>, Raw, Zeroed> _data;
};

template<typename T>
class Fixed<T, 0> {
 public:
  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  Fixed() = default;

  explicit Fixed(size_t size) : Fixed{size, [](T&, size_t) noexcept {}} {}

  Fixed(MemoryArena& arena, size_t size)
    : Fixed{arena, size, [](T&, size_t) noexcept {}} {}

  template<typename Init>
  Fixed(size_t size, Init&& init) {
    BuildDefault(nullptr, size, init);
  }

  template<typename Init>
  Fixed(MemoryArena& arena, size_t size, Init&& init) {
    BuildDefault(&arena, size, init);
  }

  template<typename Args>
  Fixed(size_t size, std::piecewise_construct_t, Args&& args) {
    BuildFrom(nullptr, size, args);
  }

  template<typename Args>
  Fixed(MemoryArena& arena, size_t size, std::piecewise_construct_t,
        Args&& args) {
    BuildFrom(&arena, size, args);
  }

  Fixed(Fixed&& rhs) noexcept
    : _data{std::exchange(rhs._data, nullptr)},
      _size{std::exchange(rhs._size, 0)},
      _on_arena{rhs._on_arena} {}

  Fixed& operator=(Fixed&& rhs) noexcept {
    std::swap(_data, rhs._data);
    std::swap(_size, rhs._size);
    std::swap(_on_arena, rhs._on_arena);
    return *this;
  }

  ~Fixed() { Release(); }

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
  template<typename Construct>
  void Build(MemoryArena* arena, size_t size, Construct& construct) {
    if (size == 0) {
      return;
    }
    T* const data =
      arena ? arena->Allocate<T>(size) : std::allocator<T>{}.allocate(size);
    size_t built = 0;
    absl::Cleanup undo = [&] noexcept {
      std::destroy_n(data, built);
      if (!arena) {
        std::allocator<T>{}.deallocate(data, size);
      }
    };
    for (; built != size; ++built) {
      construct(data + built, built);
    }
    std::move(undo).Cancel();
    _data = data;
    _size = size;
    _on_arena = arena;
  }

  template<typename Init>
  void BuildDefault(MemoryArena* arena, size_t size, Init& init) {
    auto construct = [&](T* p, size_t i) {
      if constexpr (kDefaultInit<T>) {
        ::new (static_cast<void*>(p)) T;
      } else {
        std::construct_at(p);
      }
      absl::Cleanup destroy = [&] noexcept { std::destroy_at(p); };
      init(*p, i);
      std::move(destroy).Cancel();
    };
    Build(arena, size, construct);
  }

  template<typename Args>
  void BuildFrom(MemoryArena* arena, size_t size, Args& args) {
    auto construct = [&](T* p, size_t i) {
      std::apply(
        [&](auto&&... one) {
          std::construct_at(p, std::forward<decltype(one)>(one)...);
        },
        args(i));
    };
    Build(arena, size, construct);
  }

  void Release() noexcept {
    if (_data == nullptr) {
      return;
    }
    std::destroy_n(_data, _size);
    if (!_on_arena) {
      std::allocator<T>{}.deallocate(_data, _size);
    }
  }

  T* _data = nullptr;
  size_t _size = 0;
  bool _on_arena = false;
};

}  // namespace irs::containers
