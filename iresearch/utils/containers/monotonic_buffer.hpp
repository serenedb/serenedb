////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <numeric>
#include <type_traits>

#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

template<typename T, size_t BlockSizeAlign, size_t MaxBlockSize>
class MonotonicBuffer {
  static_assert(BlockSizeAlign > 1);
  static_assert(MaxBlockSize > 1);
  static_assert(MaxBlockSize % BlockSizeAlign == 0);

  struct Block {
    T* data = nullptr;
    size_t size = 0;
  };

 public:
  MonotonicBuffer(IResourceManager& resource_manager,
                  size_t initial_size) noexcept
    : _resource_manager{resource_manager}, _next_size{initial_size} {
    SDB_ASSERT(initial_size > 1);
  }

  MonotonicBuffer(const MonotonicBuffer&) = delete;
  MonotonicBuffer& operator=(const MonotonicBuffer&) = delete;
  ~MonotonicBuffer() {
    if (!_blocks.empty()) {
      Destroy();
    }
  }

  template<typename... Args>
  T* Construct(Args&&... args) {
    auto* p = Allocate(1);
    return new (p) T{std::forward<Args>(args)...};
  }

  T* Allocate(size_t n) {
    if (_available < n) [[unlikely]] {
      AllocateMemory(n);
    }
    auto* p = _current;
    _current += n;
    _available -= n;
    return p;
  }

  // Just allow to reuse last allocated memory
  void Free(size_t n) noexcept {
    SDB_ASSERT(n <= static_cast<size_t>(_current - Initial()));
    _current -= n;
    _available += n;
  }

  // Release all memory, except current_ and keep next_size_
  void Clear() noexcept {
    if (_blocks.empty()) [[unlikely]] {
      return;
    }

    _available = _blocks.back().size;
    _current = _blocks.back().data;

    if (_blocks.size() == 1) [[unlikely]] {
      return;
    }

    _blocks.pop_back();
    Destroy();
    _blocks.emplace_back(_current, _available);
  }

  // Release all memory, but keep next_size_ equal to last allocated size
  void Reset() noexcept {
    if (_blocks.empty()) [[unlikely]] {
      return;
    }
    _next_size = _blocks.back().size;
    _available = 0;
    _current = nullptr;

    Destroy();
  }

  T* Initial() const noexcept {
    SDB_ASSERT(!_blocks.empty());
    return _blocks.back().data;
  }

  T* Current() const noexcept {
    SDB_ASSERT(_current);
    return _current;
  }

  std::span<Block> Blocks() noexcept { return _blocks; }

  size_t Available() const noexcept { return _available; }

 private:
  void Destroy() noexcept {
    size_t bytes = 0;
    for (auto& block : _blocks) {
      const auto block_bytes = block.size * sizeof(T);
      bytes += block_bytes;
      operator delete(block.data, block_bytes, std::align_val_t{alignof(T)});
    }
    _blocks = {};
    _resource_manager.Decrease(bytes);
  }

  void AllocateMemory(size_t n) {
    n = std::max(n, _next_size);
    n = (n + BlockSizeAlign - 1) / BlockSizeAlign * BlockSizeAlign;
    n = std::min(n, MaxBlockSize);
    const auto bytes = n * sizeof(T);

    _resource_manager.Increase(bytes);

    auto* data =
      static_cast<T*>(operator new(bytes, std::align_val_t{alignof(T)}));
    _blocks.emplace_back(data, n);
    _current = data;

    _available = n;
    _next_size = n * 2;
    SDB_ASSERT(_available < _next_size);
  }

  IResourceManager& _resource_manager;

  size_t _next_size;
  std::vector<Block> _blocks;

  size_t _available = 0;
  T* _current = nullptr;
};

template<typename T>
class MonotonicBuffer<T, 1, 0> {
  static constexpr size_t kAlign =
    (alignof(T) * alignof(void*)) / std::gcd(alignof(T), alignof(void*));

  struct alignas(kAlign) Block {
    Block* prev = nullptr;
  };

  static_assert(std::is_trivially_destructible_v<Block>);

 public:
  MonotonicBuffer(IResourceManager& resource_manager,
                  size_t initial_size) noexcept
    : _resource_manager{resource_manager}, _next_size{initial_size} {
    SDB_ASSERT(initial_size > 1);
  }

  MonotonicBuffer(const MonotonicBuffer&) = delete;
  MonotonicBuffer& operator=(const MonotonicBuffer&) = delete;
  ~MonotonicBuffer() {
    if (_head) {
      Destroy();
    }
  }

  template<typename... Args>
  T* Construct(Args&&... args) {
    auto* p = Allocate(1);
    return new (p) T{std::forward<Args>(args)...};
  }

  T* Allocate(size_t n) {
    if (_available < n) [[unlikely]] {
      AllocateMemory(n);
    }
    auto* p = _current;
    _current += n;
    _available -= n;
    return p;
  }

  // Just allow to reuse last allocated memory
  void Free(size_t n) noexcept {
    SDB_ASSERT(n >= (_current - Initial()));
    _current -= n;
    _available += n;
  }

  // Release all memory, except current_ and keep next_size_
  void Clear() noexcept {
    if (!_head) [[unlikely]] {
      return;
    }

    auto* const initial_current = Initial();
    _available += _current - initial_current;
    _current = initial_current;

    if (!_head->prev) [[unlikely]] {
      return;
    }

    Release(_head->prev);
    _head->prev = nullptr;

    const auto new_managed_bytes = sizeof(Block) + _available * sizeof(T);
    SDB_ASSERT(_managed_bytes > new_managed_bytes);
    _resource_manager.Decrease(_managed_bytes - new_managed_bytes);
    _managed_bytes = new_managed_bytes;
  }

  // Release all memory, but keep next_size_ equal to last allocated size
  void Reset() noexcept {
    if (!_head) [[unlikely]] {
      return;
    }
    auto* const initial_current = Initial();
    _next_size = _available + (_current - initial_current);
    _available = 0;
    _current = nullptr;

    Destroy();
    _head = nullptr;
    _managed_bytes = 0;
  }

  T* Initial() const noexcept {
    SDB_ASSERT(_head);
    return reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(_head) +
                                sizeof(Block));
  }

  T* Current() const noexcept {
    SDB_ASSERT(_current);
    return _current;
  }

 private:
  void Destroy() noexcept {
    Release(_head);
    _resource_manager.Decrease(_managed_bytes);
  }

  void Release(Block* it) noexcept {
    while (it != nullptr) {
      operator delete(std::exchange(it, it->prev), std::align_val_t{kAlign});
    }
  }

  void AllocateMemory(size_t n) {
    n = std::max(n, _next_size);
    const auto size = sizeof(Block) + n * sizeof(T);
    // TODO(mbkkt) use allocate_at_least but it's not supported by jemalloc :(
    // Probably when it will be available will make sense just store size of
    // block inside block, instead of whole buffer _managed_bytes
    _resource_manager.Increase(size);
    _managed_bytes += size;
    auto* p =
      static_cast<uint8_t*>(operator new(size, std::align_val_t{kAlign}));
    _head = new (p) Block{_head};
    SDB_ASSERT(reinterpret_cast<uint8_t*>(_head) == p);
    p += sizeof(Block);
    SDB_ASSERT(reinterpret_cast<uintptr_t>(p) % alignof(T) == 0);
    _current = reinterpret_cast<T*>(p);
    _available = n;
    _next_size = (n * 3) / 2;
    SDB_ASSERT(_available < _next_size);
  }

  IResourceManager& _resource_manager;
  size_t _managed_bytes = 0;

  size_t _next_size;
  Block* _head = nullptr;

  size_t _available = 0;
  T* _current = nullptr;
};

}  // namespace irs
