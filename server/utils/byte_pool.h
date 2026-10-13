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
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <new>

#include "server/utils/number_of_cores.h"

namespace sdb::utils {

template<typename Tag>
class BytePool {
 public:
  static constexpr size_t kMaxPooledBytes = size_t{4} << 20;

  static void* Allocate(size_t size) noexcept {
    const size_t cls = ClassOf(size);
    const bool pooled = cls <= kMaxClass;
    if (pooled) {
      if (auto* block = Pool()[cls - kMinClass].Pop()) {
        return block + kHeaderBytes;
      }
    }
    const size_t bytes = pooled ? size_t{1} << cls : size;
    auto* block = static_cast<std::byte*>(
      ::operator new(kHeaderBytes + bytes, std::nothrow));
    if (block == nullptr) {
      return nullptr;
    }
    const size_t tag = pooled ? cls : kUnpooled;
    std::memcpy(block, &tag, sizeof(tag));
    return block + kHeaderBytes;
  }

  static void Free(void* bytes) noexcept {
    if (bytes == nullptr) {
      return;
    }
    auto* block = static_cast<std::byte*>(bytes) - kHeaderBytes;
    size_t cls = kUnpooled;
    std::memcpy(&cls, block, sizeof(cls));
    if (cls != kUnpooled) {
      block = Pool()[cls - kMinClass].Push(block);
    }
    ::operator delete(block);
  }

  static void* Allocate(void*, size_t size) noexcept { return Allocate(size); }
  static void Free(void*, void* bytes) noexcept { Free(bytes); }

  static size_t Slots() noexcept {
    static const size_t kSlots =
      std::bit_ceil(static_cast<size_t>(2 * CountPhysicalCores()));
    return kSlots;
  }

 private:
  static constexpr size_t kHeaderBytes = alignof(std::max_align_t);
  static constexpr size_t kMinClass = 6;
  static constexpr size_t kMaxClass = std::bit_width(kMaxPooledBytes) - 1;
  static constexpr size_t kUnpooled = 0;

  static size_t ClassOf(size_t size) noexcept {
    return size <= (size_t{1} << kMinClass) ? kMinClass
                                            : std::bit_width(size - 1);
  }

  class RingStack {
   public:
    RingStack()
      : _mask{Slots() - 1}, _slots{std::make_unique<Slot[]>(Slots())} {}

    std::byte* Pop() noexcept {
      const size_t top = _top.fetch_sub(1, std::memory_order_relaxed) - 1;
      return _slots[top & _mask].block.exchange(nullptr,
                                                std::memory_order_acquire);
    }

    std::byte* Push(std::byte* block) noexcept {
      const size_t top = _top.fetch_add(1, std::memory_order_relaxed);
      return _slots[top & _mask].block.exchange(block,
                                                std::memory_order_acq_rel);
    }

   private:
    struct alignas(64) Slot {
      std::atomic<std::byte*> block{nullptr};
    };

    std::atomic<size_t> _top{0};
    const size_t _mask;
    std::unique_ptr<Slot[]> _slots;
  };

  using Classes = std::array<RingStack, kMaxClass - kMinClass + 1>;

  static Classes& Pool() {
    static auto* const kClasses = new Classes{};
    return *kClasses;
  }
};

template<typename Pool>
class PooledBytes {
 public:
  explicit PooledBytes(size_t size)
    : _data{static_cast<uint8_t*>(Pool::Allocate(size))}, _size{size} {
    if (_data == nullptr) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OUT_OF_MEMORY),
        ERR_MSG("out of memory: cannot allocate ", size, " bytes"));
    }
  }

  ~PooledBytes() { Pool::Free(_data); }

  PooledBytes(const PooledBytes&) = delete;
  PooledBytes& operator=(const PooledBytes&) = delete;

  uint8_t* Data() const noexcept { return _data; }
  size_t Size() const noexcept { return _size; }

 private:
  uint8_t* _data;
  size_t _size;
};

}  // namespace sdb::utils
