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

#include <simdfor.h>

#include <array>
#include <duckdb/common/allocator.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <span>
#include <vector>

#include "basics/bit_packing.hpp"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"

namespace irs {

// Chunk source for inversion containers. `allocator` should be the
// BufferAllocator of the owning database so inversion memory participates in
// the global memory budget; DefaultAllocator is the unaccounted fallback for
// standalone tests.
struct InverterMemory {
  duckdb::Allocator& allocator;
  IResourceManager& rm;

  static InverterMemory Default() noexcept {
    return {duckdb::Allocator::DefaultAllocator(), IResourceManager::gNoop};
  }
};

// Fill-time bitpacked u32 column: values stage into a plain (geometrically
// grown) buffer and every kBlockValues of them seal into a FOR-bitpacked
// arena block at the width the block's maximum actually needs. Sequential
// reads unpack one sealed block at a time; the open staging tail reads as-is.
class PackedU32Column : util::Noncopyable {
 public:
  static constexpr size_t kBlockValues = 1024;
  static constexpr size_t kSimdValues = 128;
  static constexpr size_t kMinStagingValues = kSimdValues;

  explicit PackedU32Column(duckdb::ArenaAllocator& arena) noexcept
    : _arena{&arena} {}

  IRS_FORCE_INLINE void Push(uint32_t value) {
    if (_tail == _tail_end) [[unlikely]] {
      Grow();
    }
    *_tail++ = value;
  }

  void PushN(const uint32_t* values, size_t count) {
    PushNImpl(count, [&](uint32_t* dst, size_t n) {
      std::memcpy(dst, values, n * sizeof(uint32_t));
      values += n;
    });
  }

  void PushNAdd(const uint32_t* values, size_t count, uint32_t base) {
    PushNImpl(count, [&](uint32_t* dst, size_t n) {
      for (size_t i = 0; i < n; ++i) {
        dst[i] = values[i] + base;
      }
      values += n;
    });
  }

  void PushRamp(uint32_t base, size_t count) {
    uint32_t v = base;
    PushNImpl(count, [&](uint32_t* dst, size_t n) {
      for (size_t i = 0; i < n; ++i) {
        dst[i] = ++v;
      }
    });
  }

  void PushNSub(const uint32_t* minuends, const uint32_t* subtrahends,
                size_t count) {
    PushNImpl(count, [&](uint32_t* dst, size_t n) {
      for (size_t i = 0; i < n; ++i) {
        dst[i] = minuends[i] - subtrahends[i];
      }
      minuends += n;
      subtrahends += n;
    });
  }

  IRS_FORCE_INLINE void IncBack(uint32_t delta) noexcept {
    SDB_ASSERT(_tail != _staging);
    _tail[-1] += delta;
  }

  IRS_FORCE_INLINE uint32_t Back() const noexcept {
    SDB_ASSERT(_tail != _staging);
    return _tail[-1];
  }

  uint64_t Size() const noexcept {
    return _packed.size() * kBlockValues +
           static_cast<uint64_t>(_tail - _staging);
  }

  size_t Memory() const noexcept {
    return _packed_bytes + (_tail_end - _staging) * sizeof(uint32_t) +
           _packed.capacity() * sizeof(PackedBlock);
  }

  class Cursor {
   public:
    explicit Cursor(const PackedU32Column& col) noexcept : _col{&col} {}

    std::span<const uint32_t> Next(size_t max_count) noexcept {
      if (_offset == _cur.size()) {
        if (_block < _col->_packed.size()) {
          const auto& blk = _col->_packed[_block++];
          const auto* src = reinterpret_cast<const __m128i*>(blk.data);
          for (size_t base = 0; base < kBlockValues; base += kSimdValues) {
            simdunpackFOR(blk.base, src, _buf.data() + base, blk.bits);
            src += blk.bits;
          }
          _cur = {_buf.data(), kBlockValues};
        } else if (!_tail_read && _col->_tail != _col->_staging) {
          _tail_read = true;
          _cur = {_col->_staging,
                  static_cast<size_t>(_col->_tail - _col->_staging)};
        } else {
          return {};
        }
        _offset = 0;
      }
      const auto count = std::min(max_count, _cur.size() - _offset);
      const auto res = _cur.subspan(_offset, count);
      _offset += count;
      return res;
    }

   private:
    const PackedU32Column* _col;
    std::span<const uint32_t> _cur;
    alignas(16) std::array<uint32_t, kBlockValues> _buf;
    size_t _block = 0;
    size_t _offset = 0;
    bool _tail_read = false;
  };

  void Reset() noexcept {
    _packed.clear();
    _packed_bytes = 0;
    _staging = nullptr;
    _tail = nullptr;
    _tail_end = nullptr;
  }

 private:
  struct PackedBlock {
    const uint32_t* data;
    uint32_t bits;
    uint32_t base;
  };

  template<typename Fill>
  void PushNImpl(size_t count, Fill&& fill) {
    while (count) {
      if (_tail == _tail_end) [[unlikely]] {
        Grow();
      }
      const auto batch =
        std::min(count, static_cast<size_t>(_tail_end - _tail));
      fill(_tail, batch);
      _tail += batch;
      count -= batch;
    }
  }

  // simdfor kernels use aligned SSE loads/stores; the arena only guarantees
  // 8-byte alignment.
  uint32_t* Alloc16(size_t bytes) {
    const auto raw =
      reinterpret_cast<uintptr_t>(_arena->AllocateAligned(bytes + 8));
    return reinterpret_cast<uint32_t*>((raw + 15) & ~uintptr_t{15});
  }

  IRS_NO_INLINE void Grow() {
    const auto staged = static_cast<size_t>(_tail - _staging);
    if (staged == kBlockValues) {
      Seal();
      return;
    }
    const auto capacity =
      _staging ? std::min(staged * 4, kBlockValues) : kMinStagingValues;
    auto* buf = Alloc16(capacity * sizeof(uint32_t));
    if (staged) {
      std::memcpy(buf, _staging, staged * sizeof(uint32_t));
    }
    _staging = buf;
    _tail = buf + staged;
    _tail_end = buf + capacity;
  }

  void Seal() {
    uint32_t mn = _staging[0];
    uint32_t mx = _staging[0];
    for (size_t i = 1; i < kBlockValues; ++i) {
      mn = std::min(mn, _staging[i]);
      mx = std::max(mx, _staging[i]);
    }
    const auto bits = packed::Maxbits32(mx - mn);
    const uint32_t* out = nullptr;
    if (bits) {
      const auto bytes = packed::BytesRequired32(kBlockValues, bits);
      auto* buf = Alloc16(bytes);
      auto* dst = reinterpret_cast<__m128i*>(buf);
      for (size_t base = 0; base < kBlockValues; base += kSimdValues) {
        simdpackFOR(mn, _staging + base, dst, bits);
        dst += bits;
      }
      out = buf;
      _packed_bytes += bytes;
    }
    _packed.push_back({out, bits, mn});
    _tail = _staging;
  }

  duckdb::ArenaAllocator* _arena;
  std::vector<PackedBlock> _packed;
  uint32_t* _staging = nullptr;
  uint32_t* _tail = nullptr;
  uint32_t* _tail_end = nullptr;
  size_t _packed_bytes = 0;
};

}  // namespace irs
