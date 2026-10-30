////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>

#include "iresearch/utils/assert.hpp"

namespace irs::file_utils {

inline constexpr size_t kPage = 4 * 1024;
inline constexpr size_t kMaxReadahead = 2 * 1024 * 1024;
inline constexpr size_t kPrefetchChunk = 128 * 1024;

void Prefetch(const void* addr, size_t size) noexcept;
bool IsResident(const void* addr, size_t size) noexcept;
size_t Residency(const void* addr, size_t size,
                 std::span<unsigned char> pages) noexcept;
uint32_t ResidencyEpoch() noexcept;
void InvalidateResidency() noexcept;
void SyncResidency() noexcept;
void PollResidency() noexcept;

class ResidencyMap {
 public:
  void Reset(size_t pages);

  bool Valid(uint32_t epoch) const noexcept {
    return _epoch.load(std::memory_order_acquire) == epoch;
  }

  bool Adopt(uint32_t epoch) const noexcept;

  bool Test(size_t page) const noexcept {
    SDB_ASSERT(page < _pages);
    return ((_bits[page / kBits].load(std::memory_order_relaxed) >>
             (page % kBits)) &
            1) != 0;
  }

  bool Test(size_t first, size_t last) const noexcept {
    SDB_ASSERT(first <= last && last < _pages);
    for (auto w = first / kBits, end = last / kBits; w <= end; ++w) {
      const auto mask = Mask(w, first, last);
      if ((_bits[w].load(std::memory_order_relaxed) & mask) != mask) {
        return false;
      }
    }
    return true;
  }

  void Set(size_t first, size_t last) const noexcept {
    SDB_ASSERT(first <= last && last < _pages);
    for (auto w = first / kBits, end = last / kBits; w <= end; ++w) {
      const auto mask = Mask(w, first, last);
      if ((_bits[w].load(std::memory_order_relaxed) & mask) != mask) {
        _bits[w].fetch_or(mask, std::memory_order_relaxed);
      }
    }
  }

  void Set(size_t page) const noexcept { Set(page, page); }

 private:
  static constexpr size_t kBits = 64;
  static constexpr uint32_t kClearing = std::numeric_limits<uint32_t>::max();

  static uint64_t Mask(size_t word, size_t first, size_t last) noexcept {
    const auto lo = word == first / kBits ? first % kBits : 0;
    const auto hi = word == last / kBits ? last % kBits : kBits - 1;
    return (~uint64_t{0} >> (kBits - 1 - hi + lo)) << lo;
  }

  std::unique_ptr<std::atomic<uint64_t>[]> _bits;
  size_t _pages = 0;
  mutable std::atomic<uint32_t> _epoch{0};
};

}  // namespace irs::file_utils
