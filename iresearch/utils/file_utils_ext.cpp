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

#include "file_utils_ext.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <utility>

#ifndef _WIN32
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace irs::file_utils {

#ifndef _WIN32
namespace {

constexpr size_t kWillNeedChunk = 32 * 1024;

size_t SystemPage() noexcept {
  static const size_t kPageSize = ::sysconf(_SC_PAGESIZE);
  return kPageSize;
}

std::pair<uintptr_t, size_t> PageRange(const void* addr, size_t size) noexcept {
  const auto begin = reinterpret_cast<uintptr_t>(addr);
  const auto aligned = begin & ~(SystemPage() - 1);
  return {aligned, size + (begin - aligned)};
}

std::atomic<uint32_t> gResidencyEpoch{1};

}  // namespace
#endif

void Prefetch(const void* addr, size_t size) noexcept {
#ifndef _WIN32
  if (size == 0) {
    return;
  }
  const auto [aligned, total] = PageRange(addr, size);
  for (size_t offset = 0; offset < total; offset += kWillNeedChunk) {
    ::madvise(reinterpret_cast<void*>(aligned + offset),
              std::min(kWillNeedChunk, total - offset), MADV_WILLNEED);
  }
#endif
}

bool IsResident(const void* addr, size_t size) noexcept {
#ifndef _WIN32
  if (size == 0) {
    return true;
  }
  constexpr size_t kMaxPages = 1024;
  const auto page = SystemPage();
  const auto [aligned, total] = PageRange(addr, size);
#ifdef __APPLE__
  char resident[kMaxPages];
#else
  unsigned char resident[kMaxPages];
#endif
  for (size_t done = 0; done < total; done += kMaxPages * page) {
    const auto len = std::min(total - done, kMaxPages * page);
    if (::mincore(reinterpret_cast<void*>(aligned + done), len, resident) !=
          0 ||
        !std::all_of(resident, resident + (len + page - 1) / page,
                     [](unsigned char value) { return (value & 1) != 0; })) {
      return false;
    }
  }
  return true;
#else
  return false;
#endif
}

size_t Residency(const void* addr, size_t size,
                 std::span<unsigned char> pages) noexcept {
#ifndef _WIN32
  if (size == 0) {
    return 0;
  }
  const auto page = SystemPage();
  const auto [aligned, total] = PageRange(addr, size);
#ifdef __APPLE__
  auto* resident = reinterpret_cast<char*>(pages.data());
#else
  auto* resident = pages.data();
#endif
  if ((total + page - 1) / page > pages.size() ||
      ::mincore(reinterpret_cast<void*>(aligned), total, resident) != 0) {
    return 0;
  }
  return page;
#else
  return 0;
#endif
}

uint32_t ResidencyEpoch() noexcept {
#ifndef _WIN32
  return gResidencyEpoch.load(std::memory_order_relaxed);
#else
  return 0;
#endif
}

void InvalidateResidency() noexcept {
#ifndef _WIN32
  gResidencyEpoch.fetch_add(1, std::memory_order_relaxed);
#endif
}

void SyncResidency() noexcept {
#ifdef RUSAGE_THREAD
  thread_local long faults = 0;
  rusage usage;
  if (::getrusage(RUSAGE_THREAD, &usage) != 0) {
    InvalidateResidency();
  } else if (usage.ru_majflt != faults) {
    faults = usage.ru_majflt;
    InvalidateResidency();
  }
#else
  InvalidateResidency();
#endif
}

void PollResidency() noexcept {
  constexpr uint32_t kPollEvery = 64;
  thread_local uint32_t polls = 0;
  if (++polls % kPollEvery == 0) {
    SyncResidency();
  }
}

void ResidencyMap::Reset(size_t pages) {
  _pages = pages;
  _bits =
    std::make_unique<std::atomic<uint64_t>[]>((pages + kBits - 1) / kBits);
  _epoch.store(0, std::memory_order_relaxed);
}

bool ResidencyMap::Adopt(uint32_t epoch) const noexcept {
  auto current = _epoch.load(std::memory_order_acquire);
  while (current != epoch) {
    if (current == kClearing || current > epoch) {
      return false;
    }
    if (_epoch.compare_exchange_weak(current, kClearing,
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      for (size_t i = 0, n = (_pages + kBits - 1) / kBits; i != n; ++i) {
        _bits[i].store(0, std::memory_order_relaxed);
      }
      _epoch.store(epoch, std::memory_order_release);
      return true;
    }
  }
  return true;
}

}  // namespace irs::file_utils
