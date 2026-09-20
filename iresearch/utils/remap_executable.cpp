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

#include "iresearch/utils/remap_executable.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#if defined(__linux__)
#include <dirent.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#if defined(__has_feature)
#define IRS_REMAP_HAS_FEATURE(x) __has_feature(x)
#else
#define IRS_REMAP_HAS_FEATURE(x) 0
#endif

#if defined(__linux__) && defined(__x86_64__) && defined(__SSE2__) && \
  defined(NDEBUG) && !defined(__SANITIZE_ADDRESS__) &&                \
  !defined(__SANITIZE_THREAD__) &&                                    \
  !IRS_REMAP_HAS_FEATURE(address_sanitizer) &&                        \
  !IRS_REMAP_HAS_FEATURE(thread_sanitizer) &&                         \
  !IRS_REMAP_HAS_FEATURE(memory_sanitizer)
#define IRS_REMAP_EXECUTABLE 1
#include <emmintrin.h>
#endif

#ifndef MADV_POPULATE_READ
#define MADV_POPULATE_READ 22
#endif

extern "C" int JemallocControl(const char* name, void* old_value,
                               size_t* old_size, void* new_value,
                               size_t new_size) __asm__("mallctl")
  __attribute__((weak));

namespace irs {
namespace {

#if defined(__linux__)

struct Mapping {
  uintptr_t begin = 0;
  uintptr_t end = 0;
  bool exec = false;
};

std::string ExecutablePath() {
  std::string path(4096, '\0');
  const auto n = readlink("/proc/self/exe", path.data(), path.size());
  if (n <= 0 || static_cast<size_t>(n) >= path.size()) {
    return {};
  }
  path.resize(static_cast<size_t>(n));
  return path;
}

std::vector<Mapping> ExecutableMappings() {
  std::vector<Mapping> mappings;
  const auto exe = ExecutablePath();
  if (exe.empty()) {
    return mappings;
  }
  std::ifstream maps{"/proc/self/maps"};
  std::string line;
  while (std::getline(maps, line)) {
    const auto path_at = line.find('/');
    if (path_at == std::string::npos ||
        line.compare(path_at, std::string::npos, exe) != 0) {
      continue;
    }
    char* next = nullptr;
    const auto begin = std::strtoull(line.c_str(), &next, 16);
    if (next == nullptr || *next != '-') {
      continue;
    }
    const auto end = std::strtoull(next + 1, &next, 16);
    if (next == nullptr || *next != ' ' || next[1] != 'r') {
      continue;
    }
    mappings.push_back({.begin = static_cast<uintptr_t>(begin),
                        .end = static_cast<uintptr_t>(end),
                        .exec = next[3] == 'x'});
  }
  return mappings;
}

size_t Populate(const Mapping& mapping) {
  const auto size = mapping.end - mapping.begin;
  if (madvise(reinterpret_cast<void*>(mapping.begin), size,
              MADV_POPULATE_READ) != 0) {
    return 0;
  }
  return size;
}

#endif

#if defined(IRS_REMAP_EXECUTABLE)

bool SetAllocatorBackgroundThreads(bool enabled) {
  if (JemallocControl == nullptr) {
    return false;
  }
  bool previous = false;
  size_t previous_size = sizeof(previous);
  return JemallocControl("background_thread", &previous, &previous_size,
                         &enabled, sizeof(enabled)) == 0 &&
         previous;
}

size_t ThreadCount() {
  size_t count = 0;
  if (auto* dir = opendir("/proc/self/task"); dir != nullptr) {
    while (const auto* entry = readdir(dir)) {
      count += entry->d_name[0] != '.';
    }
    closedir(dir);
  }
  return count;
}

using RawSyscallFn = int64_t (*)(...);
using RemapStep3Fn = void (*)(void*, size_t, size_t);
using RemapStep2Fn = void (*)(void*, size_t, void*, RawSyscallFn, RemapStep3Fn);

__attribute__((naked, __noinline__)) int64_t RawSyscall(...) {
  __asm__ __volatile__(R"(
    movq %%rdi,%%rax;
    movq %%rsi,%%rdi;
    movq %%rdx,%%rsi;
    movq %%rcx,%%rdx;
    movq %%r8,%%r10;
    movq %%r9,%%r8;
    movq 8(%%rsp),%%r9;
    syscall;
    ret
  )"
                       :
                       :
                       : "memory");
}

__attribute__((naked, __noinline__)) void RemapStep3(void*, size_t, size_t) {
  __asm__ __volatile__(R"(
    subq %%rdx, (%%rsp);
    movl $11, %%eax;
    syscall;
    ret
  )"
                       :
                       :
                       : "memory");
}

__attribute__((__noinline__)) void RemapStep2(void* begin, size_t size,
                                              void* scratch,
                                              RawSyscallFn raw_syscall,
                                              RemapStep3Fn step3) {
  const int64_t offset =
    reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);
  if (raw_syscall(int64_t{SYS_munmap}, begin, size) != 0) {
    return;
  }
  if (raw_syscall(int64_t{SYS_mmap}, begin, size,
                  int64_t{PROT_READ | PROT_WRITE},
                  int64_t{MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED}, int64_t{-1},
                  int64_t{0}) == -1) {
    raw_syscall(int64_t{SYS_exit}, int64_t{1});
  }
  raw_syscall(int64_t{SYS_madvise}, begin, size, int64_t{MADV_HUGEPAGE});

  auto* __restrict dst = reinterpret_cast<__m128i*>(begin);
  const auto* __restrict src = reinterpret_cast<const __m128i*>(scratch);
  const auto* __restrict src_end = reinterpret_cast<const __m128i*>(
    reinterpret_cast<const char*>(scratch) + size);
  while (src < src_end) {
    _mm_storeu_si128(dst, _mm_loadu_si128(src));
    ++dst;
    ++src;
  }

  raw_syscall(int64_t{SYS_mprotect}, begin, size,
              int64_t{PROT_READ | PROT_EXEC});
  step3(scratch, size, static_cast<size_t>(offset));
  __asm__ __volatile__("" : : : "memory");
}

__attribute__((__noinline__)) bool RemapStep1(void* begin, size_t size) {
  void* scratch = mmap(nullptr, size, PROT_READ | PROT_WRITE | PROT_EXEC,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (scratch == MAP_FAILED) {
    return false;
  }
  std::memcpy(scratch, begin, size);
  const int64_t offset =
    reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);
  const auto step2 = reinterpret_cast<RemapStep2Fn>(
    reinterpret_cast<intptr_t>(RemapStep2) + offset);
  const auto raw_syscall = reinterpret_cast<RawSyscallFn>(
    reinterpret_cast<intptr_t>(RawSyscall) + offset);
  step2(begin, size, scratch, raw_syscall, RemapStep3);
  return true;
}

bool RemapText(const Mapping& text) {
  sigset_t all;
  sigset_t saved;
  sigfillset(&all);
  pthread_sigmask(SIG_BLOCK, &all, &saved);
  const bool done =
    RemapStep1(reinterpret_cast<void*>(text.begin), text.end - text.begin);
  pthread_sigmask(SIG_SETMASK, &saved, nullptr);
  return done;
}

#endif

}  // namespace

ExecutableRemap RemapExecutable() {
  ExecutableRemap remap;
#if defined(__linux__)
  const auto mappings = ExecutableMappings();
  if (mappings.empty()) {
    remap.skipped = "cannot read the executable's mappings";
    return remap;
  }
  for (const auto& mapping : mappings) {
    if (!mapping.exec) {
      remap.populated += Populate(mapping);
    }
  }
#if defined(IRS_REMAP_EXECUTABLE)
  const bool background = SetAllocatorBackgroundThreads(false);
  if (ThreadCount() != 1) {
    remap.skipped = "other threads are already running";
  }
#else
  remap.skipped = "not supported by this build";
#endif
  for (const auto& mapping : mappings) {
    if (!mapping.exec) {
      continue;
    }
#if defined(IRS_REMAP_EXECUTABLE)
    if (remap.skipped.empty()) {
      if (RemapText(mapping)) {
        remap.remapped += mapping.end - mapping.begin;
        continue;
      }
      remap.skipped = std::strerror(errno);
    }
#endif
    remap.populated += Populate(mapping);
  }
#if defined(IRS_REMAP_EXECUTABLE)
  if (background) {
    SetAllocatorBackgroundThreads(true);
  }
#endif
#else
  remap.skipped = "not supported on this platform";
#endif
  return remap;
}

}  // namespace irs
