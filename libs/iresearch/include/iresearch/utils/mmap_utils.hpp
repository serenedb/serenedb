////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/file_utils_ext.hpp"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"

#if defined(_WIN32)

#include "mman_win32.hpp"

////////////////////////////////////////////////////////////////////////////////
/// @brief constants for madvice
////////////////////////////////////////////////////////////////////////////////
#define IR_MADVICE_NORMAL 0
#define IR_MADVICE_SEQUENTIAL 0
#define IR_MADVICE_RANDOM 0
#define IR_MADVICE_WILLNEED 0
#define IR_MADVICE_DONTNEED 0
#define IR_MADVICE_DONTDUMP 0

#else

#include <sys/mman.h>

/// a wrapper for MAP_ANONYMOUS / MAP_ANON
///
///               MAP_ANON  MAP_ANONYMOUS
///    OpenBSD         Y         Y(*)
///    Linux           Y(*)      Y
///    FreeBSD         Y         Y
///    NetBSD          Y         N
///    OSX             Y         N
///    Solaris         Y         N
///    HP-UX           N         Y
///    AIX             N         Y
///    IRIX            N         N
///    (*) synonym to other
#ifndef MAP_ANON
#ifdef MAP_ANONYMOUS
#define MAP_ANON MAP_ANONYMOUS
#else
#error "System does not support mapping anonymous pages?"
#endif
#endif

#define IR_MADVICE_NORMAL MADV_NORMAL
#define IR_MADVICE_SEQUENTIAL MADV_SEQUENTIAL
#define IR_MADVICE_RANDOM MADV_RANDOM
#define IR_MADVICE_WILLNEED MADV_WILLNEED
#define IR_MADVICE_DONTNEED MADV_DONTNEED

#endif

namespace irs::mmap_utils {

class MMapHandle : private util::Noncopyable {
 public:
  MMapHandle(IResourceManager& rm) noexcept : _rm{rm} { init(); }

  ~MMapHandle() noexcept { close(); }

  bool open(const path_char_t* file) noexcept;
  void close() noexcept;

  explicit operator bool() const noexcept { return _fd >= 0; }

  void* addr() const noexcept { return _addr; }
  size_t size() const noexcept { return _size; }
  ptrdiff_t fd() const noexcept { return _fd; }

  bool advise(int advice) noexcept {
    return 0 == ::madvise(_addr, _size, advice);
  }

  void dontneed(bool value) noexcept { _dontneed = value; }

 private:
  void init() noexcept;

  void* _addr;     // the beginning of mmapped region
  size_t _size;    // file size
  ptrdiff_t _fd;   // file descriptor
  bool _dontneed;  // request to free pages on close
  IResourceManager& _rm;
};

}  // namespace irs::mmap_utils
