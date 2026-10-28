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

#include "iresearch/utils/huge_page_allocator.hpp"

#ifdef __linux__
#include <sys/mman.h>
#endif

namespace irs {

#ifdef __linux__
namespace {

constexpr size_t kHugePage = size_t{2} << 20;

}  // namespace
#endif

void* AllocateHugePages(size_t bytes) {
#ifdef __linux__
  if (bytes >= kHugePage) {
    auto* p = ::operator new(bytes, std::align_val_t{kHugePage});
    ::madvise(p, bytes, MADV_HUGEPAGE);
    return p;
  }
#endif
  return ::operator new(bytes);
}

void FreeHugePages(void* p, size_t bytes) noexcept {
#ifdef __linux__
  if (bytes >= kHugePage) {
    ::operator delete(p, bytes, std::align_val_t{kHugePage});
    return;
  }
#endif
  ::operator delete(p, bytes);
}

}  // namespace irs
