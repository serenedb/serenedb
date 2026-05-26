////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vector>

#include "basics/managed_allocator.hpp"
#include "basics/shared.hpp"

namespace irs {

struct IResourceManager {
  static IResourceManager gNoop;
#ifdef SDB_DEV
  static IResourceManager gForbidden;
#endif

  IResourceManager() = default;
  virtual ~IResourceManager() = default;

  IResourceManager(const IResourceManager&) = delete;
  IResourceManager operator=(const IResourceManager&) = delete;

  virtual void Increase([[maybe_unused]] size_t v) {
#ifdef SDB_DEV
    SDB_ASSERT(this != &gForbidden);
#endif
    SDB_ASSERT(v != 0);
  }

  virtual void Decrease([[maybe_unused]] size_t v) noexcept {
#ifdef SDB_DEV
    SDB_ASSERT(this != &gForbidden);
#endif
    SDB_ASSERT(v != 0);
  }

  IRS_FORCE_INLINE void DecreaseChecked(size_t v) noexcept {
#ifdef SDB_DEV
    SDB_ASSERT(this != &gForbidden);
#endif
    if (v != 0) {
      Decrease(v);
    }
  }
};

struct ResourceManagementOptions {
  static ResourceManagementOptions gDefault;

  IResourceManager* transactions{&IResourceManager::gNoop};
  IResourceManager* readers{&IResourceManager::gNoop};
  IResourceManager* compactions{&IResourceManager::gNoop};
  IResourceManager* file_descriptors{&IResourceManager::gNoop};
  IResourceManager* cached_columns{&IResourceManager::gNoop};
};

// TODO(mbkkt) rename to ManagedStdAllocator
template<typename T>
struct ManagedTypedAllocator
  : ManagedAllocator<IResourceManager, std::allocator<T>> {
  using Base = ManagedAllocator<IResourceManager, std::allocator<T>>;

  explicit ManagedTypedAllocator()
    : Base{
#if !defined(_MSC_VER) && defined(SDB_DEV)
        IResourceManager::gForbidden
#else
        IResourceManager::gNoop
#endif
      } {
  }

  using Base::Base;
};

template<typename T>
using ManagedVector = std::vector<T, ManagedTypedAllocator<T>>;

}  // namespace irs
