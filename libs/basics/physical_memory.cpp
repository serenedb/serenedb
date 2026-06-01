////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "basics/physical_memory.h"

#include <unistd.h>

#include "basics/operating-system.h"
#include "basics/string_utils.h"
#include "basics/units_helper.h"

#ifdef SERENEDB_HAVE_MACH
#include <mach/mach_host.h>
#include <mach/mach_port.h>
#include <mach/mach_traps.h>
#include <mach/task.h>
#include <mach/thread_act.h>
#include <mach/vm_map.h>
#endif

#if defined(SERENEDB_HAVE_MACOS_MEM_STATS)
#include <sys/sysctl.h>
#endif

using namespace sdb;

namespace {

/// gets the physical memory size
#if defined(SERENEDB_HAVE_MACOS_MEM_STATS)
uint64_t physicalMemoryImpl() {
  int mib[2];

  // Get the Physical memory size
  mib[0] = CTL_HW;
#ifdef SERENEDB_HAVE_MACOS_MEM_STATS
  mib[1] = HW_MEMSIZE;
#else
  mib[1] = HW_PHYSMEM;  // The bytes of physical memory. (kenel + user space)
#endif
  size_t length = sizeof(int64_t);
  int64_t physicalMemory;
  sysctl(mib, 2, &physicalMemory, &length, nullptr, 0);

  return static_cast<uint64_t>(physicalMemory);
}

#else
#ifdef SERENEDB_HAVE_SC_PHYS_PAGES
uint64_t PhysicalMemoryImpl() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);

  return static_cast<uint64_t>(pages * page_size);
}

#endif
#endif

struct PhysicalMemoryCache {
  PhysicalMemoryCache()
    : cached_value(PhysicalMemoryImpl()), overridden(false) {
    const char* env = std::getenv("SERENEDB_OVERRIDE_DETECTED_TOTAL_MEMORY");
    if (env != nullptr) {
      std::string value =
        basics::string_utils::RemoveWhitespaceAndComments(env);

      if (!value.empty()) {
        uint64_t v = sdb::options::units_helper::FromString<uint64_t>(value);
        if (v != 0) {
          // value in environment variable must always be > 0
          cached_value = v;
          overridden = true;
        }
      }
    }
  }
  uint64_t cached_value;
  bool overridden;
};

const PhysicalMemoryCache kCache;

}  // namespace

/// return physical memory size from cache
uint64_t sdb::physical_memory::GetValue() { return ::kCache.cached_value; }

/// return if physical memory size was overridden
bool sdb::physical_memory::Overridden() { return ::kCache.overridden; }
