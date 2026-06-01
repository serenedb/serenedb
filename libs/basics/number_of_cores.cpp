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

#include "basics/number_of_cores.h"

#include <unistd.h>

#include <cstdint>
#include <string>
#include <thread>

#include "basics/operating-system.h"
#include "basics/string_utils.h"

using namespace sdb;

namespace {

size_t NumberOfCoresImpl() {
#ifdef SERENEDB_SC_NPROCESSORS_ONLN
  auto n = sysconf(_SC_NPROCESSORS_ONLN);

  if (n < 0) {
    n = 0;
  }

  if (n > 0) {
    return static_cast<size_t>(n);
  }
#endif

  return static_cast<size_t>(std::thread::hardware_concurrency());
}

struct NumberOfCoresCache {
  NumberOfCoresCache() : cached_value(NumberOfCoresImpl()), overridden(false) {
    if (const char* env =
          std::getenv("SERENEDB_OVERRIDE_DETECTED_NUMBER_OF_CORES");
        env != nullptr && *env != '\0') {
      uint64_t v = sdb::basics::string_utils::Uint64(env);
      if (v != 0) {
        cached_value = static_cast<size_t>(v);
        overridden = true;
      }
    }
  }

  size_t cached_value;
  bool overridden;
};

const NumberOfCoresCache kCache;

}  // namespace

/// return number of cores from cache
size_t sdb::number_of_cores::GetValue() { return ::kCache.cached_value; }

/// return if number of cores was overridden
bool sdb::number_of_cores::Overridden() { return ::kCache.overridden; }
