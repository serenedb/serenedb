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

#include "iresearch/utils/fault_hits.hpp"

#include <absl/base/no_destructor.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/numbers.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/fault_points.hpp>
#include <string>

namespace irs {
namespace {

struct Hits {
  absl::Mutex mutex;
  absl::flat_hash_map<std::string, uint64_t> left ABSL_GUARDED_BY(mutex);
};

Hits& GetHits() {
  static absl::NoDestructor<Hits> gHits;
  return *gHits;
}

}  // namespace

FaultHit ParseFaultHit(std::string_view spec) noexcept {
  const auto at = spec.rfind('@');
  uint64_t hit = 0;
  if (at == std::string_view::npos ||
      !absl::SimpleAtoi(spec.substr(at + 1), &hit) || hit == 0) {
    return {.name = spec};
  }
  return {.name = spec.substr(0, at), .hit = hit};
}

void SetFaultHit(FaultHit fault) {
  auto& hits = GetHits();
  absl::MutexLock lock{&hits.mutex};
  if (fault.hit == 0) {
    hits.left.erase(fault.name);
  } else {
    hits.left.insert_or_assign(fault.name, fault.hit);
  }
}

void ClearFaultHits() noexcept {
  auto& hits = GetHits();
  absl::MutexLock lock{&hits.mutex};
  hits.left.clear();
}

bool FaultHitFires(std::string_view name) {
  auto& hits = GetHits();
  absl::MutexLock lock{&hits.mutex};
  const auto it = hits.left.find(name);
  if (it == hits.left.end()) {
    return true;
  }
  if (it->second == 0 || --it->second != 0) {
    return false;
  }
  duckdb::FaultPoints::Disarm(name);
  return true;
}

}  // namespace irs
