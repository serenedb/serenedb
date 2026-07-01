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

#include <absl/synchronization/mutex.h>

#include <cstdint>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::network::pg {

class RoleConnectionCounter {
 public:
  bool TryAcquire(ObjectId role, int32_t limit) {
    absl::MutexLock lock{&_mutex};
    uint32_t& count = _counts[role];
    if (limit >= 0 && count >= static_cast<uint32_t>(limit)) {
      return false;
    }
    ++count;
    return true;
  }

  void Release(ObjectId role) {
    absl::MutexLock lock{&_mutex};
    auto it = _counts.find(role);
    if (it == _counts.end()) {
      return;
    }
    if (--it->second == 0) {
      _counts.erase(it);
    }
  }

 private:
  absl::Mutex _mutex;
  containers::FlatHashMap<ObjectId, uint32_t> _counts ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::network::pg
