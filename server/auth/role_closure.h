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

#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

struct Snapshot;

}  // namespace sdb::catalog

namespace sdb::auth {

struct RoleClosure {
  std::vector<ObjectId> closure;
  bool is_superuser = false;
};

class RoleClosureCache {
 public:
  const RoleClosure& Get(const catalog::Snapshot& snapshot, ObjectId role) const;

 private:
  mutable absl::Mutex _mu;
  mutable containers::NodeHashMap<ObjectId, RoleClosure> _by_role
    ABSL_GUARDED_BY(_mu);
};

}  // namespace sdb::auth
