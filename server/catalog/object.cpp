////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "object.h"

#include "app/app_server.h"
#include "catalog/identifiers/object_id.h"
#include "database/ticks.h"
#include "general_server/state.h"

#ifdef SDB_CLUSTER
#include "cluster/cluster_feature.h"
#include "cluster/cluster_info.h"
#endif

namespace sdb::catalog {

Object::~Object() = default;

Object::Object(ObjectId owner_id, ObjectId id, std::string_view name,
               ObjectType type)
  : _name{name},
    _id{id != id::kInvalid ? id : NextId()},
    _owner_id{owner_id},
    _type{type} {
  UpdateTickServer(GetId().id());
}

#ifdef SDB_CLUSTER
ObjectId NextId() {
  const ObjectId id{[] {
    if (!ServerState::instance()->IsClusterNode()) {
      return NewTickServer();
    }

// #ifdef SDB_GTEST
//     if (ClusterEngine::gMocking) {
//       return NewTickServer();
//     }
// #endif
    return SerenedServer::Instance()
      .getFeature<ClusterFeature>()
      .clusterInfo()
      .uniqid();
  }()};
  SDB_ASSERT(id.isSet());
  return id;
}
#else
ObjectId NextId() { return ObjectId{NewTickServer()}; }
#endif

}  // namespace sdb::catalog
