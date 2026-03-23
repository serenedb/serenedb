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

#pragma once

#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/resource_usage.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/identifiers/shard_id.h"

namespace sdb {

using MonitoredString =
  std::basic_string<char, std::char_traits<char>,
                    ResourceUsageAllocator<char, ResourceMonitor>>;

using MonitoredStringVector =
  std::vector<MonitoredString,
              ResourceUsageAllocator<MonitoredString, ResourceMonitor>>;

using MonitoredShardIDVector =
  std::vector<ObjectId, ResourceUsageAllocator<ObjectId, ResourceMonitor>>;

using MonitoredCollectionToShardMap = containers::FlatHashMap<
  ObjectId, MonitoredShardIDVector, absl::Hash<ObjectId>, std::equal_to<>,
  ResourceUsageAllocator<std::pair<const ObjectId, MonitoredShardIDVector>,
                         ResourceMonitor>>;

}  // namespace sdb
