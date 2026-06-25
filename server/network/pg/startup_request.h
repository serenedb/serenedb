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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"

namespace sdb::network::pg {

// A decoded StartupMessage: the requested protocol version, the client's GUC
// parameters (with the `_pq_.*` protocol options removed), the names of those
// removed `_pq_` options (the client is told which were ignored), and whether a
// replication mode was requested.
struct StartupRequest {
  uint32_t major = 0;
  uint32_t minor = 0;
  containers::FlatHashMap<std::string, std::string> params;
  std::vector<std::string> unrecognized_pq;
  bool replication = false;
  bool replication_invalid = false;
};

// Decode a StartupMessage payload: the version word followed by null-delimited
// key/value pairs. Pure -- no IO, no session state.
StartupRequest ParseStartup(std::string_view payload);

}  // namespace sdb::network::pg
