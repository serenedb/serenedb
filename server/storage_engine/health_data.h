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

#include <chrono>

#include "basics/result.h"

namespace vpack {

class Builder;
class Slice;

}  // namespace vpack
namespace sdb {

struct HealthDataBase {
  /// timestamp of full last health check execution. we only execute the
  /// full health checks every so often in order to reduce load
  std::chrono::steady_clock::time_point last_check_timestamp;
  bool background_error = false;
  uint64_t free_disk_space_bytes = 0;
  double free_disk_space_percent = 0.0;
};

struct HealthData : HealthDataBase {
  Result res;

  static HealthData fromVPack(vpack::Slice slice);
  void toVPack(vpack::Builder& builder, bool with_details) const;
};

}  // namespace sdb
