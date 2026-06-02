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

#pragma once

#include <cstdint>
#include <limits>
#include <string>

namespace sdb::catalog::persistence {

struct SequenceOptions {
  std::string name;
  uint64_t start_value = 1;
  uint64_t increment = 1;
  uint64_t min_value = 1;
  uint64_t max_value = std::numeric_limits<int64_t>::max();
  uint64_t cache = 1;
  uint64_t owner_table_id = 0;
  bool cycle = false;

  uint64_t Seed() const noexcept { return start_value - increment; }
};

}  // namespace sdb::catalog::persistence
