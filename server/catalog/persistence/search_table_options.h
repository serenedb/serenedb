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

namespace sdb::catalog::persistence {

// Persisted per-table options for a Search-engine table -- the analog of
// InvertedIndexOptions, resolved at CREATE from WITH options / config-variable
// defaults. 0 = disabled (matches the inverted semantics).
struct SearchTableOptions {
  uint32_t refresh_interval_ms = 0;
  uint32_t compaction_interval_ms = 0;
  uint32_t cleanup_interval_step = 0;
};

}  // namespace sdb::catalog::persistence
