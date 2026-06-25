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

#include "pg/system_table.h"

namespace sdb::pg {

// system.metrics: one row per process-wide dynamic gauge.
// Backed by relaxed-atomic counters (sdb::metrics), re-read per query snapshot.
// NOLINTBEGIN
struct SdbMetrics {
  static constexpr uint64_t kId = 999995;  // TODO(codeworse): assign proper OID
  static constexpr std::string_view kName = "sdb_metrics";

  Text metric;
  uint64_t value;
  Text description;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<SdbMetrics>::GetTableData();

}  // namespace sdb::pg
