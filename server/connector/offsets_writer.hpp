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
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <span>

#include "connector/highlight/highlight_types.h"

namespace sdb::connector {

inline void WriteRowOffsets(duckdb::Vector& list_vec, duckdb::idx_t row_idx,
                            std::span<const highlight::HitRange> hits) {
  const auto running = duckdb::ListVector::GetListSize(list_vec);
  const auto count = hits.size() * 2;
  duckdb::ListVector::Reserve(list_vec, running + count);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(list_vec);
  entries[row_idx].offset = running;
  entries[row_idx].length = count;
  if (!hits.empty()) {
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    auto* child_data = duckdb::FlatVector::GetDataMutable<int32_t>(child);
    for (size_t i = 0; i < hits.size(); ++i) {
      child_data[running + i * 2] = static_cast<int32_t>(hits[i].first);
      child_data[running + i * 2 + 1] = static_cast<int32_t>(hits[i].second);
    }
  }
  duckdb::ListVector::SetListSize(list_vec, running + count);
}

}  // namespace sdb::connector
