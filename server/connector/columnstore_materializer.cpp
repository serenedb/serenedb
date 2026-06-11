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

#include "connector/columnstore_materializer.h"

namespace sdb::connector {

ColumnstoreMaterializer::ColumnstoreMaterializer(
  const irs::ColReader& reader, std::span<const irs::field_id> column_ids,
  std::span<const duckdb::idx_t> output_slots)
  : _ctx{reader} {
  SDB_ASSERT(column_ids.size() == output_slots.size());
  _bound.reserve(column_ids.size());
  for (size_t i = 0; i < column_ids.size(); ++i) {
    if (const auto* r = reader.Column(column_ids[i])) {
      _bound.push_back(Binding{
        .reader = r,
        .output_slot = output_slots[i],
        .state = irs::MakeMaterializeState(*r, _ctx),
      });
    }
  }
}

}  // namespace sdb::connector
