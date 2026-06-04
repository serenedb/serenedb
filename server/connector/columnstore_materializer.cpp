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
  const irs::columnstore::Reader& reader,
  std::span<const ColumnstoreProjection> projections,
  duckdb::ClientContext* context)
  : _ctx{reader}, _context{context} {
  _bound.reserve(projections.size());
  for (const auto& cp : projections) {
    const auto* r = reader.Column(static_cast<irs::field_id>(cp.column_id));
    if (!r) {
      continue;
    }
    _bound.push_back(Binding{
      .reader = r,
      .output_slot = cp.output_slot,
      .state = irs::columnstore::MakeMaterializeState(*r, _ctx),
      .extract_path = cp.extract_path,
      .extract_scan_type = cp.extract_scan_type,
    });
  }
}

}  // namespace sdb::connector
