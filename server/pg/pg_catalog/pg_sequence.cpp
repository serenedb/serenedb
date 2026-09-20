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

#include "pg/pg_catalog/pg_sequence.h"

#include <cstdint>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <vector>

#include "auth/role_closure.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

// duckdb hands out one value at a time, so there is no cache to report;
// postgres writes 1 for an uncached sequence, which is every sequence here.
constexpr int64_t kNoSequenceCache = 1;

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgSequence>::GetTableData() {
  auto& context = _context;
  std::vector<PgSequence> values;
  VisitEntries<duckdb::SequenceCatalogEntry>(
    context, GetDatabase(), [&](const duckdb::SequenceCatalogEntry& sequence) {
      const auto data = sequence.GetData();
      values.push_back(PgSequence{
        .seqrelid = Oid{sequence.oid},
        .seqtypid = Oid{static_cast<uint64_t>(PgTypeOID::kInt8)},
        .seqstart = data.start_value,
        .seqincrement = data.increment,
        .seqmax = data.max_value,
        .seqmin = data.min_value,
        .seqcache = kNoSequenceCache,
        .seqcycle = data.cycle,
      });
    });

  auto result = CreateColumns<PgSequence>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *sdb::auth::RolesOf(&context));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
