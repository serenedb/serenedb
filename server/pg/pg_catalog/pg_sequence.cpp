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
#include <vector>

#include "catalog/catalog.h"
#include "catalog/sequence.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgSequence>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  const auto database_id = GetDatabaseId();

  std::vector<PgSequence> values;
  for (const auto& schema : catalog->GetSchemas(database_id)) {
    for (const auto& sequence :
         catalog->GetSequences(database_id, schema->GetName())) {
      const auto& options = sequence->Options();
      values.push_back(PgSequence{
        .seqrelid = sequence->GetId().id(),
        // serenedb sequences are bigint-typed, like a PostgreSQL default.
        .seqtypid = static_cast<Oid>(PgTypeOID::kInt8),
        .seqstart = static_cast<int64_t>(options.start_value),
        .seqincrement = static_cast<int64_t>(options.increment),
        .seqmax = static_cast<int64_t>(options.max_value),
        .seqmin = static_cast<int64_t>(options.min_value),
        .seqcache = static_cast<int64_t>(options.cache),
        .seqcycle = options.cycle,
      });
    }
  }

  auto result = CreateColumns<PgSequence>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
