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

#include "auth/role_closure.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/sequence.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

// The options are unsigned everywhere below the catalog -- nextval wants the
// bounds and the increment as one lattice -- while postgres reports them as
// int8, which is also the only sequence type serenedb creates.
int64_t Signed(uint64_t value) noexcept { return static_cast<int64_t>(value); }

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgSequence>::GetTableData() {
  auto& context = _config.GetClientContext();
  std::vector<PgSequence> values;
  catalog::Visit<catalog::SereneDBSequenceEntry>(
    &context, GetDatabaseId(),
    [&](const catalog::SereneDBSequenceEntry& sequence) {
      const auto& options = sequence.Options();
      values.push_back(PgSequence{
        .seqrelid = Oid{sequence.oid},
        .seqtypid = Oid{static_cast<uint64_t>(PgTypeOID::kInt8)},
        .seqstart = Signed(options.start_value),
        .seqincrement = Signed(options.increment),
        .seqmax = Signed(options.max_value),
        .seqmin = Signed(options.min_value),
        .seqcache = Signed(options.cache),
        .seqcycle = options.cycle,
      });
    });

  auto result = CreateColumns<PgSequence>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *sdb::auth::RolesOf(&context));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
