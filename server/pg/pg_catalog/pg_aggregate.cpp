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

#include "pg/pg_catalog/pg_aggregate.h"

#include <vector>

#include "pg/pg_catalog/builtin_functions.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAggregate::agginitval),
  GetIndex(&PgAggregate::aggminitval),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAggregate>::GetTableData() {
  std::vector<PgAggregate> values;

  VisitBuiltinFunctions(
    _config.GetClientContext(), [&](const BuiltinFunction& builtin) {
      if (builtin.kind != duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY) {
        return;
      }
      values.push_back(PgAggregate{
        .aggfnoid = builtin.oid.id(),
        .aggkind = PgAggregate::Aggkind::Normal,
        .aggnumdirectargs = 0,
        .aggtransfn = 0,
        .aggfinalfn = 0,
        .aggcombinefn = 0,
        .aggserialfn = 0,
        .aggdeserialfn = 0,
        .aggmtransfn = 0,
        .aggminvtransfn = 0,
        .aggmfinalfn = 0,
        .aggfinalextra = false,
        .aggmfinalextra = false,
        .aggfinalmodify = PgAggregate::Aggfinalmodify::ReadOnly,
        .aggmfinalmodify = PgAggregate::Aggfinalmodify::ReadOnly,
        .aggsortop = 0,
        .aggtranstype = static_cast<Oid>(PgTypeOID::kInternal),
        .aggtransspace = 0,
        .aggmtranstype = 0,
        .aggmtransspace = 0,
      });
    });

  auto result = CreateColumns<PgAggregate>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
