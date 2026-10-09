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

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kSequenceTypes[] = {
  duckdb::CatalogType::SEQUENCE_ENTRY};

constexpr SystemIndex kSequenceIndexes[] = {
  {kPgSequenceSql["seqrelid"], SystemLookup::Object},
};

struct Sequence {
  const duckdb::SequenceCatalogEntry& entry;
  mutable std::optional<duckdb::SequenceData> data;

  const duckdb::SequenceData& Data() const {
    if (!data) {
      data.emplace(entry.GetData());
    }
    return *data;
  }
};

class PgSequence final : public SystemTableScan<kPgSequenceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kSequenceTypes, SystemSchemas::Skip, kSequenceIndexes}};

  static constexpr auto kSequence = Shape<kSql, const Sequence>(
    Col<"seqrelid">([](const auto& row) { return row.entry.oid; }),
    Col<"seqtypid">([](const auto&) { return kInt8; }),
    Col<"seqstart">([](const auto& row) { return row.Data().start_value; }),
    Col<"seqincrement">([](const auto& row) { return row.Data().increment; }),
    Col<"seqmax">([](const auto& row) { return row.Data().max_value; }),
    Col<"seqmin">([](const auto& row) { return row.Data().min_value; }),
    Col<"seqcache">([](const auto& row) { return row.Data().cache; }),
    Col<"seqcycle">([](const auto& row) { return row.Data().cycle; }));

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!NumbersRows(sequence)) {
      Emit<kSequence>({sequence, {}});
    }
  }
};

}  // namespace

SystemTable gPgSequence = SystemTableOf<PgSequence>();

}  // namespace sdb::pg
