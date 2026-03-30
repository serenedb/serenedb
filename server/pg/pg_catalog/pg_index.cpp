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

#include "pg/pg_catalog/pg_index.h"

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/local_catalog.h"
#include "catalog/table.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"
#include "rest_server/serened.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgIndex::indexrelid),
  GetIndex(&PgIndex::indrelid),
  GetIndex(&PgIndex::indnatts),
  GetIndex(&PgIndex::indnkeyatts),
  GetIndex(&PgIndex::indisunique),
  GetIndex(&PgIndex::indnullsnotdistinct),
  GetIndex(&PgIndex::indisprimary),
  GetIndex(&PgIndex::indisexclusion),
  GetIndex(&PgIndex::indimmediate),
  GetIndex(&PgIndex::indisclustered),
  GetIndex(&PgIndex::indisvalid),
  GetIndex(&PgIndex::indcheckxmin),
  GetIndex(&PgIndex::indisready),
  GetIndex(&PgIndex::indislive),
  GetIndex(&PgIndex::indisreplident),
  GetIndex(&PgIndex::indkey),
});

}  // namespace

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgIndex>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgIndex> values;
  std::vector<std::vector<int16_t>> indkey_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    SDB_ASSERT(schema);
    for (const auto& index_ptr :
         catalog->GetIndexes(GetDatabaseId(), schema->GetName())) {
      SDB_ASSERT(index_ptr);
      auto& index = *index_ptr;
      auto column_ids = index.GetColumnIds();
      auto natts = static_cast<int16_t>(column_ids.size());

      // Build indkey: map column IDs to 1-based attnum in the parent table
      std::vector<int16_t> indkey;
      indkey.reserve(column_ids.size());

      auto table_obj = catalog->GetObject(index.GetRelationId());
      if (table_obj && table_obj->GetType() == catalog::ObjectType::Table) {
        auto& table = basics::downCast<catalog::Table>(*table_obj);
        auto& columns = table.Columns();
        for (auto col_id : column_ids) {
          int16_t attnum = 0;
          for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].id == col_id) {
              attnum = static_cast<int16_t>(i + 1);
              break;
            }
          }
          indkey.push_back(attnum);
        }
      }
      indkey_storage.push_back(std::move(indkey));
      values.push_back({
        .indexrelid = index.GetId().id(),
        .indrelid = index.GetRelationId().id(),
        .indnatts = natts,
        .indnkeyatts = natts,
        .indisunique = false,
        .indnullsnotdistinct = false,
        .indisprimary = false,
        .indisexclusion = false,
        .indimmediate = true,
        .indisclustered = false,
        .indisvalid = true,
        .indcheckxmin = false,
        .indisready = true,
        .indislive = true,
        .indisreplident = false,
        .indkey = indkey_storage.back(),
      });
    }
  }

  auto result = CreateColumns<PgIndex>(values, &pool);

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
