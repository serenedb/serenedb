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

#include "pg/pg_catalog/pg_am.h"

#include "catalog/identifiers/object_id.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr auto kSampleData = std::to_array<PgAm>({
  {
    .oid = id::kPgAmInverted.id(),
    .amname = "inverted",
    .amhandler = 0,
    .amtype = PgAm::Amtype::Index,
  },
  {
    .oid = id::kPgAmIresearch.id(),
    .amname = "iresearch",
    .amhandler = 0,
    .amtype = PgAm::Amtype::Table,
  },
  {
    .oid = id::kPgAmSecondary.id(),
    .amname = "secondary",
    .amhandler = 0,
    .amtype = PgAm::Amtype::Index,
  },
});

constexpr uint64_t kNullMask = 0;

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAm>::GetTableData() {
  auto result = CreateColumns<PgAm>(kSampleData.size());
  for (size_t row = 0; row < kSampleData.size(); ++row) {
    WriteData(result, kSampleData[row], kNullMask, row);
  }
  return {std::move(result), kSampleData.size()};
}

}  // namespace sdb::pg
