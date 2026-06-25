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

#include "pg/pg_catalog/pg_tablespace.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgTablespace::spcacl),
  GetIndex(&PgTablespace::spcoptions),
});

}  // namespace

// TODO: emit user rows here once CREATE TABLESPACE is implemented.
template<>
catalog::MaterializedData SystemTableSnapshot<PgTablespace>::GetTableData() {
  const std::array<PgTablespace, 2> values{
    PgTablespace{
      .oid = 1663,
      .spcname = "pg_default",
      .spcowner = id::kRootUser.id(),
    },
    PgTablespace{
      .oid = 1664,
      .spcname = "pg_global",
      .spcowner = id::kRootUser.id(),
    },
  };

  auto result = CreateColumns<PgTablespace>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row,
              *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
